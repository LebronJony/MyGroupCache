package geecache

import (
	"Group_Cache/geecache/consistenthash"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

/*
	我们进一步细化流程 ⑵：


使用一致性哈希选择节点        是                                    是
    |-----> 是否是远程节点 -----> HTTP 客户端访问远程节点 --> 成功？-----> 服务端返回返回值
                    |  否                                    ↓  否
                    |----------------------------> 回退到本地节点处理。
*/

/*
	geecache/
    |--lru/
        |--lru.go  // lru 缓存淘汰策略
    |--byteview.go // 缓存值的抽象与封装
    |--cache.go    // 并发控制
    |--geecache.go // 负责与外部交互，控制缓存存储和获取的主流程
	|--http.go     // 提供被其他节点访问的能力(基于http)
*/

const (
	defaultBasePath = "/_geecache/"
	defaultReplicas = 50
)

// HTTPPool :作为承载节点间 HTTP 通信的核心数据结构
type HTTPPool struct {
	// 用来记录自己的地址，包括主机名/IP 和端口
	self string
	// 作为节点间通讯地址的前缀，默认是 /_geecache/
	// 那么 http://example.com/_geecache/ 开头的请求，就用于节点间的访问。
	// 因为一个主机上还可能承载其他的服务，加一段 Path 是一个好习惯。
	// 比如，大部分网站的 API 接口，一般以 /api 作为前缀。
	basePath string
	mu       sync.Mutex
	// 类型是一致性哈希算法的 Map，用来根据具体的 key 选择节点
	peers *consistenthash.Map
	// 映射远程节点 peer(即哈希环的真实节点) 与对应的 httpGetter。每一个远程节点对应一个 httpGetter，
	// 因为 httpGetter 与远程节点的地址 baseURL 有关。
	httpGetters map[string]*httpGetter
}

// NewHTTPool :初始化
func NewHTTPool(self string) *HTTPPool {
	return &HTTPPool{
		self:     self,
		basePath: defaultBasePath,
	}
}

// Log :带有服务器名称的日志信息
func (p *HTTPPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

// 服务端
func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 判断访问路径的前缀是否是 basePath，不是返回错误。
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}

	p.Log("%s,%s", r.Method, r.URL.Path)

	// 约定访问路径格式为 /<basePath>/<groupName>/<key>
	// r.URL.Path 从basePath之后开始分割，分割出两个子串
	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	groupName := parts[0]
	key := parts[1]
	// 通过 groupName 得到 group 实例
	group := GetGroup(groupName)

	if group == nil {
		http.Error(w, "No such group: "+groupName, http.StatusNotFound)
	}

	// 再使用 group.Get(key) 获取缓存数据
	// 即远程节点又走了一遍流程123
	view, err := group.Get(key)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 设置http头 key: Content-Type（内容类型) value: application/octet-stream
	w.Header().Set("Content-Type", "application/octet-stream")
	// 最终使用 w.Write() 将缓存值作为 httpResponse 的 body 返回
	w.Write(view.ByteSlice())

}

// http客户端
// baseURL 表示将要访问的远程节点的地址，例如 http://example.com/_geecache/
type httpGetter struct {
	baseURL string
}

// Get 实现 PeerGetter 接口 ,用于从对应 group 查找缓存值。
// Get 使用 http.Get() 方式获取返回值，并转换为 []bytes 类型
func (h *httpGetter) Get(group string, key string) ([]byte, error) {
	// QueryEscape函数对参数进行转码使之可以安全的用在URL查询里
	u := fmt.Sprintf("%v%v/%v", h.baseURL, url.QueryEscape(group), url.QueryEscape(key))
	// url get请求 使用 http.Get() 方式获取返回值
	// 这一步直接到了上面的ServeHTTP()
	// ServeHTTP()返回的就是对于远程节点的缓存值
	res, err := http.Get(u)

	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned: %v", res.Status)
	}

	// 读取所有内容
	bytes, err := ioutil.ReadAll(res.Body)

	if err != nil {
		return nil, fmt.Errorf("reading response body: %v", err)
	}

	return bytes, nil

}

// 确保这个类型实现了这个接口 如果没有实现会报错
var _ PeerGetter = (*httpGetter)(nil)

// Set 方法实例化了一致性哈希算法，并且添加了传入的节点
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...)

	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		// 即对httpGetter结构体的实例化
		p.httpGetters[peer] = &httpGetter{baseURL: peer + p.basePath}
	}
}

// PickPeer 实现 PeerPicker 接口
// 用于根据传入的 key 选择相应节点 PeerGetter,返回节点对应的 HTTP 客户端
func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
		// 查询哈希环返回真实节点peer
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %s", peer)
		return p.httpGetters[peer], true
	}
	return nil, false
}
