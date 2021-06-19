package geecache

/*
	Group 是 GeeCache 最核心的数据结构，负责与用户的交互，并且控制缓存值存储和获取的流程。

                            是
接收 key --> 检查是否被缓存 -----> 返回缓存值 ⑴
                |  否                         是
                |-----> 是否应当从远程节点获取 -----> 与远程节点交互 --> 返回缓存值 ⑵
                            |  否
                            |-----> 调用`回调函数`，获取值并添加到缓存 --> 返回缓存值 ⑶
*/

import (
	"Group_Cache/geecache/singleflight"
	"fmt"
	"log"
	"sync"
)

/*
	一个 Group 可以认为是一个缓存的命名空间，
	每个 Group 拥有一个唯一的名称 name。比如可以创建三个 Group，
	缓存学生的成绩命名为 scores，缓存学生信息的命名为 info，缓存学生课程的命名为 courses。
*/
type Group struct {
	name string
	// 缓存未命中时获取源数据的回调(callback)。
	getter Getter
	// 一开始实现的并发缓存。
	mainCache cache
	// 用于根据传入的 key 选择相应节点 PeerGetter
	peers PeerPicker
	// 使用 singleflight.Group 确保每个key只获取一次
	loader *singleflight.Group
}

/*
	如果缓存不存在，应从数据源（文件，数据库等）获取数据并添加到缓存中。
	GeeCache 是否应该支持多种数据源的配置呢？不应该，
	一是数据源的种类太多，没办法一一实现；二是扩展性不好。
	如何从源头获取数据，应该是用户决定的事情，我们就把这件事交给用户好了。
	因此，我们设计了一个回调函数(callback)，在缓存不存在时，调用这个函数，得到源数据。
*/

// Getter 定义接口 通过key返回数据
type Getter interface {
	Get(key string) ([]byte, error)
}

// GetterFunc 定义函数类型 GetterFunc，用函数实现Getter。
type GetterFunc func(key string) ([]byte, error)

// Get 回调函数 实现 Getter 接口的 Get 方法
func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

var (
	mu sync.RWMutex
	// key:string value:Group指针
	groups = make(map[string]*Group)
)

// NewGroup 实例化 Group，并且将 group 存储在全局变量 groups 中。
func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: cache{cacheBytes: cacheBytes},
		loader:    &singleflight.Group{},
	}
	groups[name] = g
	return g
}

// GetGroup 返回特定名称的group
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

/*
	Get 方法实现了流程 ⑴ 和 ⑶。2
	流程 ⑴ ：从 mainCache 中查找缓存，如果存在则返回缓存值。
	流程 ⑶ ：缓存不存在，则调用 load 方法，load 调用 getLocally
	（分布式场景下会调用 getFromPeer 从其他节点获取），getLocally
	调用用户回调函数 g.getter.Get() 获取源数据，并且将源数据添加到缓存
	mainCache 中（通过 populateCache 方法）
*/
func (g *Group) Get(key string) (ByteView, error) {

	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}

	// 缓存命中
	if v, ok := g.mainCache.get(key); ok {
		log.Println("[GeeCache] hit")
		return v, nil
	}

	// 缓存不命中
	return g.load(key)
}

// RegisterPeers 实现了 PeerPicker 接口的 HTTPPool 注入到 Group 中
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

// 从远程节点获取数据,若失败,则调用回调函数从数据库获取
func (g *Group) load(key string) (value ByteView, err error) {
	// 使用 g.loader.Do 包裹，确保了并发场景下针对相同的 key，load 过程只会调用一次
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		// 从远程节点获取 peers为哈希环
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				if value, err = g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				log.Println("[GeeCache] Failed to get from peer", err)
			}
		}
		// 失败 则回调函数
		return g.getLocally(key)
	})

	if err == nil {
		return viewi.(ByteView), nil
	}
	return
}

// getFromPeer 实现了 PeerGetter 接口的 httpGetter 从访问远程节点，获取缓存值。
func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	// 返回远程节点的缓存值(http.get跳到了serverHTTP 里面返回远程节点缓存值)
	bytes, err := peer.Get(g.name, key)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: bytes}, nil
}

// 获得源数据
func (g *Group) getLocally(key string) (ByteView, error) {
	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{b: cloneBytes(bytes)}
	g.populateCache(key, value)
	return value, nil
}

// 将源数据添加到缓存 mainCache 中
func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}
