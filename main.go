package main

// Overall flow char										     requsets					        local
// gee := createGroup() --------> /api Service : 9999 ------------------------------> gee.Get(key) ------> g.mainCache.Get(key)
// 						|											^					|
// 						|											|					|remote
// 						v											|					v
// 				cache Service : 800x								|			g.peers.PickPeer(key)
// 						|create hash ring & init peerGetter			|					|
// 						|registry peers write in g.peer				|					|p.httpGetters[p.hashRing(key)]
// 						v											|					|
//			httpPool.Set(otherAddrs...)								|					v
// 		g.peers = gee.RegisterPeers(httpPool)						|			g.getFromPeer(peerGetter, key)
// 						|											|					|
// 						|											|					|
// 						v											|					v
// 		http.ListenAndServe("localhost:800x", httpPool)<------------+--------------peerGetter.Get(key)
// 						|											|
// 						|requsets									|
// 						v											|
// 					p.ServeHttp(w, r)								|
// 						|											|
// 						|url.parse()								|
// 						|--------------------------------------------

import (
	"Group_Cache/geecache"
	"flag"
	"fmt"
	"log"
	"net/http"
)


// 使用 map 模拟了数据源 db。
var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

// 创建一个名为 scores 的 Group，若缓存为空，回调函数会从 db 中获取数据并返回。
func createGroup() *geecache.Group {
	return geecache.NewGroup("scores", 2<<10, geecache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)

		},
	))
}

// 用来启动缓存服务器：创建 HTTPPool，添加节点信息，注册到 gee 中，
// 启动 HTTP 服务（共3个端口，8001/8002/8003），用户不感知。
// addr:本地节点地址    addrs:远程真实节点地址	 gee:缓存空间数据结构 一个gee即一个节点
func startCacheServer(addr string, addrs []string, gee *geecache.Group) {
	// 创建 HTTPPool
	peers := geecache.NewHTTPool(addr)
	// 添加远程节点信息
	peers.Set(addrs...)
	// 注册到 gee 中
	gee.RegisterPeers(peers)

	log.Println("geecache is running at", addr)
	// ListenAndServe函数需要一个例如“localhost:8000”的服务器地址，
	// 和一个处理所有请求的Handler接口实例。它会一直运行，
	// 直到这个服务因为一个错误而失败（或者启动失败），它的返回值一定是一个非空的错误。
	log.Fatal(http.ListenAndServe(addr[7:], peers))

}

// 用来启动一个 API 服务（端口 9999），与用户进行交互，用户感知。
// main() 函数需要命令行传入 port 和 api 2 个参数，用来在指定端口启动 HTTP 服务
func startAPIServer(apiAddr string, gee *geecache.Group) {
	http.Handle("/api", http.HandlerFunc(
		func(writer http.ResponseWriter, request *http.Request) {
			key := request.URL.Query().Get("key")
			// Get 缓存命中或不命中
			view, err := gee.Get(key)
			if err != nil {
				http.Error(writer, err.Error(), http.StatusInternalServerError)
				return
			}
			writer.Header().Set("Content-Type", "application/octet-stream")
			writer.Write(view.ByteSlice())
		}))
	log.Println("fontend server is running at", apiAddr)
	log.Fatal(http.ListenAndServe(apiAddr[7:], nil))
}

// main() 函数需要命令行传入 port 和 api 2 个参数，用来在指定端口启动 HTTP 服务。
// geecache 既作为存储的实例，提供 http 接口，又可以作为 API 层，供应用程序直接调用
func main() {
	var port int
	var api bool
	flag.IntVar(&port, "port", 8001, "Geecache server port")
	flag.BoolVar(&api, "api", false, "Start a api server?")
	flag.Parse()

	apiAddr := "http://localhost:9999"
	addrMap := map[int]string{
		8001: "http://localhost:8001",
		8002: "http://localhost:8002",
		8003: "http://localhost:8003",
	}

	var addrs []string
	for _, v := range addrMap {
		addrs = append(addrs, v)
	}

	gee := createGroup()
	if api {
		go startAPIServer(apiAddr, gee)
	}

	startCacheServer(addrMap[port], []string(addrs), gee)

}
