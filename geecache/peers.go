package geecache

/*
	我们进一步细化流程 ⑵：


使用一致性哈希选择节点        是                                    是
    |-----> 是否是远程节点 -----> HTTP 客户端访问远程节点 --> 成功？-----> 服务端返回返回值
                    |  否                                    ↓  否
                    |----------------------------> 回退到本地节点处理。
 */


// PeerPicker 的 PickPeer() 方法用于根据传入的 key 选择相应节点 PeerGetter。
type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter,ok bool)
}

// PeerGetter 接口的 Get() 方法用于从对应 group 查找缓存值。PeerGetter 就对应于上述流程中的 HTTP 客户端。
type PeerGetter interface {
	// Get 参数使用 geecachepb.pb.go 中的数据类型
	Get(group string,key string) ([]byte,error)
}
