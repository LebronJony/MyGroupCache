package geecache

/*
	实例化 lru，封装 get 和 add 方法，并添加互斥锁 mu。
*/
import (
	"Group_Cache/geecache/lru"
	"sync"
)

type cache struct {
	mu  sync.Mutex
	lru *lru.Cache
	// 允许使用的最大内存
	cacheBytes int64
}

func (c *cache) add(key string, value ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	/*
		如果等于 nil 再创建实例。这种方法称之为延迟初始化(Lazy Initialization)，
		一个对象的延迟初始化意味着该对象的创建将会延迟至第一次使用该对象时。
		主要用于提高性能，并减少程序内存要求。
	*/
	if c.lru == nil {
		c.lru = lru.New(c.cacheBytes, nil)
	}
	c.lru.Add(key, value)
}

func (c *cache) get(key string) (value ByteView, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return
	}
	if v, ok := c.lru.Get(key); ok {
		return v.(ByteView), ok
	}
	return
}
