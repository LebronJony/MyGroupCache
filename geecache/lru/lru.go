package lru

import "container/list"

type Cache struct {
	// 允许使用的最大内存
	maxBytes int64
	// 当前使用的内存
	nbytes int64
	// 双向链表
	ll *list.List
	// 键是字符串，值是双向链表中对应节点的指针。
	cache map[string]*list.Element
	// 可选并在清除条目时执行
	OnEvicted func(key string, value Value)
}

// Value :为了值的通用性， 使用接口，Len()值所占的大小
type Value interface {
	Len() int
}

// 双向链表节点的数据类型
type entry struct {
	key   string
	value Value
}

// New :Cache的实例化
func New(maxBytes int64, OnEvicted func(key string, value Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: OnEvicted,
	}
}

// Get :根据key进行查找
func (c *Cache) Get(key string) (value Value, ok bool) {
	// 双向链表作为队列，队首队尾是相对的，在这里约定 front 为队尾
	// ele为cache的值，即双向链表对应节点的指针
	if ele, ok := c.cache[key]; ok {
		// 将找到的元素节点移动至队尾
		c.ll.MoveToFront(ele)
		// (*entry)表示将Value转成*entry类型访问
		kv := ele.Value.(*entry)
		return kv.value, true
	}
	return
}

// RemoveOldest :缓存淘汰。即移除最近最少访问的节点（队首）
func (c *Cache) RemoveOldest() {
	// 取到队首节点
	ele := c.ll.Back()
	if ele != nil {
		// 将队首节点从链表中删除。
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		// 从字典中 c.cache 删除该节点的映射关系
		delete(c.cache, kv.key)
		// 更新当前所用的内存
		c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

// Add :缓存的新增和修改
func (c *Cache) Add(key string, value Value) {
	// 如果键存在，则更新对应节点的值，并将该节点移到队尾。
	if ele, ok := c.cache[key]; ok {
		// 将该节点移到队尾
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		c.nbytes += int64(value.Len()) - int64(kv.value.Len())
		// 更新该节点的值
		kv.value = value
	} else {
		// 不存在则在队尾新增节点
		ele := c.ll.PushFront(&entry{key, value})
		// 新增字典键值对
		c.cache[key] = ele
		c.nbytes += int64(len(key)) + int64(value.Len())
	}

	// 当前内存如果超过了设定的最大值，则移除最少访问的节点。
	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}

// Len :返回链表的大小 用于测试
func (c *Cache) Len() int {
	return c.ll.Len()
}
