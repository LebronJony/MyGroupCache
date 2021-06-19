package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// Hash 定义了函数类型 Hash，采取依赖注入的方式，允许用于替换成自定义的 Hash 函数，
// 也方便测试时替换，默认为 crc32.ChecksumIEEE 算法。
type Hash func(data []byte) uint32

// Map 一致性哈希算法的主数据结构
type Map struct {
	// Hash 函数
	hash Hash
	// 虚拟节点倍数
	replicas int
	// 哈希环，环上为虚拟节点的哈希，几个key映射为一个虚拟节点，几个虚拟节点映射为一个真实节点
	keys []int
	// 虚拟节点与真实节点的映射表，键key是虚拟节点的哈希值，值value是真实节点的名称。
	hashMap map[int]string
}

// New 构造函数 New() 允许自定义虚拟节点倍数和 Hash 函数。
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}

	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add 添加真实节点/机器的 Add() 方法。
// Add 函数允许传入 0 或 多个真实节点的名称。
func (m *Map) Add(keys ...string) {
	// 对每一个真实节点 key，对应创建 m.replicas 个虚拟节点，
	// 虚拟节点的名称是：strconv.Itoa(i) + key，即通过添加编号的方式区分不同虚拟节点。
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			// 使用 m.hash() 计算虚拟节点的哈希值 int转换为字符串：Itoa()
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			// 使用 append(m.keys, hash) 添加到环上。
			m.keys = append(m.keys, hash)
			// 在 hashMap 中增加虚拟节点和真实节点的映射关系。
			m.hashMap[hash] = key
		}
	}
	// 环上的哈希值排序。
	sort.Ints(m.keys)
}

// Get 实现选择节点的 Get() 方法。
// Get 获取散列中与提供的key最接近的真实节点。
func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}

	// 计算 key 的哈希值
	hash := int(m.hash([]byte(key)))
	// 顺时针找到第一个匹配的虚拟节点的下标 idx，从 m.keys 中获取到对应的哈希值。
	// 如果 idx == len(m.keys)，说明应选择 m.keys[0]，
	// 因为 m.keys 是一个环状结构，所以用取余数的方式来处理这种情况。
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	/*
		sort.Search函数使用二分查找的方法，会从[0, n)中取出一个值index，
		index为[0, n)中最小的使函数f(index)为True的值，并且f(index+1)也为True。
		如果无法找到该index值，则该方法为返回n。 此时idx == len(m.keys)
	*/

	// 通过 hashMap 映射得到真实的节点
	return m.hashMap[m.keys[idx%len(m.keys)]]
}

// Remove 删除节点操作
func (m *Map) Remove(key string) {
	for i := 0; i < m.replicas; i++ {
		hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
		idx := sort.SearchInts(m.keys, hash)
		m.keys = append(m.keys[:idx], m.keys[idx+1:]...)
		delete(m.hashMap, hash)
	}
}
