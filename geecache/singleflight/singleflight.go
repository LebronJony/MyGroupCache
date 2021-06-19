package singleflight

import "sync"

/*
	WaitGroup 对象内部有一个计数器，最初从0开始，它有三个方法：
	Add(), Done(), Wait() 用来控制计数器的数量。
	Add(n) 把计数器设置为n ，Done() 每次把计数器-1 ，
	wait() 会阻塞代码的运行，直到计数器地值减为0。
	适合用于并发协程之间不需要消息传递的情况
*/

// call 代表正在进行中，或已经结束的请求
type call struct {
	// 避免重入
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group singleflight 的主数据结构，管理不同 key 的请求(call)
type Group struct {
	mu sync.Mutex
	// 延迟初始化
	m map[string]*call
}

// Do 方法，接收 2 个参数，第一个参数是 key，第二个参数是一个函数 fn。
// Do 的作用就是，针对相同的 key，无论 Do 被调用多少次，函数 fn 都只会被调用一次，
// 等待 fn 调用结束了，返回返回值或错误。
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {

	// 保护 Group 的成员变量 m 不被并发读写而加上的锁
	g.mu.Lock()
	// g.m的延迟初始化
	if g.m == nil {
		g.m = make(map[string]*call)
	}

	if c, ok := g.m[key]; ok {
		g.mu.Unlock()

		// 如果请求正在进行中，则等待，直到计数器为0
		c.wg.Wait()
		// 请求结束，返回结果，结果直接使用之前请求查询的结果
		// 即所有用户都能收到结果，请求是在服务端阻塞的
		return c.val, c.err
	}

	// 若请求没在进行中，初始化
	c := new(call)
	// 发起请求前加锁 计数器加一
	c.wg.Add(1)
	// 添加到g.m，表明key已经有对应的请求在处理
	g.m[key] = c

	g.mu.Unlock()

	// 调用fn，发起请求
	c.val, c.err = fn()
	// 请求结束 计数器减一
	c.wg.Done()

	g.mu.Lock()
	// 更新g.m
	delete(g.m, key)
	g.mu.Unlock()

	// 返回结果
	return c.val, c.err
}


