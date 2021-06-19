package geecache

// ByteView :b 将会存储真实的缓存值。选择 byte 类型是
// 为了能够支持任意的数据类型的存储，例如字符串、图片等。
type ByteView struct {
	b []byte
}

// Len :返回内存view的大小。实现lru的Value接口的Len方法
func (v ByteView) Len() int {
	return len(v.b)
}

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// ByteSlice :b 是只读的，使用 ByteSlice() 方法
// 返回一个拷贝，防止缓存值被外部程序修改。
func (v ByteView) ByteSlice() []byte {
	return cloneBytes(v.b)
}

func (v ByteView) String() string {
	return string(v.b)
}
