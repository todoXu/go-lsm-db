package main

import (
	"fmt"
	"go-lsm-db/iterator.go"
	"go-lsm-db/kv"
)

// MockIterator 实现 Iterator 接口，用于测试
type MockIterator struct {
	keys    []kv.Key
	values  []kv.Value
	pos     int
	isValid bool // 显式跟踪有效性状态
}

// NewMockIterator 创建一个包含预定义键值对的迭代器
func NewMockIterator(keys []kv.Key, values []kv.Value) *MockIterator {
	return &MockIterator{
		keys:    keys,
		values:  values,
		pos:     0,
		isValid: true, // 初始状态为有效
	}
}

func (m *MockIterator) Key() kv.Key {
	if !m.IsValid() {
		return kv.NewKey(nil, 0) // 返回一个空键表示无效状态
	}
	return m.keys[m.pos]
}

func (m *MockIterator) Value() kv.Value {
	if !m.IsValid() {
		return kv.NewValue(nil) // 返回一个空值表示无效状态
	}
	return m.values[m.pos]
}

func (m *MockIterator) IsValid() bool {
	return m.isValid && m.pos < len(m.keys)
}

func (m *MockIterator) Next() error {
	if !m.isValid {
		return fmt.Errorf("迭代器已关闭")
	}

	if m.pos < len(m.keys) {
		m.pos++
	}
	return nil
}

func (m *MockIterator) Close() {
	m.isValid = false // 明确标记为无效
	fmt.Println("MockIterator已关闭")
}

// 打印迭代器的所有内容
func printIterator(iter iterator.Iterator) {
	count := 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("%d: %s@%d = %s\n",
			count, string(key.RawBytes()), key.Timestamp(), string(value.Value()))

		if err := iter.Next(); err != nil {
			fmt.Printf("错误: %v\n", err)
			break
		}
		count++
	}
	fmt.Printf("总共找到 %d 个结果\n", count)

	// 关闭迭代器并验证关闭效果
	iter.Close()
	fmt.Printf("迭代器关闭后IsValid = %v\n", iter.IsValid())

	// 尝试在关闭后访问
	if iter.IsValid() {
		fmt.Println("错误: 迭代器关闭后仍然有效!")
	} else {
		fmt.Println("正确: 迭代器关闭后变为无效状态")
	}
}

func main() {
	// 创建测试数据
	iter1 := NewMockIterator(
		[]kv.Key{
			kv.NewKey([]byte("apple"), 400),
			kv.NewKey([]byte("banana"), 250),
		},
		[]kv.Value{
			kv.NewValue([]byte("apple-400")),
			kv.NewValue([]byte("banana-250")),
		},
	)

	iter2 := NewMockIterator(
		[]kv.Key{
			kv.NewKey([]byte("apple"), 300),
			kv.NewKey([]byte("cherry"), 300),
		},
		[]kv.Value{
			kv.NewValue([]byte("apple-300")),
			kv.NewValue([]byte("cherry-300")),
		},
	)

	// 测试场景: 验证Close()效果
	fmt.Println("=== 测试: 验证Close()方法 ===")

	endKey := kv.NewKey([]byte("cherry"), 325)
	boundedIter := iterator.NewBoundedMergeIterator(
		[]iterator.Iterator{iter1, iter2},
		endKey,
		func() {
			fmt.Println("BoundedMergeIterator的关闭回调被调用")
		},
	)

	printIterator(boundedIter)

	// 额外测试：确保Close()完全无效化迭代器
	fmt.Println("\n=== 额外测试: Close()后尝试操作 ===")
	if boundedIter.IsValid() {
		fmt.Println("错误: 迭代器应该在Close()后变为无效!")
	}

	// 尝试在Close后调用Next()
	err := boundedIter.Next()
	if err != nil {
		fmt.Printf("正确: Next()在关闭后返回错误: %v\n", err)
	} else {
		fmt.Println("错误: Next()在关闭后应该返回错误!")
	}
}
