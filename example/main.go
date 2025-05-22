package main

import (
	"fmt"
	"go-lsm-db/kv"
	"go-lsm-db/memory"
	"os"
	"path/filepath"
)

func main() {

	fmt.Println("=== MemTable 功能测试 ===")
	path, _ := os.Getwd()
	path = filepath.Join(path, "wal")
	// 创建新的MemTable
	memtable := memory.NewMemtableWithWAL(1, 1024*1024, path)
	fmt.Printf("初始化MemTable大小: %d 字节\n", memtable.SizeInBytes())

	// 测试1: 基本的Set和Get操作
	fmt.Println("\n--- 测试1: 基本的Set和Get操作 ---")
	testBasicSetGet(memtable)

	// 测试2: 更新现有键
	fmt.Println("\n--- 测试2: 更新现有键 ---")
	testUpdateExistingKey(memtable)

	// 测试3: 删除操作
	fmt.Println("\n--- 测试3: 删除操作 ---")
	testDelete(memtable)

	// 测试4: 范围查询
	fmt.Println("\n--- 测试4: 范围查询 ---")
	testRangeScan(memtable)

	// 测试4.1: 空MemTable范围查询
	fmt.Println("\n--- 测试4.1: 空MemTable范围查询 ---")
	testEmptyMemTableScan()

	// 测试4.2: 时间戳排序和范围查询
	fmt.Println("\n--- 测试4.2: 时间戳排序和范围查询 ---")
	testTimestampOrdering()

	// 测试4.3: 边界条件范围查询
	fmt.Println("\n--- 测试4.3: 边界条件范围查询 ---")
	testBoundaryConditionScan()

	// 测试5: 大量数据插入和内存限制
	fmt.Println("\n--- 测试5: 大量数据插入和内存限制 ---")
	testMemoryLimit()

	// 测试6: 不同时间戳的相同键
	fmt.Println("\n--- 测试6: 不同时间戳的相同键 ---")
	testDifferentTimestamps()

	// 测试7: WAL恢复
	fmt.Println("\n--- 测试7: WAL恢复 ---")
	testWALRecovery(path)

	fmt.Println("\n=== 所有测试完成 ===")
}

// 测试基本的Set和Get操作
func testBasicSetGet(memtable *memory.Memtable) {
	// 插入几个键值对
	keyValues := []struct {
		key   string
		ts    uint64
		value string
	}{
		{"apple", 100, "red fruit"},
		{"banana", 200, "yellow fruit"},
		{"cherry", 300, "small red fruit"},
	}

	for _, keyValue := range keyValues {
		key := kv.NewKey([]byte(keyValue.key), keyValue.ts)
		value := kv.NewValue([]byte(keyValue.value))
		err := memtable.Set(key, value)
		if err != nil {
			fmt.Printf("插入失败 %s@%d: %v\n", keyValue.key, keyValue.ts, err)
		} else {
			fmt.Printf("插入 %s@%d: %s\n", keyValue.key, keyValue.ts, keyValue.value)
		}
	}

	// 尝试获取这些键
	for _, keyValue := range keyValues {
		key := kv.NewKey([]byte(keyValue.key), keyValue.ts)
		value, found := memtable.Get(key)
		if found {
			fmt.Printf("找到 %s@%d: %s\n", keyValue.key, keyValue.ts, string(value.Value()))
		} else {
			fmt.Printf("未找到 %s@%d\n", keyValue.key, keyValue.ts)
		}
	}

	// 尝试获取不存在的键
	key := kv.NewKey([]byte("grape"), 100)
	_, found := memtable.Get(key)
	fmt.Printf("不存在的键 'grape@100': 找到=%v (应为false)\n", found)
}

// 测试更新现有键
func testUpdateExistingKey(memtable *memory.Memtable) {
	key := kv.NewKey([]byte("apple"), 100)

	// 获取初始值
	origValue, found := memtable.Get(key)
	fmt.Printf("初始值: apple@100 = %s, 找到=%v\n", string(origValue.Value()), found)

	// 记录初始大小
	initSize := memtable.SizeInBytes()
	fmt.Printf("更新前MemTable大小: %d 字节\n", initSize)

	// 更新值
	newValue := kv.NewValue([]byte("green fruit"))
	err := memtable.Set(key, newValue)
	if err != nil {
		fmt.Printf("更新失败: %v\n", err)
		return
	}

	// 获取更新后的值
	updatedValue, found := memtable.Get(key)
	fmt.Printf("更新后的值: apple@100 = %s, 找到=%v\n", string(updatedValue.Value()), found)

	// 检查大小变化
	newSize := memtable.SizeInBytes()
	fmt.Printf("更新后MemTable大小: %d 字节 (变化: %d 字节)\n", newSize, newSize-initSize)
}

// 测试删除操作
func testDelete(memtable *memory.Memtable) {
	// 先检查键是否存在
	key := kv.NewKey([]byte("banana"), 200)
	_, found := memtable.Get(key)
	fmt.Printf("删除前: banana@200 存在=%v\n", found)

	// 删除键
	err := memtable.Delete(key)
	if err != nil {
		fmt.Printf("删除失败: %v\n", err)
		return
	}

	// 检查键是否仍然存在
	_, found = memtable.Get(key)
	fmt.Printf("删除后: banana@200 存在=%v (应为false)\n", found)

	// 检查删除不存在的键
	nonExistKey := kv.NewKey([]byte("nonexistent"), 100)
	err = memtable.Delete(nonExistKey)
	if err != nil {
		fmt.Printf("删除不存在的键失败: %v\n", err)
	} else {
		fmt.Printf("删除不存在的键成功\n")
	}
}

// 测试范围查询
func testRangeScan(memtable *memory.Memtable) {
	// 先添加一些有序的键
	orderedKeys := []struct {
		key   string
		ts    uint64
		value string
	}{
		{"alpha", 100, "first letter"},
		{"beta", 200, "second letter"},
		{"gamma", 150, "third letter"},
		{"delta", 300, "fourth letter"},
		{"epsilon", 250, "fifth letter"},
		{"alpha", 50, "older version"}, // 相同键，不同时间戳
		{"beta", 50, "older beta"},     // 另一个不同时间戳的键
		{"gamma", 50, "older gamma"},   // 另一个不同时间戳的键
		{"zeta", 100, "last letter"},   // 最后一个字母
	}

	for _, keyValue := range orderedKeys {
		key := kv.NewKey([]byte(keyValue.key), keyValue.ts)
		value := kv.NewValue([]byte(keyValue.value))
		memtable.Set(key, value)
		fmt.Printf("添加: %s@%d = %s\n", keyValue.key, keyValue.ts, keyValue.value)
	}

	memtable.AllEntries(func(key kv.Key, value kv.Value) {
		fmt.Printf("跳表: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
	})

	// 测试1: 明确的闭区间范围查询 [beta:200, delta:300]
	fmt.Println("\n范围查询1: [beta@200, delta@300]")
	testRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("beta"), 200),
		kv.NewKey([]byte("delta"), 300),
	)

	iter := memtable.Scan(testRange)
	count := 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("范围内找到 %d 个键值对\n", count)

	// 测试2: 时间戳范围查询 - 对于特定键查找特定时间戳范围内的条目
	fmt.Println("\n范围查询2: [alpha@40, alpha@60] (时间戳范围)")
	timeRangeQuery := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("alpha"), 40),
		kv.NewKey([]byte("alpha"), 60),
	)

	iter = memtable.Scan(timeRangeQuery)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("范围内找到 %d 个键值对\n", count)

	// 测试3: 精确的用户键范围 - 查找特定键的所有版本
	fmt.Println("\n范围查询3: [beta@0, beta@max] (精确用户键的所有版本)")
	exactKeyRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("beta"), 0),
		kv.NewKey([]byte("beta"), ^uint64(0)), // 最大时间戳
	)

	iter = memtable.Scan(exactKeyRange)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("范围内找到 %d 个键值对\n", count)

	// 测试5: 边界情况 - 范围边界正好是键
	fmt.Println("\n范围查询5: [alpha@50, gamma@150] (边界正好是键)")
	boundaryRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("alpha"), 50),
		kv.NewKey([]byte("gamma"), 150),
	)

	iter = memtable.Scan(boundaryRange)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("范围内找到 %d 个键值对\n", count)

	// 测试6: 窄范围 - 只有一个条目
	fmt.Println("\n范围查询6: [epsilon@250, epsilon@250] (只有一个条目的范围)")
	narrowRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("epsilon"), 250),
		kv.NewKey([]byte("epsilon"), 250),
	)

	iter = memtable.Scan(narrowRange)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("范围内找到 %d 个键值对\n", count)

	// 测试7: 空范围查询 - 范围内没有键
	fmt.Println("\n范围查询7: [omega@100, omega@200] (空范围)")
	emptyRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("omega"), 100),
		kv.NewKey([]byte("omega"), 200),
	)

	iter = memtable.Scan(emptyRange)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("范围内找到 %d 个键值对\n", count)

	// 测试8: 完整范围查询 - 包含所有键
	fmt.Println("\n范围查询8: [alpha@0, zeta@200] (包含所有键)")
	fullRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("alpha"), 0),
		kv.NewKey([]byte("zeta"), 200),
	)

	iter = memtable.Scan(fullRange)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("范围内找到 %d 个键值对\n", count)
}

// 测试空MemTable范围查询
func testEmptyMemTableScan() {
	// 创建一个新的空MemTable
	emptyMemtable := memory.NewMemtableWithWAL(9, 1024, "./empty-wal")
	defer os.RemoveAll("./empty-wal")

	fmt.Printf("空MemTable大小: %d 字节\n", emptyMemtable.SizeInBytes())
	fmt.Printf("空MemTable是否为空: %v\n", emptyMemtable.IsEmpty())

	// 测试1: 在空MemTable上进行范围查询
	fmt.Println("\n空MemTable范围查询1: [a@100, z@200]")
	emptyRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("a"), 100),
		kv.NewKey([]byte("z"), 200),
	)

	iter := emptyMemtable.Scan(emptyRange)
	count := 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("空MemTable范围内找到 %d 个键值对 (应为0)\n", count)

	// 测试2: 在添加一个键后进行范围查询
	key := kv.NewKey([]byte("lonely"), 100)
	value := kv.NewValue([]byte("single entry"))
	emptyMemtable.Set(key, value)
	fmt.Printf("添加后MemTable是否为空: %v\n", emptyMemtable.IsEmpty())

	fmt.Println("\n添加一个键后范围查询: [a@0, z@^uint64(0)]")
	singleItemRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("a"), 0),
		kv.NewKey([]byte("z"), ^uint64(0)),
	)

	iter = emptyMemtable.Scan(singleItemRange)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("范围内找到 %d 个键值对 (应为1)\n", count)

	// 测试3: 仅包含边界的范围查询
	fmt.Println("\n仅包含边界的范围查询: [lonely@100, lonely@100]")
	exactRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("lonely"), 100),
		kv.NewKey([]byte("lonely"), 100),
	)

	iter = emptyMemtable.Scan(exactRange)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("范围内找到 %d 个键值对 (应为1)\n", count)

	// 测试4: 不包含任何键的范围查询
	fmt.Println("\n不包含任何键的范围查询: [missing@100, missing@200]")
	missingRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("missing"), 100),
		kv.NewKey([]byte("missing"), 200),
	)

	iter = emptyMemtable.Scan(missingRange)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("范围内找到 %d 个键值对 (应为0)\n", count)
}

// 测试内存限制
func testMemoryLimit() {
	// 创建一个很小的MemTable
	smallMemtable := memory.NewMemtableWithWAL(2, 200, "./small-wal")
	defer os.RemoveAll("./small-wal")

	// 尝试添加一些键值对，直到达到限制
	inserted := 0
	rejected := 0

	for i := 0; i < 10; i++ {
		key := kv.NewKey([]byte(fmt.Sprintf("key%d", i)), uint64(i))
		// 创建一个足够大的值
		value := kv.NewValue([]byte(fmt.Sprintf("value%d-%s", i, string(make([]byte, 30)))))

		err := smallMemtable.Set(key, value)
		if err != nil {
			fmt.Printf("第 %d 次插入被拒绝: %v\n", i, err)
			rejected++
		} else {
			fmt.Printf("第 %d 次插入成功, MemTable大小: %d 字节\n", i, smallMemtable.SizeInBytes())
			inserted++
		}
	}

	fmt.Printf("成功插入: %d, 被拒绝: %d\n", inserted, rejected)
}

// 测试不同时间戳的相同键
func testDifferentTimestamps() {
	memtable := memory.NewMemtableWithWAL(3, 1024, "./ts-wal")
	defer os.RemoveAll("./ts-wal")

	// 插入相同键的不同版本
	versions := []struct {
		ts    uint64
		value string
	}{
		{100, "version 1"},
		{200, "version 2"},
		{150, "version 1.5"},
		{50, "old version"},
	}

	for _, v := range versions {
		key := kv.NewKey([]byte("versioned"), v.ts)
		value := kv.NewValue([]byte(v.value))
		memtable.Set(key, value)
		fmt.Printf("添加: versioned@%d = %s\n", v.ts, v.value)
	}

	// 使用扫描遍历所有版本
	fmt.Println("按顺序扫描所有版本 (应该是时间戳降序):")

	fullRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("versioned"), 0),
		kv.NewKey([]byte("versioned"), ^uint64(0)),
	)

	iter := memtable.Scan(fullRange)
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  版本: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
	}

	// 获取特定版本
	specificTs := uint64(150)
	specificKey := kv.NewKey([]byte("versioned"), specificTs)
	value, found := memtable.Get(specificKey)

	if found {
		fmt.Printf("找到特定版本 versioned@%d = %s\n", specificTs, string(value.Value()))
	} else {
		fmt.Printf("未找到特定版本 versioned@%d\n", specificTs)
	}
}

// 测试WAL恢复
func testWALRecovery(tempDir string) {
	walPath := filepath.Join(tempDir, "recovery-wal")

	{
		// 创建新的MemTable并写入数据
		fmt.Println("写入数据到新的MemTable:")
		memtable := memory.NewMemtableWithWAL(4, 1024, walPath)

		testData := []struct {
			key   string
			ts    uint64
			value string
		}{
			{"recover1", 100, "data 1"},
			{"recover2", 200, "data 2"},
			{"recover3", 300, "data 3"},
		}

		for _, data := range testData {
			key := kv.NewKey([]byte(data.key), data.ts)
			value := kv.NewValue([]byte(data.value))
			memtable.Set(key, value)
			fmt.Printf("  写入: %s@%d = %s\n", data.key, data.ts, data.value)
		}

		fmt.Println("MemTable已关闭，模拟崩溃")
	}

	{
		// 从WAL恢复
		fmt.Println("从WAL恢复数据:")
		recoveredMemtable, _, err := memory.RecoverFromWAL(4, 1024, walPath)

		if err != nil {
			fmt.Printf("恢复失败: %v\n", err)
			return
		}

		// 验证恢复的数据
		testKeys := []struct {
			key string
			ts  uint64
		}{
			{"recover1", 100},
			{"recover2", 200},
			{"recover3", 300},
		}

		for _, k := range testKeys {
			key := kv.NewKey([]byte(k.key), k.ts)
			value, found := recoveredMemtable.Get(key)

			if found {
				fmt.Printf("  恢复: %s@%d = %s\n", k.key, k.ts, string(value.Value()))
			} else {
				fmt.Printf("  错误: 未能恢复 %s@%d\n", k.key, k.ts)
			}
		}
		// 清理WAL
		recoveredMemtable.DeleteWAL()

	}
}

// 测试时间戳排序和范围查询
func testTimestampOrdering() {
	// 创建一个新的MemTable
	tsMemtable := memory.NewMemtableWithWAL(10, 1024, "./ts-order-wal")
	defer os.RemoveAll("./ts-order-wal")

	// 添加多个具有相同用户键但不同时间戳的键值对
	sameKeyEntries := []struct {
		key   string
		ts    uint64
		value string
	}{
		{"samekey", 1000, "version 10"},
		{"samekey", 900, "version 9"},
		{"samekey", 800, "version 8"},
		{"samekey", 700, "version 7"},
		{"samekey", 600, "version 6"},
		{"samekey", 500, "version 5"},
		{"samekey", 400, "version 4"},
		{"samekey", 300, "version 3"},
		{"samekey", 200, "version 2"},
		{"samekey", 100, "version 1"},
	}

	// 乱序添加条目，验证排序是否正确
	insertOrder := []int{3, 1, 7, 0, 5, 9, 2, 6, 4, 8} // 乱序索引
	for _, idx := range insertOrder {
		entry := sameKeyEntries[idx]
		key := kv.NewKey([]byte(entry.key), entry.ts)
		value := kv.NewValue([]byte(entry.value))
		tsMemtable.Set(key, value)
		fmt.Printf("添加: %s@%d = %s\n", entry.key, entry.ts, entry.value)
	}

	// 测试1: 完整范围查询 - 应按时间戳降序排列
	fmt.Println("\n时间戳测试1: 完整范围查询 [samekey@0, samekey@^uint64(0)]")
	fullRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("samekey"), 0),
		kv.NewKey([]byte("samekey"), ^uint64(0)),
	)

	iter := tsMemtable.Scan(fullRange)
	count := 0
	var lastTs uint64 = ^uint64(0) // 最大值开始
	correctOrder := true

	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		currTs := key.Timestamp()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), currTs, string(value.Value()))

		// 验证是否按时间戳降序排列
		if currTs > lastTs {
			fmt.Printf("  错误: 时间戳顺序错误! %d 应小于 %d\n", currTs, lastTs)
			correctOrder = false
		}
		lastTs = currTs

		iter.Next()
		count++
	}
	fmt.Printf("范围内找到 %d 个键值对\n", count)
	fmt.Printf("时间戳降序排序正确: %v\n", correctOrder)

	// 测试2: 时间戳范围查询
	fmt.Println("\n时间戳测试2: 时间戳范围 [samekey@300, samekey@700]")
	tsRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("samekey"), 300),
		kv.NewKey([]byte("samekey"), 700),
	)

	iter = tsMemtable.Scan(tsRange)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("时间戳范围内找到 %d 个键值对 (应为5)\n", count)

	// 测试3: 边界条件范围查询
	fmt.Println("\n时间戳测试3: 边界条件 [samekey@500, samekey@500]")
	boundaryRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("samekey"), 500),
		kv.NewKey([]byte("samekey"), 500),
	)

	iter = tsMemtable.Scan(boundaryRange)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("边界条件范围内找到 %d 个键值对 (应为1)\n", count)

	// 测试4: 添加相邻键，测试键的排序和时间戳的排序
	adjacentEntries := []struct {
		key   string
		ts    uint64
		value string
	}{
		{"samekex", 500, "adjacent key 1"},
		{"samekez", 300, "adjacent key 2"},
		{"samekex", 800, "adjacent key newer"},
	}

	for _, entry := range adjacentEntries {
		key := kv.NewKey([]byte(entry.key), entry.ts)
		value := kv.NewValue([]byte(entry.value))
		tsMemtable.Set(key, value)
		fmt.Printf("添加相邻键: %s@%d = %s\n", entry.key, entry.ts, entry.value)
	}

	tsMemtable.AllEntries(func(key kv.Key, value kv.Value) {
		fmt.Printf("跳表: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
	})

	// 测试相邻键的范围查询 - 键按字典序，每个键内部按时间戳降序
	fmt.Println("\n时间戳测试4: 相邻键范围查询 [samekey@0, samekez@^uint64(0)]")
	adjacentRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("samekey"), 0),
		kv.NewKey([]byte("samekez"), ^uint64(0)),
	)

	iter = tsMemtable.Scan(adjacentRange)
	count = 0
	var lastKey string = ""
	lastTs = ^uint64(0)
	var keySwitched bool = false
	correctKeyOrder := true

	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		currKey := string(key.RawBytes())
		currTs := key.Timestamp()
		fmt.Printf("  找到: %s@%d = %s\n", currKey, currTs, string(value.Value()))

		// 检查键的顺序
		if lastKey != "" && lastKey != currKey {
			keySwitched = true
			lastTs = ^uint64(0) // 重置时间戳比较
			if lastKey > currKey {
				fmt.Printf("  错误: 键顺序错误! %s 应在 %s 之后\n", currKey, lastKey)
				correctKeyOrder = false
			}
		}

		// 检查时间戳顺序（在同一个键内）
		if !keySwitched && currKey == lastKey && currTs > lastTs {
			fmt.Printf("  错误: 同一键的时间戳顺序错误! %d 应小于 %d\n", currTs, lastTs)
			correctOrder = false
		}

		lastKey = currKey
		lastTs = currTs
		keySwitched = false

		iter.Next()
		count++
	}
	fmt.Printf("相邻键范围内找到 %d 个键值对\n", count)
	fmt.Printf("键的排序正确: %v, 时间戳排序正确: %v\n", correctKeyOrder, correctOrder)
}

// 测试边界条件范围查询
func testBoundaryConditionScan() {
	// 创建一个新的MemTable
	boundaryMemtable := memory.NewMemtableWithWAL(11, 1024, "./boundary-wal")
	defer os.RemoveAll("./boundary-wal")

	// 添加一组有序的键进行边界测试
	boundaryEntries := []struct {
		key   string
		ts    uint64
		value string
	}{
		{"aaa", 100, "first"},
		{"bbb", 200, "second"},
		{"ccc", 300, "third"},
		{"ddd", 400, "fourth"},
		{"eee", 500, "fifth"},
	}

	for _, entry := range boundaryEntries {
		key := kv.NewKey([]byte(entry.key), entry.ts)
		value := kv.NewValue([]byte(entry.value))
		boundaryMemtable.Set(key, value)
		fmt.Printf("添加: %s@%d = %s\n", entry.key, entry.ts, entry.value)
	}

	// 测试1: 范围的开始边界正好是第一个键
	fmt.Println("\n边界测试1: 开始边界 = 第一个键 [aaa@100, ccc@300]")
	startBoundaryRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("aaa"), 100),
		kv.NewKey([]byte("ccc"), 300),
	)

	iter := boundaryMemtable.Scan(startBoundaryRange)
	count := 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("开始边界范围内找到 %d 个键值对 (应为3)\n", count)

	// 测试2: 范围的结束边界正好是最后一个键
	fmt.Println("\n边界测试2: 结束边界 = 最后一个键 [ccc@300, eee@500]")
	endBoundaryRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("ccc"), 300),
		kv.NewKey([]byte("eee"), 500),
	)

	iter = boundaryMemtable.Scan(endBoundaryRange)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("结束边界范围内找到 %d 个键值对 (应为3)\n", count)

	// 测试3: 范围包含中间的键
	fmt.Println("\n边界测试3: 范围在中间 [bbb@200, ddd@400]")
	middleRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("bbb"), 200),
		kv.NewKey([]byte("ddd"), 400),
	)

	iter = boundaryMemtable.Scan(middleRange)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("中间范围内找到 %d 个键值对 (应为3)\n", count)

	// 测试4: 范围边界在键之间
	fmt.Println("\n边界测试4: 范围边界在键之间 [aab@100, dde@400]")
	betweenKeysRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("aab"), 100),
		kv.NewKey([]byte("dde"), 400),
	)

	iter = boundaryMemtable.Scan(betweenKeysRange)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("边界在键之间范围内找到 %d 个键值对 (应为3)\n", count)

	// 测试5: 精确匹配单键 - 开始和结束边界都是同一个键
	fmt.Println("\n边界测试5: 精确匹配单键 [ccc@300, ccc@300]")
	singleKeyRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("ccc"), 300),
		kv.NewKey([]byte("ccc"), 300),
	)

	iter = boundaryMemtable.Scan(singleKeyRange)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("精确匹配单键范围内找到 %d 个键值对 (应为1)\n", count)

	// 测试6: 跨越所有键的范围
	fmt.Println("\n边界测试6: 跨越所有键 [aaa@100, eee@500]")
	allKeysRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("aaa"), 100),
		kv.NewKey([]byte("eee"), 500),
	)

	iter = boundaryMemtable.Scan(allKeysRange)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("跨越所有键范围内找到 %d 个键值对 (应为5)\n", count)

	// 测试7: 范围完全在键外 - 应该不返回任何结果
	fmt.Println("\n边界测试7: 范围完全在键外 [fff@100, zzz@500]")
	outsideRange := kv.NewInclusiveKeyRange(
		kv.NewKey([]byte("fff"), 100),
		kv.NewKey([]byte("zzz"), 500),
	)

	iter = boundaryMemtable.Scan(outsideRange)
	count = 0
	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("  找到: %s@%d = %s\n",
			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
		iter.Next()
		count++
	}
	fmt.Printf("范围完全在键外范围内找到 %d 个键值对 (应为0)\n", count)
}

// func main() {
// 	Memtable := memory.NewMemtableWithWAL(1, 1024, wal.NewWALPath("./"))
// 	orderedKeys := []struct {
// 		key   string
// 		ts    uint64
// 		value string
// 	}{
// 		{"alpha", 100, "first letter"},
// 		{"beta", 200, "second letter"},
// 		{"gamma", 150, "third letter"},
// 		{"delta", 300, "fourth letter"},
// 		{"epsilon", 250, "fifth letter"},
// 		{"alpha", 50, "older version"}, // 相同键，不同时间戳
// 		{"beta", 50, "older beta"},     // 另一个不同时间戳的键
// 		{"gamma", 50, "older gamma"},   // 另一个不同时间戳的键
// 		{"zeta", 100, "last letter"},   // 最后一个字母
// 	}

// 	for _, keyValue := range orderedKeys {
// 		key := kv.NewKey([]byte(keyValue.key), keyValue.ts)
// 		value := kv.NewValue([]byte(keyValue.value))
// 		Memtable.Set(key, value)
// 		fmt.Printf("添加: %s@%d = %s\n", keyValue.key, keyValue.ts, keyValue.value)
// 	}

// 	Memtable.AllEntries(func(key kv.Key, value kv.Value) {
// 		fmt.Printf("跳表: %s@%d = %s\n",
// 			string(key.RawBytes()), key.Timestamp(), string(value.Value()))
// 	})

// 	// 测试1: 明确的闭区间范围查询 [beta:200, delta:300]
// 	fmt.Println("\n范围查询1: [beta@200, delta@300]")
// 	testRange := kv.NewInclusiveKeyRange(
// 		kv.NewKey([]byte("beta"), 200),
// 		kv.NewKey([]byte("delta"), 300),
// 	)
// 	dd := Memtable.Scan(testRange)
// 	for dd.IsValid() {
// 		fmt.Println(dd.Key().RawString())
// 		dd.Next()
// 	}

// }
