package main

import (
	"fmt"
	"go-lsm-db/kv"
	"go-lsm-db/sstable"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

// TestKeyValue 结构体用于方便地组织测试数据
type TestKeyValue struct {
	key   []byte
	ts    uint64
	value []byte
}

const testSSTableDir = "./test_sstable_data"
const testSSTableID = 1
const testBlockSize = 128 // 较小的块大小以便测试多个块

func main() {
	// 设置随机种子
	rand.Seed(time.Now().UnixNano())

	// 创建测试目录
	if err := os.MkdirAll(testSSTableDir, 0755); err != nil {
		fmt.Printf("创建测试目录失败: %v\n", err)
		return
	}
	defer os.RemoveAll(testSSTableDir) // 测试结束后清理

	fmt.Println("=== SSTable 功能测试 ===")

	// 1. 运行基本的SSTable构建和加载测试
	fmt.Println("\n--- 1. 基本SSTable构建和加载测试 ---")
	initialTestData := runBasicSSTableTest(testSSTableID, testSSTableDir, testBlockSize)
	if initialTestData == nil {
		fmt.Println("基本SSTable测试失败，后续测试可能无意义。")
		return
	}

	// 重新加载SSTable以用于后续测试
	loadedTable, err := sstable.Load(testSSTableID, testSSTableDir, testBlockSize)
	if err != nil {
		fmt.Printf("为后续测试重新加载SSTable失败: %v\n", err)
		return
	}

	// 2. 验证Bloom Filter
	fmt.Println("\n--- 2. Bloom Filter 验证 ---")
	testBloomFilterVerification(loadedTable, initialTestData)

	// 3. 测试区块查找和随机访问
	fmt.Println("\n--- 3. 区块查找和随机访问测试 ---")
	testRandomAccessAndBlockLookup(loadedTable, initialTestData)

	fmt.Println("\n=== 所有测试完成 ===")
}

// runBasicSSTableTest 测试SSTable的构建和加载
func runBasicSSTableTest(id uint64, dir string, blockSize uint) []TestKeyValue {
	builder := sstable.NewSSTableBuilder(uint32(blockSize), 7, 0.01) // 10个预期元素, 1%假阳性率

	testData := []TestKeyValue{
		{[]byte("cherry"), 101, []byte("small red fruit")},
		{[]byte("date"), 103, []byte("brown sweet fruit")},
		{[]byte("apple"), 100, []byte("red fruit")},
		{[]byte("apple"), 90, []byte("old red fruit")},
		{[]byte("banana"), 102, []byte("yellow fruit")},
		{[]byte("elderberry"), 100, []byte("dark purple fruit")}, // 时间戳相同，但key不同
		{[]byte("apple"), 105, []byte("new red fruit")},
	}

	fmt.Println("构建SSTable...")
	for _, data := range testData {
		builder.Add(kv.NewKey(data.key, data.ts), kv.NewValue(data.value))
		fmt.Printf("  添加: Key=%s, TS=%d, Value=%s\n", string(data.key), data.ts, string(data.value))
	}

	table, err := builder.Build(id, dir)
	if err != nil {
		fmt.Printf("构建SSTable失败: %v\n", err)
		return nil
	}
	fmt.Printf("SSTable构建成功. ID: %d, Path: %s\n", table.Id(), filepath.Join(dir, fmt.Sprintf("%d.sst", id)))
	fmt.Printf("  SSTable范围: [%s] - [%s]\n", table.StartingKey().RawString(), table.EndingKey().RawString())

	// 加载SSTable
	fmt.Println("\n加载SSTable...")
	loadedTable, err := sstable.Load(id, dir, blockSize)
	if err != nil {
		fmt.Printf("加载SSTable失败: %v\n", err)
		return nil
	}
	fmt.Println("SSTable加载成功.")
	fmt.Printf("  加载的SSTable范围: [%s] - [%s]\n", loadedTable.StartingKey().RawString(), loadedTable.EndingKey().RawString())

	// 验证键范围
	if table.StartingKey().RawString() != loadedTable.StartingKey().RawString() ||
		table.EndingKey().RawString() != loadedTable.EndingKey().RawString() {
		fmt.Println("错误: 原表和加载表的键范围不匹配!")
	} else {
		fmt.Println("成功: 键范围匹配.")
	}

	// 验证所有键值对
	fmt.Println("\n验证所有键值对 (通过迭代):")
	if !verifyAllKeyValuePairs(loadedTable, testData) {
		fmt.Println("错误: 键值对验证失败.")
		return nil
	}
	fmt.Println("成功: 所有键值对均正确加载并通过迭代验证.")
	return testData
}

// verifyAllKeyValuePairs 迭代SSTable并验证所有键值对
func verifyAllKeyValuePairs(table *sstable.SSTable, expected []TestKeyValue) bool {
	iter, err := table.SeekToFirst()
	if err != nil {
		fmt.Printf("  获取迭代器失败: %v\n", err)
		return false
	}

	foundCount := 0
	expectedMap := make(map[string]TestKeyValue)
	for _, data := range expected {
		// Key的字符串表示作为map的键，因为kv.Key本身不能直接做map键
		expectedMap[kv.NewKey(data.key, data.ts).RawString()] = data
	}

	for iter.IsValid() {
		key := iter.Key()
		value := iter.Value()
		// fmt.Printf("  迭代器找到: Key=%s, TS=%d, Value=%s\n", string(key.UserKey()), key.Timestamp(), string(value.Value()))

		expectedData, ok := expectedMap[key.RawString()]
		if !ok {
			fmt.Printf("  错误: 迭代器找到了未预期的键: %s\n", key.RawString())
			return false
		}

		if value.String() != string(expectedData.value) {
			fmt.Printf("  错误: 键 %s 的值不匹配. 预期: %s, 实际: %s\n",
				key.RawString(), string(expectedData.value), value.String())
			return false
		}
		foundCount++
		delete(expectedMap, key.RawString()) // 从map中移除已找到的项
		iter.Next()
	}

	if len(expectedMap) > 0 {
		fmt.Printf("  错误: 未能从SSTable中找到以下预期的键值对:\n")
		for kStr, data := range expectedMap {
			_ = kStr // 避免 "declared and not used"
			fmt.Printf("    Key=%s, TS=%d, Value=%s\n", string(data.key), data.ts, string(data.value))
		}
		return false
	}

	if foundCount != len(expected) {
		fmt.Printf("  错误: 找到的键值对数量 (%d) 与预期数量 (%d) 不符.\n", foundCount, len(expected))
		return false
	}
	return true
}

func testBloomFilterVerification(table *sstable.SSTable, existingData []TestKeyValue) {
	fmt.Println("测试已存在的键 (应全部返回 true for MayContain):")
	for _, data := range existingData {
		key := kv.NewKey(data.key, data.ts)
		if table.MayContain(key) {
			fmt.Printf("  Key: %s, TS: %d -> MayContain: true (正确)\n", string(data.key), data.ts)
		} else {
			fmt.Printf("  错误: Key: %s, TS: %d -> MayContain: false (错误, 应为true)\n", string(data.key), data.ts)
		}
	}

	fmt.Println("\n生成10000个不存在的键进行测试...")
	// 生成用于测试的随机键
	nonExistingKeys := generateNonExistingKeys(10000, existingData)

	// 测试并计数假阳性
	falsePositives := 0
	testedCount := 0
	for _, data := range nonExistingKeys {
		testedCount++
		key := kv.NewKey(data.key, data.ts)

		if table.MayContain(key) {
			falsePositives++

			// 仅打印前几个假阳性以减少输出量
			if falsePositives <= 10 {
				fmt.Printf("  Key: %s, TS: %d -> MayContain: true (假阳性)\n", string(data.key), data.ts)

				// 进一步验证确实不存在
				iter, _ := table.SeekToKey(key)
				if iter.IsValid() && compareKeys(iter.Key(), key) {
					fmt.Printf("    警告: Bloom Filter说可能存在，且Seek确实找到了键 %s. 这不应发生!\n", key.RawString())
				}
			}
		}

		// 每测试1000个样本打印一次进度
		if testedCount%1000 == 0 {
			fmt.Printf("  已测试 %d 个键, 目前发现 %d 个假阳性 (%.2f%%)\n",
				testedCount, falsePositives, float64(falsePositives)*100/float64(testedCount))
		}
	}

	// 打印最终统计结果
	falsePositiveRate := float64(falsePositives) / float64(len(nonExistingKeys)) * 100
	fmt.Printf("\n测试完成: 在 %d 个不存在的键中检测到 %d 个假阳性 (%.4f%%).\n",
		len(nonExistingKeys), falsePositives, falsePositiveRate)
	fmt.Printf("预期假阳性率: 1.00%%, 实际假阳性率: %.4f%%\n", falsePositiveRate)
}

// 生成不存在于SSTable中的随机键
func generateNonExistingKeys(count int, existingData []TestKeyValue) []TestKeyValue {
	result := make([]TestKeyValue, 0, count)
	existingKeys := make(map[string]bool)

	// 首先将现有键添加到map中
	for _, data := range existingData {
		key := string(data.key) + "@" + fmt.Sprintf("%d", data.ts)
		existingKeys[key] = true
	}

	// 生成两种类型的不存在键:
	// 1. 完全随机的用户键
	// 2. 现有用户键但时间戳不同

	// 现有用户键列表(不包含重复)
	existingUserKeys := make([][]byte, 0)
	existingUserKeysMap := make(map[string]bool)
	for _, data := range existingData {
		userKey := string(data.key)
		if !existingUserKeysMap[userKey] {
			existingUserKeysMap[userKey] = true
			existingUserKeys = append(existingUserKeys, data.key)
		}
	}

	// 字符集用于生成随机键
	charset := "abcdefghijklmnopqrstuvwxyz"

	for len(result) < count {
		var newKey TestKeyValue

		// 有20%的概率使用现有用户键但不同时间戳
		if rand.Float32() < 0.2 && len(existingUserKeys) > 0 {
			userKey := existingUserKeys[rand.Intn(len(existingUserKeys))]
			// 生成一个不存在的时间戳
			ts := uint64(rand.Intn(10000))

			// 确保这个时间戳不存在
			keyStr := string(userKey) + "@" + fmt.Sprintf("%d", ts)
			for existingKeys[keyStr] {
				ts = uint64(rand.Intn(10000))
				keyStr = string(userKey) + "@" + fmt.Sprintf("%d", ts)
			}

			newKey = TestKeyValue{
				key:   userKey,
				ts:    ts,
				value: []byte(""),
			}
		} else {
			// 生成完全随机的用户键
			keyLength := 3 + rand.Intn(10) // 3-12字符长度的键
			keyBytes := make([]byte, keyLength)
			for i := 0; i < keyLength; i++ {
				keyBytes[i] = charset[rand.Intn(len(charset))]
			}

			ts := uint64(rand.Intn(10000))

			// 确保这个键不存在
			keyStr := string(keyBytes) + "@" + fmt.Sprintf("%d", ts)
			for existingKeys[keyStr] {
				ts = uint64(rand.Intn(10000))
				keyStr = string(keyBytes) + "@" + fmt.Sprintf("%d", ts)
			}

			newKey = TestKeyValue{
				key:   keyBytes,
				ts:    ts,
				value: []byte(""),
			}
		}

		// 将新键添加到结果和existingKeys中
		keyStr := string(newKey.key) + "@" + fmt.Sprintf("%d", newKey.ts)
		if !existingKeys[keyStr] {
			existingKeys[keyStr] = true
			result = append(result, newKey)
		}
	}

	return result
}

// testRandomAccessAndBlockLookup 测试随机访问和区块查找
func testRandomAccessAndBlockLookup(table *sstable.SSTable, testData []TestKeyValue) {
	fmt.Println("测试随机Seek:")

	// 组合测试用例：一些存在的，一些不存在的
	seekTestCases := []struct {
		keyToSeek   TestKeyValue
		expectFound bool
	}{
		{testData[0], true},                                                    // 第一个存在的键
		{testData[len(testData)/2], true},                                      // 中间一个存在的键
		{testData[len(testData)-1], true},                                      // 最后一个存在的键
		{TestKeyValue{[]byte("banana"), 102, []byte("yellow fruit")}, true},    // 明确存在的
		{TestKeyValue{[]byte("apple"), 90, []byte("old red fruit")}, true},     // 明确存在的（旧版本）
		{TestKeyValue{[]byte("mango"), 100, []byte("")}, false},                // 完全不存在的键
		{TestKeyValue{[]byte("cherry"), 101, []byte("small red fruit")}, true}, // 键存在，值用于比较
		{TestKeyValue{[]byte("apple"), 105, []byte("new red fruit")}, true},
	}

	for _, tc := range seekTestCases {
		key := kv.NewKey(tc.keyToSeek.key, tc.keyToSeek.ts)
		fmt.Printf("  Seeking for Key: %s, TS: %d (期望找到: %t)\n", string(tc.keyToSeek.key), tc.keyToSeek.ts, tc.expectFound)

		iter, err := table.SeekToKey(key)
		if err != nil {
			fmt.Printf("    Seek时发生错误: %v\n", err)
			continue
		}

		if tc.expectFound {
			if !iter.IsValid() {
				fmt.Printf("    错误: 期望找到键 %s, 但迭代器无效.\n", key.RawString())
				continue
			}
			if !compareKeys(iter.Key(), key) {
				fmt.Printf("    错误: 期望找到键 %s, 但找到的是 %s.\n", key.RawString(), iter.Key().RawString())
				continue
			}
			// 验证值
			if string(iter.Value().Value()) != string(tc.keyToSeek.value) {
				fmt.Printf("    错误: 键 %s 的值不匹配. 预期: %s, 实际: %s\n",
					key.RawString(), string(tc.keyToSeek.value), string(iter.Value().Value()))
			} else {
				fmt.Printf("    成功: 找到键 %s, 值: %s\n", key.RawString(), string(iter.Value().Value()))
			}
		} else { // expectFound == false
			if iter.IsValid() && compareKeys(iter.Key(), key) {
				fmt.Printf("    错误: 期望找不到键 %s, 但找到了: %s.\n", key.RawString(), iter.Key().RawString())
			} else {
				if iter.IsValid() {
					fmt.Printf("    成功: 未找到精确匹配的键 %s (迭代器当前位置: %s).\n", key.RawString(), iter.Key().RawString())
				} else {
					fmt.Printf("    成功: 未找到精确匹配的键 %s (迭代器无效).\n", key.RawString())
				}
			}
		}
	}
}

// compareKeys 比较两个kv.Key是否完全相等
func compareKeys(k1, k2 kv.Key) bool {
	// 注意：kv.Key.RawString() 包含了userkey和timestamp的组合形式
	return k1.RawString() == k2.RawString()
}
