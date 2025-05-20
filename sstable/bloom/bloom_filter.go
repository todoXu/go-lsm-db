package bloom

import (
	"encoding/binary"
	"go-lsm-db/kv"
	"go-lsm-db/sstable/block"
	"math"

	"github.com/bits-and-blooms/bitset"
	"github.com/spaolacci/murmur3"
)

type BloomFilter struct {
	numberOfHashFunctions uint8
	falsePositiveRate     float64        //假阳率
	bitVec                *bitset.BitSet //位数列
}

func NewBloomFilter(numberOfHashFunctions uint8, falsePositiveRate float64, bitVecSize uint) *BloomFilter {
	return &BloomFilter{
		numberOfHashFunctions: numberOfHashFunctions,
		falsePositiveRate:     falsePositiveRate,
		bitVec:                bitset.New(bitVecSize),
	}
}

func DecodeToBloomFilter(buffer []byte) (BloomFilter, error) {
	//在解码时，我们事先不知道位向量的确切大小，这个信息包含在编码数据中
	//UnmarshalBinary 方法会从二进制数据中读取位向量的大小信息，并自动调整 bitArray 对象的内部存储
	bitVec := new(bitset.BitSet)
	filter := buffer[0 : len(buffer)-block.Uint8Size-8]
	err := bitVec.UnmarshalBinary(filter)
	if err != nil {
		return BloomFilter{}, err
	}

	numberOfHashFunctions := uint8(buffer[len(buffer)-block.Uint8Size-8])

	fpBytes := buffer[len(buffer)-8:]
	falsePositiveRate := math.Float64frombits(binary.BigEndian.Uint64(fpBytes))

	return BloomFilter{
		numberOfHashFunctions: numberOfHashFunctions,
		falsePositiveRate:     falsePositiveRate,
		bitVec:                bitVec,
	}, nil
}

func (filter *BloomFilter) Encode() ([]byte, error) {
	buffer, err := filter.bitVec.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buffer = append(buffer, filter.numberOfHashFunctions)

	fpBytes := make([]byte, 8) // float64需要8字节
	binary.BigEndian.PutUint64(fpBytes, math.Float64bits(filter.falsePositiveRate))
	buffer = append(buffer, fpBytes...)

	return buffer, nil
}

func (filter *BloomFilter) bitPositionsFor(key kv.Key) []uint32 {
	positions := make([]uint32, 0)

	keyBytes := key.RawBytes()
	keyTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(keyTimestampBytes, key.Timestamp())
	keyBytes = append(keyBytes, keyTimestampBytes...)

	for index := uint8(0); index < filter.numberOfHashFunctions; index++ {
		hash := murmur3.Sum32WithSeed(keyBytes, uint32(index))
		positions = append(positions, hash%uint32(filter.bitVec.Len()))
	}
	return positions
}

// 数列里的1可能重叠 导致假阳性
func (filter *BloomFilter) add(key kv.Key) {
	positions := filter.bitPositionsFor(key)
	for _, position := range positions {
		filter.bitVec.Set(uint(position))
	}
}

// 全1就可能存在 0就一定不存在
func (filter *BloomFilter) MayContain(key kv.Key) bool {
	positions := filter.bitPositionsFor(key)
	for _, position := range positions {
		if !filter.bitVec.Test(uint(position)) {
			return false
		}
	}
	return true
}

// 计算最优哈希函数数量
// k = (m/n) * ln(2)
func optimalNumberOfHashFunctions(bitVectorSize uint, expectedElements uint) uint8 {
	if expectedElements <= 0 {
		return 1
	}

	hashFuncs := (float64(bitVectorSize) / float64(expectedElements)) * math.Log(2)

	if hashFuncs < 1 {
		return 1
	}
	if hashFuncs > 255 {
		return 255
	}

	return uint8(math.Round(hashFuncs))
}

// 计算最佳位向量大小
//
//	m = -n * ln(p) / (ln(2)^2)
func optimalBitVectorSize(expectedElements int, falsePositiveRate float64) uint {
	numerator := -float64(expectedElements) * math.Log(falsePositiveRate)
	denominator := math.Pow(math.Log(2), 2)
	return uint(math.Ceil(numerator / denominator))
}
