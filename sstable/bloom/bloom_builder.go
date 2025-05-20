package bloom

import "go-lsm-db/kv"

type BloomFilterBuilder struct {
	keys              []kv.Key
	expectedElements  int
	falsePositiveRate float64
}

func NewBloomFilterBuilder(expectedElements int, falsePositiveRate float64) *BloomFilterBuilder {
	return &BloomFilterBuilder{
		keys:              make([]kv.Key, 0),
		expectedElements:  expectedElements,
		falsePositiveRate: falsePositiveRate,
	}
}

func (b *BloomFilterBuilder) Add(key kv.Key) {
	b.keys = append(b.keys, key)
}

func (b *BloomFilterBuilder) Build() *BloomFilter {
	bitVectorSize := optimalBitVectorSize(b.expectedElements, b.falsePositiveRate)
	numberOfHashFunctions := optimalNumberOfHashFunctions(bitVectorSize, uint(b.expectedElements))

	bf := NewBloomFilter(numberOfHashFunctions, b.falsePositiveRate, uint(bitVectorSize))
	for _, key := range b.keys {
		bf.add(key)
	}

	return bf
}
