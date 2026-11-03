package lsm

import "math/bits"

func hashKey64(data []byte) uint64 {
	var h uint64 = 14695981039346656037 // FNV-1a 64-bit offset basis
	for _, b := range data {
		h ^= uint64(b)
		h *= 1099511628211
	}
	// Finalizer mix
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	return h
}

// BuildBloomFilter constructs a serialized Bloom filter bitset for the given user keys.
// The last byte stores k (the number of hash probes).
func BuildBloomFilter(userKeys [][]byte, bitsPerKey int) []byte {
	if bitsPerKey <= 0 {
		bitsPerKey = 10
	}
	k := int(float64(bitsPerKey) * 0.69) // ln(2) * bitsPerKey
	k = max(1, min(k, 30))

	nBits := max(64, len(userKeys)*bitsPerKey)
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8

	filter := make([]byte, nBytes+1)
	filter[nBytes] = byte(k)

	for _, key := range userKeys {
		h := hashKey64(key)
		delta := bits.RotateLeft64(h, 31) | 1
		for j := 0; j < k; j++ {
			bitPos := uint32((h + uint64(j)*delta) % uint64(nBits))
			filter[bitPos/8] |= 1 << (bitPos % 8)
		}
	}
	return filter
}

// BloomMayContain returns false if userKey is definitely not in the SSTable, or true if it may be present.
func BloomMayContain(filter []byte, userKey []byte) bool {
	if len(filter) < 2 {
		return true
	}
	k := int(filter[len(filter)-1])
	if k > 30 {
		return true
	}
	nBytes := len(filter) - 1
	nBits := uint64(nBytes * 8)

	h := hashKey64(userKey)
	delta := bits.RotateLeft64(h, 31) | 1
	for j := 0; j < k; j++ {
		bitPos := uint32((h + uint64(j)*delta) % nBits)
		if (filter[bitPos/8] & (1 << (bitPos % 8))) == 0 {
			return false
		}
	}
	return true
}