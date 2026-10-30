package sim

import (
	"crypto/sha256"
	"encoding/binary"
	"math/bits"
)

// StreamRand is a deterministic xoshiro256** PRNG initialized from (masterSeed, streamName).
type StreamRand struct {
	s [4]uint64
}

// NewStreamRand derives an independent PRNG stream from a master seed and stream identifier.
func NewStreamRand(masterSeed uint64, streamName string) *StreamRand {
	var seedBuf [8]byte
	binary.LittleEndian.PutUint64(seedBuf[:], masterSeed)

	h := sha256.New()
	h.Write(seedBuf[:])
	h.Write([]byte(":"))
	h.Write([]byte(streamName))
	digest := h.Sum(nil) // 32 bytes = 4 x uint64

	r := &StreamRand{}
	for i := 0; i < 4; i++ {
		r.s[i] = binary.LittleEndian.Uint64(digest[i*8 : (i+1)*8])
	}
	// Guard against the all-zero state
	if r.s[0] == 0 && r.s[1] == 0 && r.s[2] == 0 && r.s[3] == 0 {
		r.s[0] = 0x9e3779b97f4a7c15
	}
	return r
}

// Uint64 returns the next deterministic 64-bit value using xoshiro256**.
func (r *StreamRand) Uint64() uint64 {
	result := bits.RotateLeft64(r.s[1]*5, 7) * 9
	t := r.s[1] << 17

	r.s[2] ^= r.s[0]
	r.s[3] ^= r.s[1]
	r.s[1] ^= r.s[2]
	r.s[0] ^= r.s[3]

	r.s[2] ^= t
	r.s[3] = bits.RotateLeft64(r.s[3], 45)
	return result
}

// Intn returns a uniform random int in [0, n). Panics if n <= 0.
func (r *StreamRand) Intn(n int) int {
	if n <= 0 {
		panic("sim.Rand.Intn: n must be > 0")
	}
	return int(r.Int63n(int64(n)))
}

// Int63n returns a uniform random int64 in [0, n) without modulo bias.
func (r *StreamRand) Int63n(n int64) int64 {
	if n <= 0 {
		panic("sim.Rand.Int63n: n must be > 0")
	}
	un := uint64(n)
	threshold := (-un) % un
	for {
		v := r.Uint64()
		if v >= threshold {
			return int64(v % un)
		}
	}
}

// Float64 returns a uniform float64 in [0.0, 1.0).
func (r *StreamRand) Float64() float64 {
	return float64(r.Uint64()>>11) * (1.0 / (1 << 53))
}
