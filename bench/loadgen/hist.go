package main

import (
	"math"
	"math/bits"
	"sync/atomic"
)

// Log-linear latency histogram over nanoseconds: 64 linear sub-buckets per
// power of two, so any recorded value is reported within 1/64 (~1.6%) of
// the truth. Fixed size, no allocation per record, mergeable, and cheap
// enough to keep one per connection goroutine.
const (
	subBits    = 6
	subBuckets = 1 << subBits // 64
	maxExp     = 40           // 2^46 ns ~ 19.5 hours: nothing real exceeds it
	nBuckets   = (maxExp + 1) * subBuckets
)

type Hist struct {
	counts [nBuckets]uint64
	n      uint64
	sum    float64
	min    uint64
	max    uint64
}

func bucketOf(v uint64) int {
	if v < subBuckets {
		return int(v)
	}
	e := bits.Len64(v) - subBits - 1 // v>>e lands in [64,128)
	idx := (e+1)*subBuckets + int(v>>uint(e)) - subBuckets
	if idx >= nBuckets {
		return nBuckets - 1
	}
	return idx
}

// upperOf is the largest value bucket idx holds.
func upperOf(idx int) uint64 {
	if idx < subBuckets {
		return uint64(idx)
	}
	e := idx/subBuckets - 1
	m := uint64(idx%subBuckets + subBuckets)
	return ((m + 1) << uint(e)) - 1
}

func (h *Hist) Record(ns uint64) {
	h.counts[bucketOf(ns)]++
	h.n++
	h.sum += float64(ns)
	if h.n == 1 || ns < h.min {
		h.min = ns
	}
	if ns > h.max {
		h.max = ns
	}
}

func (h *Hist) Merge(o *Hist) {
	if o.n == 0 {
		return
	}
	for i, c := range o.counts {
		h.counts[i] += c
	}
	if h.n == 0 || o.min < h.min {
		h.min = o.min
	}
	if o.max > h.max {
		h.max = o.max
	}
	h.n += o.n
	h.sum += o.sum
}

// Quantile in nanoseconds (bucket upper bound, clamped to the exact max).
func (h *Hist) Quantile(q float64) uint64 {
	if h.n == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(h.n)))
	if rank < 1 {
		rank = 1
	}
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= rank {
			u := upperOf(i)
			if u > h.max {
				u = h.max
			}
			return u
		}
	}
	return h.max
}

func (h *Hist) Mean() float64 {
	if h.n == 0 {
		return 0
	}
	return h.sum / float64(h.n)
}

// Sparse dump: [upper_ns, count] for every non-empty bucket, so the run's
// full distribution is kept and can be merged or re-quantiled later.
func (h *Hist) Sparse() [][2]uint64 {
	out := [][2]uint64{}
	for i, c := range h.counts {
		if c > 0 {
			out = append(out, [2]uint64{upperOf(i), c})
		}
	}
	return out
}

// AtomicHist: shared per-second histograms for time-series output, where
// one histogram per goroutine per second would cost too much memory.
type AtomicHist struct {
	counts [nBuckets]uint64
	n      uint64
}

func (h *AtomicHist) Record(ns uint64) {
	atomic.AddUint64(&h.counts[bucketOf(ns)], 1)
	atomic.AddUint64(&h.n, 1)
}

func (h *AtomicHist) Snapshot() *Hist {
	out := &Hist{}
	for i := range h.counts {
		c := atomic.LoadUint64(&h.counts[i])
		if c > 0 {
			out.counts[i] = c
			out.n += c
			u := upperOf(i)
			out.sum += float64(u) * float64(c)
			if out.min == 0 || u < out.min {
				out.min = u
			}
			if u > out.max {
				out.max = u
			}
		}
	}
	return out
}
