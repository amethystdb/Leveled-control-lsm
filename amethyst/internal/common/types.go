package common
import "sync/atomic"

type CompactionType int

const (
	LEVELED CompactionType = iota
)

type SegmentMeta struct {
	ID     string
	Offset int64
	Length int64

	MinKey string
	MaxKey string

	Strategy CompactionType

	ReadCount    int64
	WriteCount   int64
	OverlapCount int64

	CreatedAt     int64
	LastRewriteAt int64

	Obsolete          bool
	SparseIndex       interface{}
	DataStartOffset   int64
	SparseIndexOffset int64
}

func (s *SegmentMeta) Size() int64 {
	return s.Length
}

func (s *SegmentMeta) ReadWriteRatio() float64 {
	if s.WriteCount == 0 {
		return float64(s.ReadCount)
	}
	return float64(s.ReadCount) / float64(s.WriteCount)
}

func (s *SegmentMeta) CooldownExpired(now int64, minInterval int64) bool {
	return now-s.LastRewriteAt >= minInterval
}

type WALEntry struct {
	Key       string
	Value     []byte
	Tombstone bool
}

type KVEntry struct {
	Key       string
	Value     []byte
	Tombstone bool
}

//
// 🔴 GLOBAL METRICS (aligned with adaptive)
//

var PhysicalWriteBytes int64
var PhysicalReadBytes int64
var SegmentReadCount int64

func AddWriteBytes(n int64) {
	atomic.AddInt64(&PhysicalWriteBytes, n)
}

func AddReadBytes(n int64) {
	atomic.AddInt64(&PhysicalReadBytes, n)
}

func IncSegmentRead() {
	atomic.AddInt64(&SegmentReadCount, 1)
}

