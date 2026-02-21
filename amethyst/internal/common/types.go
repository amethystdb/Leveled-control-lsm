package common

type CompactionType int

const (
	// Keep these identical across all repos for standard results
	LEVELED CompactionType = iota // 1
)

type SegmentMeta struct {
	ID     string
	Level  int
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

// Size returns the on-disk size of the segment in bytes for compaction decision
func (s *SegmentMeta) Size() int64 {
	return s.Length
}

// ReadWriteRatio returns reads / writes, guarding against divide-by-zero.
func (s *SegmentMeta) ReadWriteRatio() float64 {
	if s.WriteCount == 0 {
		return float64(s.ReadCount)
	}
	return float64(s.ReadCount) / float64(s.WriteCount)
}

// CooldownExpired returns true if enough time has passed since last rewrite.
func (s *SegmentMeta) CooldownExpired(now int64, minInterval int64) bool {
	return now-s.LastRewriteAt >= minInterval
}

type WALEntry struct {
	Key       string
	Value     []byte
	Tombstone bool
}

// our odered key equivalentt
type KVEntry struct {
	Key       string
	Value     []byte
	Tombstone bool
}
