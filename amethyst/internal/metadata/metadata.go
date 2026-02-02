package metadata

import (
	"amethyst/internal/common"
	"sync"
)

type Tracker interface {
	RegisterSegment(meta *common.SegmentMeta)
	GetSegmentsForKey(key string) []*common.SegmentMeta
	GetAllSegments() []*common.SegmentMeta
	GetOverlappingSegments(target *common.SegmentMeta) []*common.SegmentMeta

	MarkObsolete(id string)
	UpdateStats(id string, reads int64, writes int64)
}

type tracker struct {
	mu       sync.RWMutex
	segments map[string]*common.SegmentMeta
	ordered  []*common.SegmentMeta // newest first ordered
}

// NewTracker creates a new MetadataTracker.
func NewTracker() Tracker {
	return &tracker{
		segments: make(map[string]*common.SegmentMeta),
		ordered:  make([]*common.SegmentMeta, 0),
	}
}

func (t *tracker) RegisterSegment(meta *common.SegmentMeta) {
	t.mu.Lock() // MUST acquire write lock to modify internal maps and slices
	defer t.mu.Unlock()

	// Calculate overlap count - count how many existing segments overlap with this new one
	var overlaps int64
	for _, other := range t.ordered {
		if other.Obsolete {
			continue
		}
		// Range overlap check: NOT (A is entirely before B OR A is entirely after B)
		// If ranges overlap, increment count
		if !(meta.MaxKey < other.MinKey || meta.MinKey > other.MaxKey) {
			overlaps++
			// BIDIRECTIONAL: also increment the other segment's overlap count
			other.OverlapCount++
		}
	}
	meta.OverlapCount = overlaps

	// Register the segment
	t.segments[meta.ID] = meta
	// prepend so newest segments come first
	t.ordered = append([]*common.SegmentMeta{meta}, t.ordered...)
}

func (t *tracker) GetSegmentsForKey(key string) []*common.SegmentMeta {
	result := make([]*common.SegmentMeta, 0)

	for _, seg := range t.ordered {
		if seg.Obsolete {
			continue
		}
		if key >= seg.MinKey && key <= seg.MaxKey {
			result = append(result, seg)
		}
	}
	return result
}

func (t *tracker) GetAllSegments() []*common.SegmentMeta {
	t.mu.RLock() // Add read lock
	defer t.mu.RUnlock()

	result := make([]*common.SegmentMeta, 0, len(t.ordered))
	for _, seg := range t.ordered {
		if !seg.Obsolete {
			result = append(result, seg)
		}
	}
	return result
}

// GetOverlappingSegments returns all non-obsolete segments that overlap with the target segment
func (t *tracker) GetOverlappingSegments(target *common.SegmentMeta) []*common.SegmentMeta {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]*common.SegmentMeta, 0)
	for _, seg := range t.ordered {
		// Don't overlap with yourself or obsolete files
		if seg.Obsolete || seg.ID == target.ID {
			continue
		}

		// THE CORE LEVELED LOGIC:
		// Check if the ranges [min, max] intersect
		// They overlap UNLESS (seg is entirely to the left) OR (seg is entirely to the right)
		if !(seg.MaxKey < target.MinKey || seg.MinKey > target.MaxKey) {
			result = append(result, seg)
		}
	}
	return result
}

func (t *tracker) MarkObsolete(id string) {
	if seg, ok := t.segments[id]; ok {
		seg.Obsolete = true
	}
}

func (t *tracker) UpdateStats(id string, reads int64, writes int64) {
	if seg, ok := t.segments[id]; ok {
		seg.ReadCount += reads
		seg.WriteCount += writes
	}
}
