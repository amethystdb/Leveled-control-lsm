package metadata

import (
	"amethyst/internal/common"
	"sort"
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
	ordered  []*common.SegmentMeta
}

// NewTracker creates a new MetadataTracker.
func NewTracker() Tracker {
	return &tracker{
		segments: make(map[string]*common.SegmentMeta),
		ordered:  make([]*common.SegmentMeta, 0),
	}
}

func (t *tracker) RegisterSegment(meta *common.SegmentMeta) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.segments[meta.ID] = meta

	// Compute overlaps with existing segments
	for _, seg := range t.ordered {
		// overlap check
		if !(seg.MaxKey < meta.MinKey || seg.MinKey > meta.MaxKey) {
			seg.OverlapCount++
			meta.OverlapCount++
		}
	}

	// Add new segment to ordered list (front = newest)
	t.ordered = append([]*common.SegmentMeta{meta}, t.ordered...)
}

func (t *tracker) GetSegmentsForKey(key string) []*common.SegmentMeta {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]*common.SegmentMeta, 0)

	for _, seg := range t.ordered {
		if key >= seg.MinKey && key <= seg.MaxKey {
			result = append(result, seg)
		}
	}
	return result
}

func (t *tracker) GetAllSegments() []*common.SegmentMeta {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]*common.SegmentMeta, 0, len(t.ordered))
	for _, seg := range t.ordered {
		result = append(result, seg)
	}

	// LEVELED REQUIREMENT: Sort by MinKey so Binary Search in main.go works
	sort.Slice(result, func(i, j int) bool {
		return result[i].MinKey < result[j].MinKey
	})

	return result
}

// GetOverlappingSegments returns all segments that overlap with the target segment
func (t *tracker) GetOverlappingSegments(target *common.SegmentMeta) []*common.SegmentMeta {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]*common.SegmentMeta, 0)
	for _, seg := range t.ordered {
		// Don't overlap with yourself
		if seg.ID == target.ID {
			continue
		}

		// Check if ranges intersect
		if !(seg.MaxKey < target.MinKey || seg.MinKey > target.MaxKey) {
			result = append(result, seg)
		}
	}
	return result
}

func (t *tracker) MarkObsolete(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	seg, ok := t.segments[id]
	if !ok || seg.Obsolete {
		return
	}

	// First: decrement overlap counts of other segments
	for _, other := range t.ordered {
		if other.ID == seg.ID {
			continue
		}

		if !(other.MaxKey < seg.MinKey || other.MinKey > seg.MaxKey) {
			if other.OverlapCount > 0 {
				other.OverlapCount--
			}
		}
	}

	// Mark obsolete
	seg.Obsolete = true
	seg.OverlapCount = 0

	// Remove from ordered slice
	newOrdered := make([]*common.SegmentMeta, 0, len(t.ordered))
	for _, s := range t.ordered {
		if s.ID != seg.ID {
			newOrdered = append(newOrdered, s)
		}
	}
	t.ordered = newOrdered
}

func (t *tracker) UpdateStats(id string, reads int64, writes int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if seg, ok := t.segments[id]; ok {
		seg.ReadCount += reads
		seg.WriteCount += writes
	}
}

