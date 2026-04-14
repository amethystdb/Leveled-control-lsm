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
	mu           sync.RWMutex
	segments     map[string]*common.SegmentMeta
	ordered      []*common.SegmentMeta
	sortedCache  []*common.SegmentMeta  // Cached sorted, non-obsolete segments
	cacheValid   bool                   // Whether sortedCache is up-to-date
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
	t.ordered = append([]*common.SegmentMeta{meta}, t.ordered...)
	t.cacheValid = false  // Invalidate cache on segment registration
}

func (t *tracker) GetSegmentsForKey(key string) []*common.SegmentMeta {
	t.mu.RLock()
	defer t.mu.RUnlock()

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
	// Fast path: cache hit under read lock
	t.mu.RLock()
	if t.cacheValid {
		result := make([]*common.SegmentMeta, len(t.sortedCache))
		copy(result, t.sortedCache)
		t.mu.RUnlock()
		return result
	}
	t.mu.RUnlock()

	// Slow path: rebuild under write lock
	t.mu.Lock()
	defer t.mu.Unlock()

	// Re-check after acquiring write lock (another goroutine may have rebuilt)
	if t.cacheValid {
		result := make([]*common.SegmentMeta, len(t.sortedCache))
		copy(result, t.sortedCache)
		return result
	}

	result := make([]*common.SegmentMeta, 0, len(t.ordered))
	for _, seg := range t.ordered {
		if !seg.Obsolete {
			result = append(result, seg)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].MinKey < result[j].MinKey
	})
	t.sortedCache = result
	t.cacheValid = true

	out := make([]*common.SegmentMeta, len(result))
	copy(out, result)
	return out
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
	t.mu.Lock()
	defer t.mu.Unlock()

	if seg, ok := t.segments[id]; ok {
		seg.Obsolete = true
		t.cacheValid = false  // Invalidate cache when segment becomes obsolete
	}
}

func (t *tracker) UpdateStats(id string, reads int64, writes int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if seg, ok := t.segments[id]; ok {
		seg.ReadCount += reads
		seg.WriteCount += writes
	}
}
