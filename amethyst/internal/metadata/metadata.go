package metadata

import "amethyst/internal/common"

type Tracker interface {
	RegisterSegment(meta *common.SegmentMeta)
	GetSegmentsForKey(key string) []*common.SegmentMeta
	GetAllSegments() []*common.SegmentMeta

	MarkObsolete(id string)
	UpdateStats(id string, reads int64, writes int64)
}

type tracker struct {
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
	// 1. Calculate overlap count before registering
	var overlaps int64
	for _, other := range t.ordered {
		if other.Obsolete {
			continue
		}
		// Range overlap check: Not (A is entirely before B OR A is entirely after B)
		if !(meta.MaxKey < other.MinKey || meta.MinKey > other.MaxKey) {
			overlaps++
		}
	}
	meta.OverlapCount = overlaps

	// 2. Register the segment
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
	result := make([]*common.SegmentMeta, 0, len(t.ordered))

	for _, seg := range t.ordered {
		if !seg.Obsolete {
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
