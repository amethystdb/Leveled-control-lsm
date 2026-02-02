package compaction

import (
	"amethyst/internal/common"
	"amethyst/internal/controller"
	"amethyst/internal/metadata"
)

type Plan struct {
	Inputs         []*common.SegmentMeta
	OutputStrategy common.CompactionType
	Reason         string
}

type Director interface {
	MaybePlan() *Plan
}

type Controller interface {
	ShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string)
}

type director struct {
	meta metadata.Tracker
	ctrl Controller
}

func NewDirector(meta metadata.Tracker, ctrl Controller) *director {
	return &director{
		meta: meta,
		ctrl: ctrl,
	}
}

// NewDefaultDirector uses the LeveledController
func NewDefaultDirector(meta metadata.Tracker) *director {
	return &director{
		meta: meta,
		ctrl: controller.NewLeveledController(),
	}
}

func (d *director) MaybePlan() *Plan {
	segments := d.meta.GetAllSegments()
	// DEBUG: fmt.Printf("Director seeing %d segments\n", len(segments))

	if len(segments) < 2 {
		return nil
	}

	for _, seg := range segments {
		// DEBUG: fmt.Printf("Checking Segment %s: Range [%s - %s]\n", seg.ID, seg.MinKey, seg.MaxKey)
		overlaps := d.collectAllOverlaps(seg)
		// DEBUG: fmt.Printf("Found %d overlaps for %s\n", len(overlaps), seg.ID)

		if len(overlaps) > 1 {
			return &Plan{
				Inputs:         overlaps,
				OutputStrategy: common.LEVELED,
				Reason:         "Leveled: merging overlapping ranges",
			}
		}
	}
	return nil
}

// pickCompactionTarget selects a segment to compact (usually from L0 or a small level)
func pickCompactionTarget(segments []*common.SegmentMeta) *common.SegmentMeta {
	for _, seg := range segments {
		if !seg.Obsolete {
			return seg
		}
	}
	return nil
}

// collectAllOverlaps recursively finds all segments that overlap with the target
// This is critical for leveled compaction: you must compact overlapping files together
// collectAllOverlaps replacement using the new Metadata Ordered Map logic
func (d *director) collectAllOverlaps(target *common.SegmentMeta) []*common.SegmentMeta {
	inputs := []*common.SegmentMeta{target}
	seen := make(map[string]bool)
	seen[target.ID] = true

	changed := true
	for changed {
		changed = false
		// For each segment in our current merge set, find its overlaps
		for i := 0; i < len(inputs); i++ {
			input := inputs[i]
			// This call uses the internal ordered slice we built in metadata.go
			overlaps := d.meta.GetOverlappingSegments(input)
			for _, overlap := range overlaps {
				if !seen[overlap.ID] {
					inputs = append(inputs, overlap)
					seen[overlap.ID] = true
					changed = true
				}
			}
		}
	}

	return inputs
}
