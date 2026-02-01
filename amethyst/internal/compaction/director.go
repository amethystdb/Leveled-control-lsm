package compaction

import (
	"amethyst/internal/common"
	"amethyst/internal/metadata"
	"amethyst/internal/controller"
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

// NewLeveledDirector explicitly uses leveled compaction strategy
func NewLeveledDirector(meta metadata.Tracker) *director {
	return &director{
		meta: meta,
		ctrl: controller.NewLeveledController(),
	}
}

func (d *director) MaybePlan() *Plan {
	segments := d.meta.GetAllSegments()

	for _, seg := range segments {
		if seg.Obsolete {
			continue
		}

		should, newStrategy, reason := d.ctrl.ShouldRewrite(seg)

		if !should {
			continue
		}

		// LEVELED: must collect ALL overlapping segments to maintain level invariant
		inputs := d.collectAllOverlaps(seg)

		return &Plan{
			Inputs:         inputs,
			OutputStrategy: newStrategy,
			Reason:         reason,
		}
	}
	return nil
}

// collectAllOverlaps recursively finds all segments that overlap with the target
// This is critical for leveled compaction: you must compact overlapping files together
func (d *director) collectAllOverlaps(target *common.SegmentMeta) []*common.SegmentMeta {
	inputs := []*common.SegmentMeta{target}
	seen := make(map[string]bool)
	seen[target.ID] = true

	// Iteratively expand the set of inputs until no new overlaps are found
	changed := true
	for changed {
		changed = false
		// For each segment currently in inputs, find all segments that overlap with it
		for _, input := range inputs {
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