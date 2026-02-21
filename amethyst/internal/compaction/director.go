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
	TargetLevel    int
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
	// Check Level 0 first (Trigger: File count >= 4)
	l0Segments := d.getSegmentsAtLevel(0)
	if len(l0Segments) >= 4 {
		return d.buildL0toL1Plan(l0Segments)
	}

	// Check Level 1 (Trigger: Total size > 10MB)
	l1Segments := d.getSegmentsAtLevel(1)
	if d.totalSize(l1Segments) > 10*1024*1024 { // 10 MB limit
		return d.buildLevelPlan(1, 2, l1Segments)
	}

	return nil
}

// --- NEW HELPER METHODS ---

func (d *director) getSegmentsAtLevel(level int) []*common.SegmentMeta {
	var result []*common.SegmentMeta
	for _, seg := range d.meta.GetAllSegments() {
		if seg.Level == level && !seg.Obsolete {
			result = append(result, seg)
		}
	}
	return result
}

func (d *director) totalSize(segments []*common.SegmentMeta) int64 {
	var size int64 = 0
	for _, seg := range segments {
		size += seg.Length
	}
	return size
}

// L0 to L1 merges ALL of L0 into overlapping L1 files
func (d *director) buildL0toL1Plan(l0Segs []*common.SegmentMeta) *Plan {
	inputs := append([]*common.SegmentMeta{}, l0Segs...)

	// Find all L1 segments that overlap with ANY L0 segment
	l1Segs := d.getSegmentsAtLevel(1)
	seen := make(map[string]bool)

	for _, l0 := range l0Segs {
		for _, l1 := range l1Segs {
			if !seen[l1.ID] && !(l1.MaxKey < l0.MinKey || l1.MinKey > l0.MaxKey) {
				inputs = append(inputs, l1)
				seen[l1.ID] = true
			}
		}
	}

	return &Plan{
		Inputs:         inputs,
		OutputStrategy: common.LEVELED,
		Reason:         "Leveled: L0 hit capacity (4 files), merging to L1",
		TargetLevel:    1,
	}
}

// L1+ merges pick ONE file and merge it into overlapping files in the next level
func (d *director) buildLevelPlan(currentLevel int, targetLevel int, currentSegs []*common.SegmentMeta) *Plan {
	target := currentSegs[0] // Pick the oldest
	inputs := []*common.SegmentMeta{target}

	nextLevelSegs := d.getSegmentsAtLevel(targetLevel)
	for _, next := range nextLevelSegs {
		if !(next.MaxKey < target.MinKey || next.MinKey > target.MaxKey) {
			inputs = append(inputs, next)
		}
	}

	return &Plan{
		Inputs:         inputs,
		OutputStrategy: common.LEVELED,
		Reason:         "Leveled: Capacity exceeded, cascading down",
		TargetLevel:    targetLevel,
	}
}
