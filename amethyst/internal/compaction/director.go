package compaction

import (
	"amethyst/internal/benchmarks" // Import the rules you wrote
	"amethyst/internal/common"
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

// NewDefaultDirector now officially uses your Leveled strategy
func NewDefaultDirector(meta metadata.Tracker) *director {
	return &director{
		meta: meta,
		ctrl: benchmarks.NewLeveledController(),
	}
}

func (d *director) MaybePlan() *Plan {
	segments := d.meta.GetAllSegments()

	for _, seg := range segments {
		if seg.Obsolete {
			continue
		}

		// Always use the controller logic from benchmarks/leveled.go
		should, newStrategy, reason := d.ctrl.ShouldRewrite(seg)

		if !should {
			continue
		}

		return &Plan{
			Inputs:         []*common.SegmentMeta{seg},
			OutputStrategy: newStrategy,
			Reason:         reason,
		}
	}
	return nil
}
