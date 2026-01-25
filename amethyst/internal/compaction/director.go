package compaction

import (
	"amethyst/internal/common"
	"amethyst/internal/metadata"
	"time"
)

// Base LSM compaction thresholds
const (
	MinSegmentSize       = int64(4 * 1024) // 4KB
	MinRewriteInterval   = int64(1)        // 1 second cooldown
	ReadCountThreshold   = int64(10)       // Trigger compaction after 10 reads
	OverlapCountThreshold = 0               // Any overlap triggers compaction
)

type Plan struct {
	Inputs         []*common.SegmentMeta
	OutputStrategy common.CompactionType
	Reason         string
}

type Director interface {
	MaybePlan() *Plan
}

// Controller interface allows plugging in different compaction strategies
// (kept for benchmarking compatibility)
type Controller interface {
	ShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string)
}

// director produces compaction plans based on metadata.
type director struct {
	meta metadata.Tracker
	ctrl Controller
}

// NewDirector creates a director with a custom controller (for benchmarking)
func NewDirector(
	meta metadata.Tracker,
	ctrl Controller,
) *director {
	return &director{
		meta: meta,
		ctrl: ctrl,
	}
}

// NewDefaultDirector creates a director with simple base LSM leveled compaction
func NewDefaultDirector(meta metadata.Tracker) *director {
	return &director{
		meta: meta,
		ctrl: nil, // Use built-in logic
	}
}

// MaybePlan returns at most one compaction plan.
// Returns nil if no rewrite is needed.
func (d *director) MaybePlan() *Plan {
	segments := d.meta.GetAllSegments()

	// Newest → oldest scan
	for _, seg := range segments {
		if seg.Obsolete {
			continue
		}

		var should bool
		var newStrategy common.CompactionType
		var reason string

		// Use custom controller if provided (for benchmarking)
		if d.ctrl != nil {
			should, newStrategy, reason = d.ctrl.ShouldRewrite(seg)
		} else {
			// Built-in base LSM leveled compaction logic
			should, newStrategy, reason = baseLSMShouldRewrite(seg)
		}

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

// baseLSMShouldRewrite implements simple leveled compaction for base LSM
func baseLSMShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string) {
	now := time.Now().Unix()

	// Cooldown check to prevent thrashing
	if !meta.CooldownExpired(now, MinRewriteInterval) {
		return false, common.LEVELED, ""
	}

	// Too small to care
	if meta.Size() < MinSegmentSize {
		return false, common.LEVELED, ""
	}

	// Trigger compaction on overlap or high read count (fragmentation)
	if meta.OverlapCount > OverlapCountThreshold || meta.ReadCount > ReadCountThreshold {
		return true, common.LEVELED, "Base LSM: leveled merge"
	}

	return false, common.LEVELED, ""
}
