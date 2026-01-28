package benchmarks

import (
	"amethyst/internal/common"
	"time"
)

// Standardized with adaptive controller to ensure fair comparison
const MinSegmentSize = int64(4 * 1024)

type StaticLeveledController struct{}

func NewLeveledController() *StaticLeveledController {
	return &StaticLeveledController{}
}

func (c *StaticLeveledController) ShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string) {
	//current time
	now := time.Now().Unix()

	//cooldown check to prevent thrashing
	if !meta.CooldownExpired(now, 1) {
		return false, common.LEVELED, ""
	}

	// size gate: don't compact tiny files (standardized for benchmarks)
	if meta.Size() < MinSegmentSize {
		return false, common.LEVELED, ""
	}

	//fragment check (overlap or read count-> here set to 10 can change)
	if meta.OverlapCount > 0 || meta.ReadCount > 10 {
		//returns true, specifies leveled
		return true, common.LEVELED, "Baseline: Static Leveled merge"
	}
	//default
	return false, common.LEVELED, ""
}
