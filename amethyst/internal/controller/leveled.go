package controller

import (
	"amethyst/internal/common"
	"time"
)

// ============================================================================
// LEVELED CONTROLLER - Pure Leveled Compaction Strategy
// ============================================================================
// Leveled compaction favors read performance by maintaining non-overlapping
// key ranges at each level. Triggers compaction ONLY when overlaps exist.
// Reads are fast because segments don't overlap.

type LeveledController struct{}

func NewLeveledController() *LeveledController {
	return &LeveledController{}
}

func (c *LeveledController) ShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string) {
	now := time.Now().Unix()

	// Cooldown: prevent thrashing by not recompacting too frequently
	if !meta.CooldownExpired(now, 1) {
		return false, common.LEVELED, ""
	}

	// Overlap detection: if this segment overlaps with others, compact them together
	// This is the ONLY trigger for leveled compaction
	// Overlaps break the level invariant and slow down reads
	if meta.OverlapCount > 0 {
		return true, common.LEVELED, "Leveled: Overlaps detected, merging to maintain level invariant"
	}

	// NOTE: Removed ReadCount > 10 trigger
	// This was causing compactions on EVERY READ and crushing read performance
	// Reads will naturally trigger compactions through overlap resolution

	return false, common.LEVELED, ""
}