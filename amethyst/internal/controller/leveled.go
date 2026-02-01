package controller

import (
	"amethyst/internal/common"
	"time"
)

// ============================================================================
// LEVELED CONTROLLER - Pure Leveled Compaction Strategy
// ============================================================================
// Leveled compaction favors read performance by maintaining non-overlapping
// key ranges at each level. Triggers compaction when overlaps exist or when
// reads are frequent (indicating hot data that should be optimized).

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
	// This is CRITICAL for leveled compaction - overlaps break the level invariant
	if meta.OverlapCount > 0 {
		return true, common.LEVELED, "Leveled: Overlaps detected, merging to maintain level invariant"
	}

	// Read-heavy detection: if a segment is being read frequently, compact it
	// to optimize read path by removing old data
	if meta.ReadCount > 10 {
		return true, common.LEVELED, "Leveled: Hot segment (high read count), rewriting to consolidate"
	}

	return false, common.LEVELED, ""
}