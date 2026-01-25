package engine

// Basic test for base LSM compaction

import (
	"amethyst/internal/common"
	"amethyst/internal/compaction"
	"amethyst/internal/metadata"
	"amethyst/internal/segmentfile"
	"amethyst/internal/sparseindex"
	"amethyst/internal/sstable/reader"
	"amethyst/internal/sstable/writer"
	"testing"
)

func TestBaseLSMCompaction(t *testing.T) {
	// Setup components
	tracker := metadata.NewTracker()
	sfm, _ := segmentfile.NewSegmentFileManager("test_data")
	sw := writer.NewWriter(sfm, sparseindex.NewBuilder(128))
	sr := reader.NewReader(sfm)

	dir := compaction.NewDefaultDirector(tracker)
	exe := compaction.NewExecutor(tracker, sr, sw)

	// Create dummy segment with high read count to trigger compaction
	dummyMeta := &common.SegmentMeta{
		ID:         "base-seg-1",
		Strategy:   common.TIERED,
		Length:     5000, // Above MinSegmentSize (4KB)
		ReadCount:  15,   // Above ReadCountThreshold (10)
		WriteCount: 0,
		MinKey:     "a",
		MaxKey:     "z",
	}
	tracker.RegisterSegment(dummyMeta)

	// Trigger compaction
	plan := dir.MaybePlan()
	if plan == nil {
		t.Fatal("Expected compaction plan, got nil")
	}

	if plan.OutputStrategy != common.LEVELED {
		t.Errorf("Expected LEVELED strategy, got %v", plan.OutputStrategy)
	}

	_, err := exe.Execute(plan)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	t.Log("Base LSM compaction completed successfully")
}
