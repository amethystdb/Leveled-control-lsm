package engine

import (
	"amethyst/internal/common"
	"amethyst/internal/compaction"
	"amethyst/internal/memtable"
	"amethyst/internal/metadata"
	"amethyst/internal/segmentfile"
	"amethyst/internal/sparseindex"
	"amethyst/internal/sstable/reader"
	"amethyst/internal/sstable/writer"
	"os"
	"testing"
)

func TestLeveled_OverlapMerge(t *testing.T) {
	testFile := "leveled_test_unique.data"
	os.Remove(testFile)
	defer os.Remove(testFile)

	// Setup components
	fileMgr, _ := segmentfile.NewSegmentFileManager(testFile)
	meta := metadata.NewTracker()
	indexBuilder := sparseindex.NewBuilder(16)
	sstWriter := writer.NewWriter(fileMgr, indexBuilder)
	sstReader := reader.NewReader(fileMgr)
	executor := compaction.NewExecutor(meta, sstReader, sstWriter)
	director := compaction.NewDefaultDirector(meta)
	mem := memtable.NewMemtable(100)

	// Phase 1: Create a wide range in Segment A
	mem.Put("key-1", []byte("val-a"))
	mem.Put("key-9", []byte("val-z")) // Segment A now covers [key-1, key-9]
	segA, _ := sstWriter.WriteSegment(mem.Flush(), common.LEVELED)
	meta.RegisterSegment(segA)

	// Phase 2: Create a segment that falls INSIDE that range
	mem.Put("key-5", []byte("val-overlap")) // Key-5 is between 1 and 9
	segB, _ := sstWriter.WriteSegment(mem.Flush(), common.LEVELED)
	meta.RegisterSegment(segB)

	// Phase 3: Trigger Leveled Compaction
	// In Leveled, the director must find that segA and segB overlap
	plan := director.MaybePlan()
	if plan == nil {
		t.Fatalf("FAIL: Leveled director failed to produce a plan for overlapping segments")
	}

	// Verify the recursive overlap logic worked
	if len(plan.Inputs) < 2 {
		t.Errorf("FAIL: Leveled compaction should have picked 2 segments, got %d", len(plan.Inputs))
	}

	newSeg, err := executor.Execute(plan)
	if err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	// Phase 4: Verification
	// Use a fresh reader to bypass any memory caching issues
	freshReader := reader.NewReader(fileMgr)
	val1, found1 := freshReader.Get(newSeg, "key-1")
	val2, found2 := freshReader.Get(newSeg, "key-5") // Change key-2 to key-5

	if !found1 || string(val1) != "val-a" || !found2 || string(val2) != "val-overlap" { // Change value check
		t.Errorf("FAIL: Data missing after Leveled merge")
	} else {
		t.Log("SUCCESS: Leveled merge correctly combined overlapping segments.")
	}
}
