package main

import (
	"amethyst/internal/benchmarks"
	"amethyst/internal/common"
	"amethyst/internal/compaction"
	"amethyst/internal/memtable"
	"amethyst/internal/metadata"
	"amethyst/internal/segmentfile"
	"amethyst/internal/sparseindex"
	"amethyst/internal/sstable/reader"
	"amethyst/internal/sstable/writer"
	"amethyst/internal/wal"
	"fmt"
	"log"
	"os"
	"time"
)

func runStrategy(strategyName string, ctrl compaction.Controller) {
	log.Printf("\n╔════════════════════════════════════════╗")
	log.Printf("║  Testing: %-28s ║", strategyName)
	log.Printf("╚════════════════════════════════════════╝\n")

	// Clean environment
	os.Remove("wal.log")
	os.Remove("sstable.data")

	// Initialize components
	w, _ := wal.NewDiskWAL("wal.log")
	mem := memtable.NewMemtable(4 * 1024)
	meta := metadata.NewTracker()
	fileMgr, _ := segmentfile.NewSegmentFileManager("sstable.data")
	indexBuilder := sparseindex.NewBuilder(16)
	sstWriter := writer.NewWriter(fileMgr, indexBuilder)
	sstReader := reader.NewReader(fileMgr)

	var director compaction.Director
	if ctrl != nil {
		director = compaction.NewDirector(meta, ctrl)
	} else {
		director = compaction.NewDefaultDirector(meta)
	}
	executor := compaction.NewExecutor(meta, sstReader, sstWriter)

	// Write initial data
	log.Println("Writing 500 keys...")
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%06d", i)
		val := []byte(fmt.Sprintf("value-%06d", i))
		w.LogPut(key, val)
		mem.Put(key, val)
	}

	// Flush to disk
	log.Println("Flushing to disk...")
	data := mem.Flush()
	seg1, _ := sstWriter.WriteSegment(data, common.TIERED)
	meta.RegisterSegment(seg1)
	w.Truncate()
	log.Printf("  Segment created: %s (Strategy: %v)", seg1.ID[:8], seg1.Strategy)

	// Simulate workload based on strategy
	currentSeg := meta.GetAllSegments()[0]

	if strategyName == "Static Tiered" {
		// Tiered triggers on high write count
		log.Println("\nSimulating write-heavy workload...")
		for i := 0; i < 60; i++ {
			meta.UpdateStats(currentSeg.ID, 0, 1)
		}
		currentSeg = meta.GetAllSegments()[0]
		log.Printf("  Write Count: %d", currentSeg.WriteCount)
	} else {
		// Leveled triggers on high read count or overlap
		log.Println("\nSimulating read-heavy workload...")
		for i := 0; i < 15; i++ {
			key := fmt.Sprintf("key-%06d", i%500)
			sstReader.Get(currentSeg, key)
			meta.UpdateStats(currentSeg.ID, 1, 0)
		}
		currentSeg = meta.GetAllSegments()[0]
		log.Printf("  Read Count: %d", currentSeg.ReadCount)
	}

	// Wait for cooldown
	time.Sleep(2 * time.Second)

	// Trigger compaction
	log.Println("\nChecking for compaction...")
	plan := director.MaybePlan()
	if plan == nil {
		log.Println("  ✗ No compaction triggered")
	} else {
		log.Printf("  ✓ Compaction triggered: %s", plan.Reason)
		log.Printf("  Strategy: %v → %v", plan.Inputs[0].Strategy, plan.OutputStrategy)

		newSeg, err := executor.Execute(plan)
		if err != nil {
			log.Printf("  ✗ Error: %v", err)
		} else {
			log.Printf("  ✓ New segment: %s (Strategy: %v)", newSeg.ID[:8], newSeg.Strategy)
		}
	}

	// Verify data integrity
	log.Println("\nVerifying data integrity...")
	finalSegs := meta.GetAllSegments()
	if len(finalSegs) > 0 {
		seg := finalSegs[0]
		failCount := 0
		for i := 0; i < 500; i++ {
			key := fmt.Sprintf("key-%06d", i)
			expectedVal := fmt.Sprintf("value-%06d", i)
			val, ok := sstReader.Get(seg, key)
			if !ok || string(val) != expectedVal {
				failCount++
			}
		}
		if failCount == 0 {
			log.Printf("  ✓ All 500 keys verified")
		} else {
			log.Printf("  ✗ %d keys failed verification", failCount)
		}
	}
}

func main() {
	log.Println("Testing Different Compaction Strategies")

	// Test Static Tiered
	runStrategy("Static Tiered", benchmarks.NewTieredController())

	log.Println("\n═════════════════════════════════════════════")

	// Test Static Leveled (same as base LSM)
	runStrategy("Static Leveled", benchmarks.NewLeveledController())

	log.Println("\n✓ All strategy tests complete!")
}
