package main

import (
	"amethyst/internal/common"
	"amethyst/internal/compaction"
	"amethyst/internal/memtable"
	"amethyst/internal/metadata"
	"amethyst/internal/segmentfile"
	"amethyst/internal/sparseindex"
	"amethyst/internal/sstable/reader"
	"amethyst/internal/sstable/writer"
	"amethyst/internal/wal"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"
)

var (
	workloadFlag  = flag.String("workload", "shift", "Workload type")
	numKeysFlag   = flag.Int("keys", 10000000, "Number of keys")
	valueSizeFlag = flag.Int("value-size", 256, "Value size in bytes")
	engineFlag    = flag.String("engine", "leveled", "Engine name for output")
)

// Results structure for JSON output
type Results struct {
	Engine             string        `json:"engine"`
	Workload           string        `json:"workload"`
	NumKeys            int           `json:"num_keys"`
	ValueSize          int           `json:"value_size"`
	WriteAmplification float64       `json:"write_amplification"`
	ReadAmplification  float64       `json:"read_amplification"`
	SpaceAmplification float64       `json:"space_amplification"`
	CompactionCount    int           `json:"compaction_count"`
	TotalDurationSec   float64       `json:"total_duration_sec"`
	LogicalBytes       int64         `json:"logical_bytes"`
	PhysicalBytes      int64         `json:"physical_bytes"`
	TotalReads         int64         `json:"total_reads"`
	SegmentScans       int64         `json:"segment_scans"`
	LiveDataBytes      int64         `json:"live_data_bytes"`
	TotalDiskBytes     int64         `json:"total_disk_bytes"`
	Phases             []PhaseResult `json:"phases,omitempty"`
}

type PhaseResult struct {
	Name     string  `json:"name"`
	Duration float64 `json:"duration_sec"`
	WA       float64 `json:"wa"`
	RA       float64 `json:"ra"`
}

// Zipfian distribution generator
func zipfian(n int, s float64) int {
	sum := 0.0
	for i := 1; i <= n; i++ {
		sum += 1.0 / math.Pow(float64(i), s)
	}

	r := rand.Float64() * sum
	partialSum := 0.0

	for i := 1; i <= n; i++ {
		partialSum += 1.0 / math.Pow(float64(i), s)
		if partialSum >= r {
			return i - 1
		}
	}
	return n - 1
}

func binarySearchSegment(segs []*common.SegmentMeta, key string, totalSegmentScans *int64) *common.SegmentMeta {
	left, right := 0, len(segs)-1
	for left <= right {
		mid := (left + right) / 2
		seg := segs[mid]
		*totalSegmentScans++

		if key >= seg.MinKey && key <= seg.MaxKey {
			return seg
		} else if key < seg.MinKey {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	return nil
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	// Validate inputs
	if *numKeysFlag <= 0 {
		fmt.Fprintf(os.Stderr, "Error: --keys must be > 0\n")
		os.Exit(1)
	}
	if *valueSizeFlag <= 0 {
		fmt.Fprintf(os.Stderr, "Error: --value-size must be > 0\n")
		os.Exit(1)
	}

	// Clean slate
	os.Remove("wal.log")
	os.Remove("sstable.data")

	fmt.Printf("╔════════════════════════════════════════╗\n")
	fmt.Printf("║  AMETHYST BENCHMARK                    ║\n")
	fmt.Printf("╚════════════════════════════════════════╝\n")
	fmt.Printf("Engine:   %s\n", *engineFlag)
	fmt.Printf("Workload: %s\n", *workloadFlag)
	fmt.Printf("Keys:     %d\n", *numKeysFlag)
	fmt.Printf("Value:    %d bytes\n", *valueSizeFlag)
	fmt.Println()

	// Initialize components
	w, err := wal.NewDiskWAL("wal.log")
	if err != nil {
		panic(err)
	}

	mem := memtable.NewMemtable(4 * 1024)
	meta := metadata.NewTracker()

	fileMgr, err := segmentfile.NewSegmentFileManager("sstable.data")
	if err != nil {
		panic(err)
	}

	indexBuilder := sparseindex.NewBuilder(16)
	sstWriter := writer.NewWriter(fileMgr, indexBuilder)
	sstReader := reader.NewReader(fileMgr)

	director := compaction.NewDefaultDirector(meta)
	executor := compaction.NewExecutor(meta, sstReader, sstWriter)

	// Metrics tracking
	var logicalBytes int64 = 0
	var physicalBytes int64 = 0
	var totalReads int64 = 0
	var totalSegmentScans int64 = 0
	compactionCount := 0
	var phases []PhaseResult

	startTime := time.Now()

	// Run workload
	switch *workloadFlag {
	case "shift":
		phases = runShift(w, mem, meta, sstWriter, sstReader, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes,
			&totalReads, &totalSegmentScans, &compactionCount)

	case "pure-write":
		runPureWrite(w, mem, meta, sstWriter, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes, &compactionCount)

	case "pure-read":
		runPureRead(w, mem, meta, sstWriter, sstReader,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes,
			&totalReads, &totalSegmentScans)

	case "mixed":
		runMixed(w, mem, meta, sstWriter, sstReader, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes,
			&totalReads, &totalSegmentScans, &compactionCount)

	case "read-heavy":
		runReadHeavy(w, mem, meta, sstWriter, sstReader, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes,
			&totalReads, &totalSegmentScans, &compactionCount)

	case "write-heavy":
		runWriteHeavy(w, mem, meta, sstWriter, sstReader, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes,
			&totalReads, &totalSegmentScans, &compactionCount)

	case "zipfian":
		runZipfian(w, mem, meta, sstWriter, sstReader, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes,
			&totalReads, &totalSegmentScans, &compactionCount)

	default:
		fmt.Printf("Unknown workload: %s\n", *workloadFlag)
		os.Exit(1)
	}

	totalDuration := time.Since(startTime)

	// Calculate final metrics
	wa := 0.0
	if logicalBytes > 0 {
		wa = float64(physicalBytes) / float64(logicalBytes)
	}
	ra := 0.0
	if totalReads > 0 {
		ra = float64(totalSegmentScans) / float64(totalReads)
	}

	// Space amplification (approximate)
	allSegs := meta.GetAllSegments()
	liveDataBytes := int64(*numKeysFlag * (*valueSizeFlag + 20)) // rough estimate
	totalDiskBytes := int64(0)
	for _, seg := range allSegs {
		totalDiskBytes += seg.Length
	}

	sa := 0.0
	if liveDataBytes > 0 {
		sa = float64(totalDiskBytes) / float64(liveDataBytes)
	}

	// Sanitize all metrics (handle NaN/Inf)
	if math.IsNaN(wa) || math.IsInf(wa, 0) {
		wa = 0.0
	}
	if math.IsNaN(ra) || math.IsInf(ra, 0) {
		ra = 0.0
	}
	if math.IsNaN(sa) || math.IsInf(sa, 0) {
		sa = 0.0
	}

	// Create results
	results := Results{
		Engine:             *engineFlag,
		Workload:           *workloadFlag,
		NumKeys:            *numKeysFlag,
		ValueSize:          *valueSizeFlag,
		WriteAmplification: wa,
		ReadAmplification:  ra,
		SpaceAmplification: sa,
		CompactionCount:    compactionCount,
		TotalDurationSec:   totalDuration.Seconds(),
		LogicalBytes:       logicalBytes,
		PhysicalBytes:      physicalBytes,
		TotalReads:         totalReads,
		SegmentScans:       totalSegmentScans,
		LiveDataBytes:      liveDataBytes,
		TotalDiskBytes:     totalDiskBytes,
		Phases:             phases,
	}

	// Print summary
	fmt.Printf("\n")
	fmt.Printf("╔════════════════════════════════════════╗\n")
	fmt.Printf("║  RESULTS                               ║\n")
	fmt.Printf("╚════════════════════════════════════════╝\n")
	fmt.Printf("Write Amplification:  %.2f\n", wa)
	fmt.Printf("Read Amplification:   %.2f\n", ra)
	fmt.Printf("Space Amplification:  %.2f\n", sa)
	fmt.Printf("Compaction Count:     %d\n", compactionCount)
	fmt.Printf("Total Duration:       %.2fs\n", totalDuration.Seconds())
	fmt.Printf("Throughput:           %.0f ops/sec\n",
		float64(*numKeysFlag)/totalDuration.Seconds())
	fmt.Println()

	// Save to JSON
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		panic(err)
	}

	filename := fmt.Sprintf("results_%s_%s.json", *engineFlag, *workloadFlag)
	err = os.WriteFile(filename, data, 0644)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Results saved to: %s\n", filename)
}

// ========================================
// WORKLOAD IMPLEMENTATIONS
// ========================================

func runShift(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes, totalReads, totalSegmentScans *int64,
	compactionCount *int) []PhaseResult {

	var phases []PhaseResult

	// PHASE 1: Write
	fmt.Println("=== PHASE 1: Write ===")
	phase1Start := time.Now()

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%010d", i)
		val := make([]byte, valueSize)
		rand.Read(val)

		w.LogPut(key, val)
		mem.Put(key, val)
		*logicalBytes += int64(len(key) + valueSize)

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
			*physicalBytes += seg.Length
			meta.RegisterSegment(seg)
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Written: %d/%d (%.1f%%)\r", i, numKeys, float64(i)*100/float64(numKeys))
		}
	}
	fmt.Println()

	// Final flush — always drain the memtable
	data := mem.Flush()
	if len(data) > 0 {
		seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
		*physicalBytes += seg.Length
		meta.RegisterSegment(seg)
	}

	phase1Duration := time.Since(phase1Start)
	phase1WA := float64(*physicalBytes) / float64(*logicalBytes)
	phases = append(phases, PhaseResult{
		Name:     "write",
		Duration: phase1Duration.Seconds(),
		WA:       phase1WA,
		RA:       0,
	})

	fmt.Printf("  Segments: %d\n", len(meta.GetAllSegments()))
	fmt.Printf("  Duration: %v\n", phase1Duration)

	// Establish leveled invariant before measuring reads
	fmt.Println("\n=== PRE-READ CLEANUP (Establishing Leveled Invariant) ===")
	compactionsDone := 0
	for plan := director.MaybePlan(); plan != nil; plan = director.MaybePlan() {
		fmt.Printf("  Compacting %d segments to ensure disjoint ranges...\n", len(plan.Inputs))
		newSeg, _ := executor.Execute(plan)
		*physicalBytes += newSeg.Length
		*compactionCount++
		compactionsDone++
	}
	fmt.Printf("  Pre-read compactions: %d, Segments remaining: %d\n",
		compactionsDone, len(meta.GetAllSegments()))

	// PHASE 2: Read
	fmt.Println("\n=== PHASE 2: Read (3x) ===")
	time.Sleep(2 * time.Second)

	phase2Start := time.Now()
	phase2StartSegmentScans := *totalSegmentScans  // Capture baseline for phase-local RA
	phase2StartReads := *totalReads                // Capture baseline for phase-local RA
	numReads := numKeys * 3

	for i := 0; i < numReads; i++ {
		key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
		segs := meta.GetAllSegments() // Returns sorted list

		*totalReads++
		seg := binarySearchSegment(segs, key, totalSegmentScans)
		if seg != nil {
			sstReader.Get(seg, key)
			meta.UpdateStats(seg.ID, 1, 0)
		}

		if i > 0 && i%500000 == 0 {
			fmt.Printf("  Reads: %d/%d (%.1f%%)\r", i, numReads, float64(i)*100/float64(numReads))
		}
	}
	fmt.Println()

	phase2Duration := time.Since(phase2Start)
	phase2RA := float64(*totalSegmentScans-phase2StartSegmentScans) / float64(*totalReads-phase2StartReads)  // Phase-local RA
	phase2WA := float64(*physicalBytes) / float64(*logicalBytes)
	phases = append(phases, PhaseResult{
		Name:     "read",
		Duration: phase2Duration.Seconds(),
		WA:       phase2WA,
		RA:       phase2RA,
	})

	fmt.Printf("  Current RA: %.2f\n", phase2RA)
	fmt.Printf("  Duration: %v\n", phase2Duration)

	// Phase 2 compaction (after reads)
	for {
		plan := director.MaybePlan()
		if plan == nil {
			break
		}
		fmt.Printf("  Compaction triggered: %s\n", plan.Reason)
		newSeg, _ := executor.Execute(plan)
		*physicalBytes += newSeg.Length
		*compactionCount++
		fmt.Printf("  New strategy: %v\n", newSeg.Strategy)
	}

	// PHASE 3: Write again
	fmt.Println("\n=== PHASE 3: Write (50%) ===")
	phase3Start := time.Now()

	for i := 0; i < numKeys/2; i++ {
		key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
		val := make([]byte, valueSize)
		rand.Read(val)

		w.LogPut(key, val)
		mem.Put(key, val)
		*logicalBytes += int64(len(key) + valueSize)

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
			*physicalBytes += seg.Length
			meta.RegisterSegment(seg)
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Written: %d/%d (%.1f%%)\r", i, numKeys/2, float64(i)*100/float64(numKeys/2))
		}
	}
	fmt.Println()

	// Final flush — always drain the memtable
	data = mem.Flush()
	if len(data) > 0 {
		seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
		*physicalBytes += seg.Length
		meta.RegisterSegment(seg)
	}

	phase3Duration := time.Since(phase3Start)
	phase3WA := float64(*physicalBytes) / float64(*logicalBytes)
	phases = append(phases, PhaseResult{
		Name:     "write2",
		Duration: phase3Duration.Seconds(),
		WA:       phase3WA,
		RA:       0,  // Write phase — no reads
	})

	fmt.Printf("  Duration: %v\n", phase3Duration)

	// Phase 3 compaction (after writes)
	for {
		plan := director.MaybePlan()
		if plan == nil {
			break
		}
		fmt.Printf("  Compaction triggered: %s\n", plan.Reason)
		newSeg, _ := executor.Execute(plan)
		*physicalBytes += newSeg.Length
		*compactionCount++
		fmt.Printf("  New strategy: %v\n", newSeg.Strategy)
	}

	return phases
}

func runPureWrite(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes *int64, compactionCount *int) {

	fmt.Println("=== PURE WRITE WORKLOAD ===")

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%010d", i)
		val := make([]byte, valueSize)
		rand.Read(val)

		w.LogPut(key, val)
		mem.Put(key, val)
		*logicalBytes += int64(len(key) + valueSize)

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
			*physicalBytes += seg.Length
			meta.RegisterSegment(seg)
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Progress: %d/%d (%.1f%%)\r", i, numKeys, float64(i)*100/float64(numKeys))
		}
	}
	fmt.Println()

	// Final flush — always drain the memtable
	data := mem.Flush()
	if len(data) > 0 {
		seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
		*physicalBytes += seg.Length
		meta.RegisterSegment(seg)
	}

	// Compaction drain loop
	for {
		plan := director.MaybePlan()
		if plan == nil {
			break
		}
		newSeg, _ := executor.Execute(plan)
		*physicalBytes += newSeg.Length
		*compactionCount++
	}
}

func runPureRead(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	numKeys, valueSize int, logicalBytes, physicalBytes, totalReads, totalSegmentScans *int64) {

	fmt.Println("=== PURE READ WORKLOAD ===")

	// Phase 1: Populate
	fmt.Println("Populating data...")
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%010d", i)
		val := make([]byte, valueSize)
		rand.Read(val)

		w.LogPut(key, val)
		mem.Put(key, val)

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
			meta.RegisterSegment(seg)
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Populated: %d/%d (%.1f%%)\r", i, numKeys, float64(i)*100/float64(numKeys))
		}
	}
	fmt.Println()

	// Final flush — always drain the memtable
	data := mem.Flush()
	if len(data) > 0 {
		seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
		meta.RegisterSegment(seg)
	}

	// Reset counters (don't count population)
	*logicalBytes = 0
	*physicalBytes = 0

	// Phase 2: Read
	fmt.Println("Reading (3x)...")
	numReads := numKeys * 3

	for i := 0; i < numReads; i++ {
		key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
		segs := meta.GetAllSegments()

		*totalReads++
		seg := binarySearchSegment(segs, key, totalSegmentScans)
		if seg != nil {
			sstReader.Get(seg, key)
		}

		if i > 0 && i%500000 == 0 {
			fmt.Printf("  Progress: %d/%d (%.1f%%)\r", i, numReads, float64(i)*100/float64(numReads))
		}
	}
	fmt.Println()
}

func runMixed(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes, totalReads, totalSegmentScans *int64,
	compactionCount *int) {

	fmt.Println("=== MIXED WORKLOAD (50/50) ===")

	// Populate
	fmt.Println("Populating...")
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%010d", i)
		val := make([]byte, valueSize)
		rand.Read(val)

		w.LogPut(key, val)
		mem.Put(key, val)
		*logicalBytes += int64(len(key) + valueSize)

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
			*physicalBytes += seg.Length
			meta.RegisterSegment(seg)
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numKeys)
		}
	}
	fmt.Println()

	// Final flush — always drain the memtable
	data := mem.Flush()
	if len(data) > 0 {
		seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
		*physicalBytes += seg.Length
		meta.RegisterSegment(seg)
	}

	// Mixed operations
	fmt.Println("Running mixed operations...")
	numOps := numKeys * 2

	for i := 0; i < numOps; i++ {
		if rand.Float32() < 0.5 {
			// Write
			key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
			val := make([]byte, valueSize)
			rand.Read(val)

			w.LogPut(key, val)
			mem.Put(key, val)
			*logicalBytes += int64(len(key) + valueSize)

			if mem.ShouldFlush() {
				data := mem.Flush()
				seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
				*physicalBytes += seg.Length
				meta.RegisterSegment(seg)
				w.Truncate()
			}
		} else {
			// Read
			key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
			segs := meta.GetAllSegments()

			*totalReads++
			seg := binarySearchSegment(segs, key, totalSegmentScans)
			if seg != nil {
				sstReader.Get(seg, key)
				meta.UpdateStats(seg.ID, 1, 0)
			}
		}

		if i > 0 && i%200000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numOps)
		}
	}
	fmt.Println()

	// Compaction drain loop
	for {
		plan := director.MaybePlan()
		if plan == nil {
			break
		}
		newSeg, _ := executor.Execute(plan)
		*physicalBytes += newSeg.Length
		*compactionCount++
	}
}

func runReadHeavy(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes, totalReads, totalSegmentScans *int64,
	compactionCount *int) {

	fmt.Println("=== READ-HEAVY WORKLOAD (95% reads) ===")

	// Populate
	fmt.Println("Populating...")
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%010d", i)
		val := make([]byte, valueSize)
		rand.Read(val)

		w.LogPut(key, val)
		mem.Put(key, val)
		*logicalBytes += int64(len(key) + valueSize)

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
			*physicalBytes += seg.Length
			meta.RegisterSegment(seg)
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numKeys)
		}
	}
	fmt.Println()

	// Final flush — always drain the memtable
	data := mem.Flush()
	if len(data) > 0 {
		seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
		*physicalBytes += seg.Length
		meta.RegisterSegment(seg)
	}

	// Operations (95% read)
	fmt.Println("Running read-heavy operations...")
	numOps := numKeys * 2

	for i := 0; i < numOps; i++ {
		if rand.Float32() < 0.95 {
			// Read
			key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
			segs := meta.GetAllSegments()

			*totalReads++
			seg := binarySearchSegment(segs, key, totalSegmentScans)
			if seg != nil {
				sstReader.Get(seg, key)
				meta.UpdateStats(seg.ID, 1, 0)
			}
		} else {
			// Write
			key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
			val := make([]byte, valueSize)
			rand.Read(val)

			w.LogPut(key, val)
			mem.Put(key, val)
			*logicalBytes += int64(len(key) + valueSize)

			if mem.ShouldFlush() {
				data := mem.Flush()
				seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
				*physicalBytes += seg.Length
				meta.RegisterSegment(seg)
				w.Truncate()
			}
		}

		if i > 0 && i%200000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numOps)
		}
	}
	fmt.Println()

	// Compaction drain loop
	for {
		plan := director.MaybePlan()
		if plan == nil {
			break
		}
		newSeg, _ := executor.Execute(plan)
		*physicalBytes += newSeg.Length
		*compactionCount++
	}
}

func runWriteHeavy(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes, totalReads, totalSegmentScans *int64,
	compactionCount *int) {

	fmt.Println("=== WRITE-HEAVY WORKLOAD (95% writes) ===")

	// Populate
	fmt.Println("Populating...")
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%010d", i)
		val := make([]byte, valueSize)
		rand.Read(val)

		w.LogPut(key, val)
		mem.Put(key, val)
		*logicalBytes += int64(len(key) + valueSize)

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
			*physicalBytes += seg.Length
			meta.RegisterSegment(seg)
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numKeys)
		}
	}
	fmt.Println()

	// Final flush — always drain the memtable
	data := mem.Flush()
	if len(data) > 0 {
		seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
		*physicalBytes += seg.Length
		meta.RegisterSegment(seg)
	}

	// Operations (95% write)
	fmt.Println("Running write-heavy operations...")
	numOps := numKeys * 2

	for i := 0; i < numOps; i++ {
		if rand.Float32() < 0.95 {
			// Write
			key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
			val := make([]byte, valueSize)
			rand.Read(val)

			w.LogPut(key, val)
			mem.Put(key, val)
			*logicalBytes += int64(len(key) + valueSize)

			if mem.ShouldFlush() {
				data := mem.Flush()
				seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
				*physicalBytes += seg.Length
				meta.RegisterSegment(seg)
				w.Truncate()
			}
		} else {
			// Read
			key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
			segs := meta.GetAllSegments()

			*totalReads++
			seg := binarySearchSegment(segs, key, totalSegmentScans)
			if seg != nil {
				sstReader.Get(seg, key)
			}
		}

		if i > 0 && i%200000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numOps)
		}
	}
	fmt.Println()

	// Compaction drain loop
	for {
		plan := director.MaybePlan()
		if plan == nil {
			break
		}
		newSeg, _ := executor.Execute(plan)
		*physicalBytes += newSeg.Length
		*compactionCount++
	}
}

func runZipfian(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes, totalReads, totalSegmentScans *int64,
	compactionCount *int) {

	fmt.Println("=== ZIPFIAN WORKLOAD (hot keys, s=1.5) ===")

	// Phase 1: Write (sequential)
	fmt.Println("Populating data...")
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%010d", i)
		val := make([]byte, valueSize)
		rand.Read(val)

		w.LogPut(key, val)
		mem.Put(key, val)
		*logicalBytes += int64(len(key) + valueSize)

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
			*physicalBytes += seg.Length
			meta.RegisterSegment(seg)
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numKeys)
		}
	}
	fmt.Println()

	// Final flush — always drain the memtable
	data := mem.Flush()
	if len(data) > 0 {
		seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
		*physicalBytes += seg.Length
		meta.RegisterSegment(seg)
	}

	// Phase 2: Zipfian reads
	fmt.Println("Reading with Zipfian distribution (s=1.5)...")
	numReads := numKeys * 3
	hotKeyAccesses := make(map[int]int)

	for i := 0; i < numReads; i++ {
		keyIdx := zipfian(numKeys, 1.5)
		hotKeyAccesses[keyIdx]++

		key := fmt.Sprintf("key-%010d", keyIdx)
		segs := meta.GetAllSegments()

		*totalReads++
		seg := binarySearchSegment(segs, key, totalSegmentScans)
		if seg != nil {
			sstReader.Get(seg, key)
			meta.UpdateStats(seg.ID, 1, 0)
		}

		if i > 0 && i%500000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numReads)
		}
	}
	fmt.Println()

	// Report hot key stats
	top10Percent := 0
	for i := 0; i < numKeys/10; i++ {
		top10Percent += hotKeyAccesses[i]
	}
	fmt.Printf("  Hot key distribution: Top 10%% of keys = %d%% of accesses\n",
		top10Percent*100/numReads)

	// Compaction drain loop
	for {
		plan := director.MaybePlan()
		if plan == nil {
			break
		}
		fmt.Printf("  Compaction triggered: %s\n", plan.Reason)
		newSeg, _ := executor.Execute(plan)
		*physicalBytes += newSeg.Length
		*compactionCount++
	}
}
