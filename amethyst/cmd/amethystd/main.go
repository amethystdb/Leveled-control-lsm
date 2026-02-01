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
	Engine             string  `json:"engine"`
	Workload           string  `json:"workload"`
	NumKeys            int     `json:"num_keys"`
	ValueSize          int     `json:"value_size"`
	WriteAmplification float64 `json:"write_amplification"`
	ReadAmplification  float64 `json:"read_amplification"`
	SpaceAmplification float64 `json:"space_amplification"`
	CompactionCount    int     `json:"compaction_count"`
	TotalDurationSec   float64 `json:"total_duration_sec"`
	TotalOps           int64   `json:"total_ops"`
	Throughput         float64 `json:"throughput_ops_per_sec"`

	// Debug info
	LogicalBytes   int64 `json:"logical_bytes"`
	PhysicalBytes  int64 `json:"physical_bytes"`
	TotalReads     int64 `json:"total_reads"`
	SegmentScans   int64 `json:"segment_scans"`
	LiveDataBytes  int64 `json:"live_data_bytes"`
	TotalDiskBytes int64 `json:"total_disk_bytes"`

	// Phase breakdown (for shift workload)
	Phases []PhaseResult `json:"phases,omitempty"`
}

type PhaseResult struct {
	Name     string  `json:"name"`
	Duration float64 `json:"duration_sec"`
	WA       float64 `json:"wa"`
	RA       float64 `json:"ra"`
}

// ZipfianGenerator pre-computes the cumulative distribution for O(log n) sampling
type ZipfianGenerator struct {
	cdf []float64
	n   int
}

// NewZipfianGenerator creates a generator with pre-computed CDF
func NewZipfianGenerator(n int, s float64) *ZipfianGenerator {
	cdf := make([]float64, n)
	sum := 0.0
	for i := 1; i <= n; i++ {
		sum += 1.0 / math.Pow(float64(i), s)
		cdf[i-1] = sum
	}
	// Normalize to [0, 1]
	for i := range cdf {
		cdf[i] /= sum
	}
	return &ZipfianGenerator{cdf: cdf, n: n}
}

// Next returns the next Zipfian-distributed value using binary search O(log n)
func (z *ZipfianGenerator) Next() int {
	r := rand.Float64()
	// Binary search for the first index where cdf[i] >= r
	lo, hi := 0, z.n-1
	for lo < hi {
		mid := (lo + hi) / 2
		if z.cdf[mid] < r {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

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
	var totalOps int64 = 0
	compactionCount := 0
	var phases []PhaseResult

	startTime := time.Now()

	// Run workload
	switch *workloadFlag {
	case "shift":
		phases = runShift(w, mem, meta, sstWriter, sstReader, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes,
			&totalReads, &totalSegmentScans, &compactionCount, &totalOps)

	case "pure-write":
		runPureWrite(w, mem, meta, sstWriter, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes, &compactionCount, &totalOps)

	case "pure-read":
		runPureRead(w, mem, meta, sstWriter, sstReader, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes,
			&totalReads, &totalSegmentScans, &compactionCount, &totalOps)

	case "mixed":
		runMixed(w, mem, meta, sstWriter, sstReader, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes,
			&totalReads, &totalSegmentScans, &compactionCount, &totalOps)

	case "read-heavy":
		runReadHeavy(w, mem, meta, sstWriter, sstReader, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes,
			&totalReads, &totalSegmentScans, &compactionCount, &totalOps)

	case "write-heavy":
		runWriteHeavy(w, mem, meta, sstWriter, sstReader, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes,
			&totalReads, &totalSegmentScans, &compactionCount, &totalOps)

	case "zipfian":
		runZipfian(w, mem, meta, sstWriter, sstReader, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes,
			&totalReads, &totalSegmentScans, &compactionCount, &totalOps)

	default:
		fmt.Printf("Unknown workload: %s\n", *workloadFlag)
		os.Exit(1)
	}

	totalDuration := time.Since(startTime)

	// Calculate final metrics
	wa := float64(physicalBytes) / float64(logicalBytes)
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
	sa := float64(totalDiskBytes) / float64(liveDataBytes)

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
		TotalOps:           totalOps,
		Throughput:         float64(totalOps) / totalDuration.Seconds(),
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
		float64(totalOps)/totalDuration.Seconds())
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
	compactionCount *int, totalOps *int64) []PhaseResult {

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
		*totalOps++

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
			*physicalBytes += seg.Length
			meta.RegisterSegment(seg)
			// ALWAYS compact in leveled
			if plan := director.MaybePlan(); plan != nil {
				newSeg, _ := executor.Execute(plan)
				*physicalBytes += newSeg.Length
				*compactionCount++
			}
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Written: %d/%d (%.1f%%)\r", i, numKeys, float64(i)*100/float64(numKeys))
		}
	}
	fmt.Println()

	// Final flush
	if mem.ShouldFlush() {
		data := mem.Flush()
		seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
		*physicalBytes += seg.Length
		meta.RegisterSegment(seg)
		// ALWAYS compact in leveled
		if plan := director.MaybePlan(); plan != nil {
			newSeg, _ := executor.Execute(plan)
			*physicalBytes += newSeg.Length
			*compactionCount++
		}
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

	// PHASE 2: Read
	fmt.Println("\n=== PHASE 2: Read (3x) ===")
	time.Sleep(2 * time.Second)

	phase2Start := time.Now()
	numReads := numKeys * 3

	for i := 0; i < numReads; i++ {
		key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
		segs := meta.GetAllSegments()

		*totalReads++
		*totalOps++
		found := false
		for _, seg := range segs {
			*totalSegmentScans++
			if !found && key >= seg.MinKey && key <= seg.MaxKey {
				sstReader.Get(seg, key)
				meta.UpdateStats(seg.ID, 1, 0)
				found = true
			}
		}

		if i > 0 && i%500000 == 0 {
			fmt.Printf("  Reads: %d/%d (%.1f%%)\r", i, numReads, float64(i)*100/float64(numReads))
		}
	}
	fmt.Println()

	phase2Duration := time.Since(phase2Start)
	phase2RA := float64(*totalSegmentScans) / float64(*totalReads)
	phases = append(phases, PhaseResult{
		Name:     "read",
		Duration: phase2Duration.Seconds(),
		WA:       phase1WA,
		RA:       phase2RA,
	})

	fmt.Printf("  Current RA: %.2f\n", phase2RA)
	fmt.Printf("  Duration: %v\n", phase2Duration)

	// Try compaction
	time.Sleep(2 * time.Second)
	if plan := director.MaybePlan(); plan != nil {
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
		*totalOps++

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
			*physicalBytes += seg.Length
			meta.RegisterSegment(seg)
			// ALWAYS compact in leveled
			if plan := director.MaybePlan(); plan != nil {
				newSeg, _ := executor.Execute(plan)
				*physicalBytes += newSeg.Length
				*compactionCount++
			}
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Written: %d/%d (%.1f%%)\r", i, numKeys/2, float64(i)*100/float64(numKeys/2))
		}
	}
	fmt.Println()

	// Final flush
	if mem.ShouldFlush() {
		data := mem.Flush()
		seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
		*physicalBytes += seg.Length
		meta.RegisterSegment(seg)
		// ALWAYS compact in leveled
		if plan := director.MaybePlan(); plan != nil {
			newSeg, _ := executor.Execute(plan)
			*physicalBytes += newSeg.Length
			*compactionCount++
		}
	}

	phase3Duration := time.Since(phase3Start)
	phase3WA := float64(*physicalBytes) / float64(*logicalBytes)
	phases = append(phases, PhaseResult{
		Name:     "write2",
		Duration: phase3Duration.Seconds(),
		WA:       phase3WA,
		RA:       phase2RA,
	})

	fmt.Printf("  Duration: %v\n", phase3Duration)

	// Try compaction again
	time.Sleep(2 * time.Second)
	if plan := director.MaybePlan(); plan != nil {
		fmt.Printf("  Compaction triggered: %s\n", plan.Reason)
		newSeg, _ := executor.Execute(plan)
		*physicalBytes += newSeg.Length
		*compactionCount++
		fmt.Printf("  New strategy: %v\n", newSeg.Strategy)
	}

	return phases
}

func runPureWrite(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes *int64, compactionCount *int, totalOps *int64) {

	fmt.Println("=== PURE WRITE WORKLOAD ===")

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%010d", i)
		val := make([]byte, valueSize)
		rand.Read(val)

		w.LogPut(key, val)
		mem.Put(key, val)
		*logicalBytes += int64(len(key) + valueSize)
		*totalOps++

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
			*physicalBytes += seg.Length
			meta.RegisterSegment(seg)
			// ALWAYS compact in leveled
			if plan := director.MaybePlan(); plan != nil {
				newSeg, _ := executor.Execute(plan)
				*physicalBytes += newSeg.Length
				*compactionCount++
			}
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Progress: %d/%d (%.1f%%)\r", i, numKeys, float64(i)*100/float64(numKeys))
		}
	}
	fmt.Println()

	// Final flush
	if mem.ShouldFlush() {
		data := mem.Flush()
		seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
		*physicalBytes += seg.Length
		meta.RegisterSegment(seg)
		// ALWAYS compact in leveled
		if plan := director.MaybePlan(); plan != nil {
			newSeg, _ := executor.Execute(plan)
			*physicalBytes += newSeg.Length
			*compactionCount++
		}
	}
}

func runPureRead(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes, totalReads, totalSegmentScans *int64,
	compactionCount *int, totalOps *int64) {

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
			// ALWAYS compact in leveled
			if plan := director.MaybePlan(); plan != nil {
				executor.Execute(plan)
				*compactionCount++
			}
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Populated: %d/%d (%.1f%%)\r", i, numKeys, float64(i)*100/float64(numKeys))
		}
	}
	fmt.Println()

	// Final flush
	if mem.ShouldFlush() {
		data := mem.Flush()
		seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
		meta.RegisterSegment(seg)
		// ALWAYS compact in leveled
		if plan := director.MaybePlan(); plan != nil {
			executor.Execute(plan)
			*compactionCount++
		}
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
		*totalOps++
		found := false
		for _, seg := range segs {
			*totalSegmentScans++
			if !found && key >= seg.MinKey && key <= seg.MaxKey {
				sstReader.Get(seg, key)
				meta.UpdateStats(seg.ID, 1, 0)
				found = true
			}
		}

		if i > 0 && i%500000 == 0 {
			fmt.Printf("  Progress: %d/%d (%.1f%%)\r", i, numReads, float64(i)*100/float64(numReads))
		}
	}
	fmt.Println()

	// Try compaction after reads
	time.Sleep(2 * time.Second)
	if plan := director.MaybePlan(); plan != nil {
		newSeg, _ := executor.Execute(plan)
		*physicalBytes += newSeg.Length
		*compactionCount++
	}
}

func runMixed(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes, totalReads, totalSegmentScans *int64,
	compactionCount *int, totalOps *int64) {

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
		*totalOps++

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
			*physicalBytes += seg.Length
			meta.RegisterSegment(seg)
			// ALWAYS compact in leveled
			if plan := director.MaybePlan(); plan != nil {
				newSeg, _ := executor.Execute(plan)
				*physicalBytes += newSeg.Length
				*compactionCount++
			}
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numKeys)
		}
	}
	fmt.Println()

	// Final flush
	if mem.ShouldFlush() {
		data := mem.Flush()
		seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
		*physicalBytes += seg.Length
		meta.RegisterSegment(seg)
		// ALWAYS compact in leveled
		if plan := director.MaybePlan(); plan != nil {
			newSeg, _ := executor.Execute(plan)
			*physicalBytes += newSeg.Length
			*compactionCount++
		}
	}

	// Mixed operations
	fmt.Println("Running mixed operations...")
	numOps := numKeys * 2

	for i := 0; i < numOps; i++ {
		*totalOps++
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
				// ALWAYS compact in leveled
				if plan := director.MaybePlan(); plan != nil {
					newSeg, _ := executor.Execute(plan)
					*physicalBytes += newSeg.Length
					*compactionCount++
				}
				w.Truncate()
			}
		} else {
			// Read
			key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
			segs := meta.GetAllSegments()

			*totalReads++
			found := false
			for _, seg := range segs {
				*totalSegmentScans++
				if !found && key >= seg.MinKey && key <= seg.MaxKey {
					sstReader.Get(seg, key)
					meta.UpdateStats(seg.ID, 1, 0)
					found = true
				}
			}
		}

		if i > 0 && i%200000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numOps)
		}
	}
	fmt.Println()

	// Compaction
	time.Sleep(2 * time.Second)
	if plan := director.MaybePlan(); plan != nil {
		newSeg, _ := executor.Execute(plan)
		*physicalBytes += newSeg.Length
		*compactionCount++
	}
}

func runReadHeavy(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes, totalReads, totalSegmentScans *int64,
	compactionCount *int, totalOps *int64) {

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
		*totalOps++

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
			*physicalBytes += seg.Length
			meta.RegisterSegment(seg)
			// ALWAYS compact in leveled
			if plan := director.MaybePlan(); plan != nil {
				newSeg, _ := executor.Execute(plan)
				*physicalBytes += newSeg.Length
				*compactionCount++
			}
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numKeys)
		}
	}
	fmt.Println()

	// Final flush
	if mem.ShouldFlush() {
		data := mem.Flush()
		seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
		*physicalBytes += seg.Length
		meta.RegisterSegment(seg)
		// ALWAYS compact in leveled
		if plan := director.MaybePlan(); plan != nil {
			newSeg, _ := executor.Execute(plan)
			*physicalBytes += newSeg.Length
			*compactionCount++
		}
	}

	// Operations (95% read)
	fmt.Println("Running read-heavy operations...")
	numOps := numKeys * 2

	for i := 0; i < numOps; i++ {
		*totalOps++
		if rand.Float32() < 0.95 {
			// Read
			key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
			segs := meta.GetAllSegments()

			*totalReads++
			found := false
			for _, seg := range segs {
				*totalSegmentScans++
				if !found && key >= seg.MinKey && key <= seg.MaxKey {
					sstReader.Get(seg, key)
					meta.UpdateStats(seg.ID, 1, 0)
					found = true
				}
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
				// ALWAYS compact in leveled
				if plan := director.MaybePlan(); plan != nil {
					newSeg, _ := executor.Execute(plan)
					*physicalBytes += newSeg.Length
					*compactionCount++
				}
				w.Truncate()
			}
		}

		if i > 0 && i%200000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numOps)
		}
	}
	fmt.Println()

	// Compaction
	time.Sleep(2 * time.Second)
	if plan := director.MaybePlan(); plan != nil {
		newSeg, _ := executor.Execute(plan)
		*physicalBytes += newSeg.Length
		*compactionCount++
	}
}

func runWriteHeavy(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes, totalReads, totalSegmentScans *int64,
	compactionCount *int, totalOps *int64) {

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
		*totalOps++

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
			*physicalBytes += seg.Length
			meta.RegisterSegment(seg)
			// ALWAYS compact in leveled
			if plan := director.MaybePlan(); plan != nil {
				newSeg, _ := executor.Execute(plan)
				*physicalBytes += newSeg.Length
				*compactionCount++
			}
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numKeys)
		}
	}
	fmt.Println()

	// Final flush
	if mem.ShouldFlush() {
		data := mem.Flush()
		seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
		*physicalBytes += seg.Length
		meta.RegisterSegment(seg)
		// ALWAYS compact in leveled
		if plan := director.MaybePlan(); plan != nil {
			newSeg, _ := executor.Execute(plan)
			*physicalBytes += newSeg.Length
			*compactionCount++
		}
	}

	// Operations (95% write)
	fmt.Println("Running write-heavy operations...")
	numOps := numKeys * 2

	for i := 0; i < numOps; i++ {
		*totalOps++
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
				// ALWAYS compact in leveled
				if plan := director.MaybePlan(); plan != nil {
					newSeg, _ := executor.Execute(plan)
					*physicalBytes += newSeg.Length
					*compactionCount++
				}
			}
		} else {
			// Read
			key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
			segs := meta.GetAllSegments()

			*totalReads++
			found := false
			for _, seg := range segs {
				*totalSegmentScans++
				if !found && key >= seg.MinKey && key <= seg.MaxKey {
					sstReader.Get(seg, key)
					meta.UpdateStats(seg.ID, 1, 0)
					found = true
				}
			}
		}

		if i > 0 && i%200000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numOps)
		}
	}
	fmt.Println()

	// Compaction
	time.Sleep(2 * time.Second)
	if plan := director.MaybePlan(); plan != nil {
		newSeg, _ := executor.Execute(plan)
		*physicalBytes += newSeg.Length
		*compactionCount++
	}
}

func runZipfian(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes, totalReads, totalSegmentScans *int64,
	compactionCount *int, totalOps *int64) {
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
		*totalOps++

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
			*physicalBytes += seg.Length
			meta.RegisterSegment(seg)
			// ALWAYS compact in leveled
			if plan := director.MaybePlan(); plan != nil {
				newSeg, _ := executor.Execute(plan)
				*physicalBytes += newSeg.Length
				*compactionCount++
			}
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numKeys)
		}
	}
	fmt.Println()

	// Final flush
	if mem.ShouldFlush() {
		data := mem.Flush()
		seg, _ := sstWriter.WriteSegment(data, common.LEVELED)
		*physicalBytes += seg.Length
		meta.RegisterSegment(seg)
		// ALWAYS compact in leveled
		if plan := director.MaybePlan(); plan != nil {
			newSeg, _ := executor.Execute(plan)
			*physicalBytes += newSeg.Length
			*compactionCount++
		}
	}

	// Phase 2: Zipfian reads
	fmt.Println("Pre-computing Zipfian distribution (s=1.5)...")
	zipf := NewZipfianGenerator(numKeys, 1.5)

	fmt.Println("Reading with Zipfian distribution...")
	numReads := numKeys * 3
	hotKeyAccesses := make(map[int]int)

	for i := 0; i < numReads; i++ {
		keyIdx := zipf.Next()
		hotKeyAccesses[keyIdx]++

		key := fmt.Sprintf("key-%010d", keyIdx)
		segs := meta.GetAllSegments()

		*totalReads++
		*totalOps++
		found := false
		for _, seg := range segs {
			*totalSegmentScans++
			if !found && key >= seg.MinKey && key <= seg.MaxKey {
				sstReader.Get(seg, key)
				meta.UpdateStats(seg.ID, 1, 0)
				found = true
			}
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

	// Compaction
	time.Sleep(2 * time.Second)
	if plan := director.MaybePlan(); plan != nil {
		fmt.Printf("  Compaction triggered: %s\n", plan.Reason)
		newSeg, _ := executor.Execute(plan)
		*physicalBytes += newSeg.Length
		*compactionCount++
	}
}
