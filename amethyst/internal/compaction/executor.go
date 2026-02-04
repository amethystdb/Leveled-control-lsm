package compaction

import (
	"amethyst/internal/common"
	"amethyst/internal/metadata"
	"amethyst/internal/sstable/reader"
	"amethyst/internal/sstable/writer"
	"log"
	"sort"
)

type Executor interface {
	Execute(plan *Plan) (*common.SegmentMeta, error)
}

type executor struct {
	meta   metadata.Tracker
	reader reader.SSTableReader
	writer writer.SSTableWriter
}

func NewExecutor(
	meta metadata.Tracker,
	reader reader.SSTableReader,
	writer writer.SSTableWriter,
) *executor {
	return &executor{
		meta:   meta,
		reader: reader,
		writer: writer,
	}
}

func (e *executor) Execute(plan *Plan) (*common.SegmentMeta, error) {
	merged := make(map[string][]byte)

	// 1. Merge all input segments (Oldest to Newest)
	// Process in reverse so newer values override older ones
	for i := len(plan.Inputs) - 1; i >= 0; i-- {
		seg := plan.Inputs[i]
		data, err := e.reader.Scan(seg)
		if err != nil {
			return nil, err
		}
		for k, v := range data {
			merged[k] = v
		}
	}

	// 2. Extract and Sort Keys (SSTables must be sorted on disk)
	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 3. Convert Map to KVEntry slice for writer
	finalEntries := make([]common.KVEntry, 0, len(keys))
	for _, k := range keys {
		val := merged[k]
		finalEntries = append(finalEntries, common.KVEntry{
			Key:       k,
			Value:     val,
			Tombstone: val == nil, // If value is nil, it's a delete
		})
	}

	// 4. Write the sorted entries to disk
	newSeg, err := e.writer.WriteSegment(finalEntries, plan.OutputStrategy)
	if err != nil {
		return nil, err
	}

	e.meta.RegisterSegment(newSeg)

	// Mark old segments obsolete
	for _, seg := range plan.Inputs {
		e.meta.MarkObsolete(seg.ID)
	}

	strategyName := "LEVELED"

	log.Printf("COMPACT %d segments → %s strategy (%s)", len(plan.Inputs), strategyName, plan.Reason)
	return newSeg, nil
}