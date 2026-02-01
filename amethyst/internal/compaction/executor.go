package compaction

import (
	"amethyst/internal/common"
	"amethyst/internal/metadata"
	"amethyst/internal/sstable/reader"
	"amethyst/internal/sstable/writer"
	"log"
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

	// CRITICAL: Process from oldest to newest so newer values override older ones
	// Since metadata.ordered is newest-first, we need to reverse iteration
	// Process oldest first (end of list) → newest last (start of list)
	for i := len(plan.Inputs) - 1; i >= 0; i-- {
		seg := plan.Inputs[i]
		data, err := e.reader.Scan(seg)
		if err != nil {
			return nil, err
		}

		// Merge: write to map, newer values will override
		for k, v := range data {
			merged[k] = v
		}
	}

	// Write new segment with the target strategy
	newSeg, err := e.writer.WriteSegment(merged, plan.OutputStrategy)
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
