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
	input := plan.Inputs[0]
	merged := make(map[string][]byte)

	// Scan all input segments and merge (last write wins)
	for _, seg := range plan.Inputs {
		data, err := e.reader.Scan(seg)
		if err != nil {
			return nil, err
		}

		// Merge: later segments in the list override earlier ones
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

	log.Printf("REWRITE %v → %v (%s)", input.Strategy, newSeg.Strategy, plan.Reason)
	return newSeg, nil
}
