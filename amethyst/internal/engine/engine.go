package engine

import (
	"amethyst/internal/common"
	"amethyst/internal/memtable"
	"amethyst/internal/segmentfile"
	"amethyst/internal/sstable/writer"
	"amethyst/internal/wal"
	"fmt"
)

type Engine struct {
	wal    wal.WAL
	mem    memtable.Memtable
	sfm    segmentfile.SegmentFileManager
	writer writer.SSTableWriter
}

// initializes pipe
func NewEngine(w wal.WAL, m memtable.Memtable, s segmentfile.SegmentFileManager, sw writer.SSTableWriter) *Engine {
	return &Engine{
		wal:    w,
		mem:    m,
		sfm:    s,
		writer: sw,
	}
}

// handles the WAL -> Memtable flow
func (e *Engine) Put(key string, value []byte) error {
	// Log to WAL for durability
	if err := e.wal.LogPut(key, value); err != nil {
		return fmt.Errorf("WAL log failure: %w", err)
	}

	//Insert into Memtable
	e.mem.Put(key, value)

	//Check if Memtable reached its limit
	if e.mem.ShouldFlush() {
		return e.ExecuteFlush()
	}

	return nil
}

// handles the Memtable -> SSTable -> Truncate flow
func (e *Engine) ExecuteFlush() error {
	fmt.Println("Threshold reached: Starting Flush Plumbing...")

	data := e.mem.Flush()
	if len(data) == 0 {
		return nil
	}

	// Assign to 'seg' and actually use it (or use _ if you really don't need it)
	seg, err := e.writer.WriteSegment(data, common.LEVELED)
	if err != nil {
		return fmt.Errorf("SSTable write failure: %w", err)
	}

	// LOG OR REGISTER 'seg' TO REMOVE THE COMPILER ERROR
	fmt.Printf("Created new Leveled segment: %s (Size: %d bytes)\n", seg.ID, seg.Length)

	if err := e.wal.Truncate(); err != nil {
		return fmt.Errorf("WAL cleanup failure: %w", err)
	}

	return nil
}
