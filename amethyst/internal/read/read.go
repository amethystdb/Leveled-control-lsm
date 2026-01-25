package read

import (
	"amethyst/internal/memtable"
	"amethyst/internal/metadata"
	"amethyst/internal/sstable/reader"
)

type Handler struct {
	memtable memtable.Memtable
	meta     metadata.Tracker
	reader   reader.SSTableReader
}

func NewHandler(
	mem memtable.Memtable,
	meta metadata.Tracker,
	reader reader.SSTableReader,
) *Handler {
	return &Handler{
		memtable: mem,
		meta:     meta,
		reader:   reader,
	}
}

func (h *Handler) Get(key string) ([]byte, bool) {
	// 1. Memtable first
	if val, ok := h.memtable.Get(key); ok {
		return val, true
	}

	// 2. On-disk segments (newest → oldest)
	segs := h.meta.GetSegmentsForKey(key)

	for _, seg := range segs {
		val, ok := h.reader.Get(seg, key)
		h.meta.UpdateStats(seg.ID, 1, 0)

		if ok {
			return val, true
		}
	}

	return nil, false
}
