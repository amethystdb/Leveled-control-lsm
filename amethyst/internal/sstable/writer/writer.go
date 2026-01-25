package writer

import (
	"amethyst/internal/common"
	"amethyst/internal/segmentfile"
	"amethyst/internal/sparseindex"
	"encoding/binary"
	"github.com/google/uuid"
	"sort"
	"time"
)

type SSTableWriter interface {
	WriteSegment(
		sortedData map[string][]byte,
		strategy common.CompactionType,
	) (*common.SegmentMeta, error)
}

type writer struct {
	fileMgr      segmentfile.SegmentFileManager
	indexBuilder sparseindex.Builder
}

func NewWriter(fileMgr segmentfile.SegmentFileManager, indexBuilder sparseindex.Builder) *writer {
	return &writer{
		fileMgr:      fileMgr,
		indexBuilder: indexBuilder,
	}
}

func (w *writer) WriteSegment(
	sortedData map[string][]byte,
	strategy common.CompactionType,
) (*common.SegmentMeta, error) {
	segmentID := uuid.New().String()
	now := time.Now().Unix()

	buf := make([]byte, 0, 1024)

	// header
	writeString := func(s string) {
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(len(s)))
		buf = append(buf, tmp...)
		buf = append(buf, []byte(s)...)
	}
	writeString(segmentID)

	var minKey, maxKey string
	first := true

	keys := make([]string, 0, len(sortedData))
	for k := range sortedData {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	//keys are sorted by memtable contract
	for _, key := range keys {
		if first {
			minKey = key
			first = false

		}
		maxKey = key
	}
	writeString(minKey)
	writeString(maxKey)
	buf = append(buf, byte(strategy))
	tmp8 := make([]byte, 8)
	binary.BigEndian.PutUint64(tmp8, uint64(len(sortedData)))
	buf = append(buf, tmp8...)

	//actual data entry
	offsets := make([]int64, 0, len(keys))
	dataStartOffset := int64(len(buf))

	for _, key := range keys {
		offsets = append(offsets, int64(len(buf))-dataStartOffset)
		val := sortedData[key]
		tombstone := val == nil

		tmp := make([]byte, 9)
		binary.BigEndian.PutUint32(tmp[0:4], uint32(len(key)))
		binary.BigEndian.PutUint32(tmp[4:8], uint32(len(val)))
		if tombstone {
			tmp[8] = 1
		} else {
			tmp[8] = 0
		}
		buf = append(buf, tmp...)
		buf = append(buf, []byte(key)...)
		buf = append(buf, val...)

	}
	//sparseindex
	sparse := w.indexBuilder.Build(keys, offsets)
	//serialize sparse index
	sparseOffset := int64(len(buf))

	for i, k := range sparse.Keys {
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(len(k)))
		buf = append(buf, tmp...)
		buf = append(buf, []byte(k)...)
		tmp8 := make([]byte, 8)
		binary.BigEndian.PutUint64(tmp8, uint64(sparse.Offsets[i]))
		buf = append(buf, tmp8...)
	}

	//fooooooter
	tmp8 = make([]byte, 8)
	binary.BigEndian.PutUint64(tmp8, uint64(sparseOffset))
	buf = append(buf, tmp8...)

	//writing to disk
	offset, length, err := w.fileMgr.Append(buf)
	if err != nil {
		return nil, err
	}

	meta := &common.SegmentMeta{
		ID:     segmentID,
		Offset: offset,
		Length: length,

		MinKey: minKey,
		MaxKey: maxKey,

		Strategy: strategy,

		ReadCount:    0,
		WriteCount:   0,
		OverlapCount: 0,

		CreatedAt:     now,
		LastRewriteAt: now,

		Obsolete:          false,
		SparseIndex:       sparse,
		DataStartOffset:   dataStartOffset,
		SparseIndexOffset: sparseOffset,
	}
	return meta, nil
}
