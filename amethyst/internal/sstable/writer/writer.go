package writer

import (
	"amethyst/internal/common"
	"amethyst/internal/segmentfile"
	"amethyst/internal/sparseindex"
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

var GlobalPhysicalWriteBytes int64 = 0
var GlobalCompactionWriteBytes int64 = 0

type SSTableWriter interface {
	WriteSegment(
		sortedData []common.KVEntry,
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
	sortedData []common.KVEntry,
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
	if len(sortedData) > 0 {
		minKey = sortedData[0].Key
		maxKey = sortedData[len(sortedData)-1].Key
	}

	writeString(minKey)
	writeString(maxKey)
	buf = append(buf, byte(strategy))
	tmp8 := make([]byte, 8)
	binary.BigEndian.PutUint64(tmp8, uint64(len(sortedData)))
	buf = append(buf, tmp8...)

	//actual data entry
	offsetsForIndex := make([]int64, 0, len(sortedData))
	keysForIndex := make([]string, 0, len(sortedData))
	dataStartOffset := int64(len(buf))

	// 3. Data Entry Loop
	for _, entry := range sortedData {
		offsetsForIndex = append(offsetsForIndex, int64(len(buf))-dataStartOffset)
		keysForIndex = append(keysForIndex, entry.Key)

		// Write Lengths (4 bytes key, 4 bytes value) + 1 byte Tombstone
		tmp := make([]byte, 9)
		binary.BigEndian.PutUint32(tmp[0:4], uint32(len(entry.Key)))
		binary.BigEndian.PutUint32(tmp[4:8], uint32(len(entry.Value)))

		// Physically write the tombstone bit
		if entry.Tombstone {
			tmp[8] = 1
		} else {
			tmp[8] = 0
		}

		buf = append(buf, tmp...)
		buf = append(buf, []byte(entry.Key)...)
		buf = append(buf, entry.Value...)
	}
	//sparseindex
	sparse := w.indexBuilder.Build(keysForIndex, offsetsForIndex)
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

	// Track write metrics
	atomic.AddInt64(&GlobalPhysicalWriteBytes, length)
	// All writes are compaction writes in this implementation (no TIERED constant)
	atomic.AddInt64(&GlobalCompactionWriteBytes, length)

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
