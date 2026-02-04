package reader

import (
	"amethyst/internal/common"
	"amethyst/internal/segmentfile"
	"amethyst/internal/sparseindex"
	"bytes"
	"encoding/binary"
)

type SSTableReader interface {
	Get(meta *common.SegmentMeta, key string) ([]byte, bool)
	Scan(meta *common.SegmentMeta) (map[string][]byte, error)
}

type Reader struct {
	fileMgr segmentfile.SegmentFileManager
}

func NewReader(fileMgr segmentfile.SegmentFileManager) *Reader {
	return &Reader{fileMgr: fileMgr}
}

func (r *Reader) Get(meta *common.SegmentMeta, target string) ([]byte, bool) {
	// Fast reject by key range
	if target < meta.MinKey || target > meta.MaxKey {
		return nil, false
	}

	idx, ok := meta.SparseIndex.(*sparseindex.SparseIndex)
	if !ok || idx == nil {
		return nil, false
	}

	// Get mmapped data
	mmapData, err := r.fileMgr.GetMmapData()
	if err != nil {
		return nil, false
	}

	// Compute absolute start offset
	start := meta.Offset + meta.DataStartOffset + idx.Seek(target)
	end := meta.Offset + meta.SparseIndexOffset

	// Check bounds
	if start < 0 || end > int64(len(mmapData)) || start > end {
		return nil, false
	}

	// Use direct slice from mmap - zero copy!
	data := mmapData[start:end]
	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
		var kLen uint32
		var vLen uint32
		var tomb byte

		if err := binary.Read(buf, binary.BigEndian, &kLen); err != nil {
			return nil, false
		}
		if err := binary.Read(buf, binary.BigEndian, &vLen); err != nil {
			return nil, false
		}
		if err := binary.Read(buf, binary.BigEndian, &tomb); err != nil {
			return nil, false
		}

		keyBytes := make([]byte, kLen)
		if _, err := buf.Read(keyBytes); err != nil {
			return nil, false
		}
		key := string(keyBytes)

		var valBytes []byte
		if vLen > 0 {
			valBytes = make([]byte, vLen)
			if _, err := buf.Read(valBytes); err != nil {
				return nil, false
			}
		}

		if key == target {
			if tomb == 1 {
				return nil, false
			}
			return valBytes, true
		}

		// Sorted order invariant: stop early
		if key > target {
			return nil, false
		}
	}

	return nil, false
}

func (r *Reader) Scan(meta *common.SegmentMeta) (map[string][]byte, error) {
	result := make(map[string][]byte)

	// Get mmapped data
	mmapData, err := r.fileMgr.GetMmapData()
	if err != nil {
		return nil, err
	}

	start := meta.Offset + meta.DataStartOffset
	end := meta.Offset + meta.SparseIndexOffset

	// Check bounds
	if start < 0 || end > int64(len(mmapData)) || start > end {
		return nil, nil
	}

	// Use direct slice from mmap - zero copy!
	data := mmapData[start:end]
	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
		var kLen, vLen uint32
		var tomb byte

		if err := binary.Read(buf, binary.BigEndian, &kLen); err != nil {
			break
		}
		if err := binary.Read(buf, binary.BigEndian, &vLen); err != nil {
			break
		}
		if err := binary.Read(buf, binary.BigEndian, &tomb); err != nil {
			break
		}

		keyBytes := make([]byte, kLen)
		if _, err := buf.Read(keyBytes); err != nil {
			break
		}
		key := string(keyBytes)

		var valBytes []byte
		if vLen > 0 {
			valBytes = make([]byte, vLen)
			if _, err := buf.Read(valBytes); err != nil {
				break
			}
		}

		if tomb == 1 {
			result[key] = nil
		} else {
			result[key] = valBytes
		}
	}
	return result, nil
}
