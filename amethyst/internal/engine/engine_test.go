package engine

import (
	"amethyst/internal/common"
	"amethyst/internal/memtable"
	"amethyst/internal/segmentfile"
	"amethyst/internal/wal"
	"os"
	"testing"
)

// Writer Mock
type MockWriter struct{}

func (m *MockWriter) WriteSegment(data map[string][]byte, s common.CompactionType) (*common.SegmentMeta, error) {
	return &common.SegmentMeta{ID: "test-1"}, nil
}

func TestEngineWritePath(t *testing.T) {
	//Setup
	w, _ := wal.NewDiskWAL("test.wal")
	m := memtable.NewMemtable(2) //Small limit to trigger flush
	s, _ := segmentfile.NewSegmentFileManager("test.db")
	sw := &MockWriter{}

	//Create the engine
	db := NewEngine(w, m, s, sw)

	//Pipe WAL and Memtable
	db.Put("key1", []byte("val1"))
	db.Put("key2", []byte("val2")) //triggers e.ExecuteFlush()

	//Verify
	if _, found := m.Get("key1"); found {
		t.Error("Expected memtable to be cleared after flush")
	}

	// Clean up files
	os.Remove("test.wal")
	os.Remove("test.db")
}
