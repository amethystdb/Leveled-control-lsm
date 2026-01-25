package memtable

import (
	"amethyst/internal/common"
	"sort" //sort keys in Flush
	"sync"
)

type Memtable interface {
	Put(key string, value []byte)
	Delete(key string)
	Get(key string) ([]byte, bool)

	ShouldFlush() bool
	Flush() map[string][]byte
}

type memtable struct {
	data       map[string]common.WALEntry
	maxEntries int
	mu         sync.RWMutex
}

func NewMemtable(maxEntries int) Memtable {
	return &memtable{
		data:       make(map[string]common.WALEntry),
		maxEntries: maxEntries,
	}
}

func (m *memtable) Put(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = common.WALEntry{Key: key, Value: value, Tombstone: false}
}

func (m *memtable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = common.WALEntry{Key: key, Tombstone: true}
}

func (m *memtable) Get(key string) ([]byte, bool) {
	m.mu.RLock() // Request shared read access
	defer m.mu.RUnlock()

	entry, ok := m.data[key]
	if !ok || entry.Tombstone {
		return nil, false //not found if missing
	}
	return entry.Value, true
}

// returns true if mem is full
func (m *memtable) ShouldFlush() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data) >= m.maxEntries
}

// clears data and returns sorted for SSTable Writer
func (m *memtable) Flush() map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	//keys to sort
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}

	//sorted for SSTable
	sort.Strings(keys)

	//sorted output map
	result := make(map[string][]byte)
	for _, k := range keys {
		entry := m.data[k]
		if !entry.Tombstone {
			result[k] = entry.Value
		}
	}

	//reset internal map for new batch
	m.data = make(map[string]common.WALEntry)
	return result
}
