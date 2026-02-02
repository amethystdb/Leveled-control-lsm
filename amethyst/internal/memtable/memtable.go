package memtable

import (
	"amethyst/internal/common" //sort keys in Flush
	"sync"
)

type Memtable interface {
	Put(key string, value []byte)
	Delete(key string)
	Get(key string) ([]byte, bool)

	ShouldFlush() bool
	Flush() []common.KVEntry
}

type memtable struct {
	data       []common.KVEntry
	maxEntries int
	mu         sync.RWMutex
}

func NewMemtable(maxEntries int) Memtable {
	return &memtable{
		data:       make([]common.KVEntry, 0),
		maxEntries: maxEntries,
	}
}

func (m *memtable) Put(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = append(m.data, common.KVEntry{Key: key, Value: value, Tombstone: false})
}

func (m *memtable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = append(m.data, common.KVEntry{Key: key, Tombstone: true})
}

func (m *memtable) Get(key string) ([]byte, bool) {
	m.mu.RLock() // Request shared read access
	defer m.mu.RUnlock()

	// Search backwards to get most recent entry for the key
	for i := len(m.data) - 1; i >= 0; i-- {
		if m.data[i].Key == key {
			if m.data[i].Tombstone {
				return nil, false
			}
			return m.data[i].Value, true
		}
	}
	return nil, false
}

// returns true if mem is full
func (m *memtable) ShouldFlush() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data) >= m.maxEntries
}

func (m *memtable) Flush() []common.KVEntry {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Return a copy and reset internal state
	data := m.data
	m.data = make([]common.KVEntry, 0)
	return data
}
