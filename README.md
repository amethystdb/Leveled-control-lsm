# Amethyst LSM-Tree (Leveled Compaction)

A clean implementation of an LSM-Tree storage engine with leveled compaction in Go. This version uses pure leveled compaction strategy throughout.

## Features

- **Write-Ahead Log (WAL)**: Durability guarantees
- **Memtable**: In-memory sorted buffer
- **SSTable**: Sorted String Table persistence
- **Sparse Index**: Efficient key lookup
- **Leveled Compaction**: Read-optimized compaction strategy

## Architecture

```
Write Path: Put() → WAL → Memtable → Flush → SSTable (LEVELED)
Read Path:  Get() → Memtable → SSTable (with sparse index)
Compaction: Background merge (LEVELED strategy)
```

## Usage

### Run the main demo:
```bash
cd amethyst
go run ./cmd/amethystd
```

### Test leveled compaction strategy:
```bash
cd amethyst
go run test_strategies.go
```

### Run tests:
```bash
cd amethyst
go test ./...
```

## Project Structure

```
amethyst/
├── cmd/amethystd/          # Main executable
├── internal/
│   ├── benchmarks/         # Static strategy controllers
│   ├── compaction/         # Compaction logic (director + executor)
│   ├── engine/            # Core LSM engine
│   ├── memtable/          # In-memory buffer
│   ├── metadata/          # Segment tracking
│   ├── sstable/           # SSTable reader/writer
│   ├── sparseindex/       # Efficient key indexing
│   ├── segmentfile/       # Disk I/O management
│   └── wal/               # Write-ahead log
└── test_strategies.go     # Strategy comparison tool
```

## Compaction Strategy

**Leveled Compaction**:
- Triggers on read count > 10 or overlap detection
- Optimized for read-heavy workloads
- Reduces fragmentation through background merging
- All segments use LEVELED strategy

## Extending the System

To customize the compaction strategy, implement the `Controller` interface:

```go
type Controller interface {
    ShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string)
}
```

Then use it with:
```go
director := compaction.NewDirector(meta, yourController)
```
