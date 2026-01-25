# Amethyst LSM-Tree (Control/Baseline)

A baseline implementation of an LSM-Tree storage engine in Go. This serves as the control for benchmarking against adaptive compaction strategies.

## Features

- **Write-Ahead Log (WAL)**: Durability guarantees
- **Memtable**: In-memory sorted buffer
- **SSTable**: Sorted String Table persistence
- **Sparse Index**: Efficient key lookup
- **Leveled Compaction**: Default base LSM strategy
- **Benchmark Controllers**: Static tiered and leveled strategies for comparison

## Architecture

```
Write Path: Put() → WAL → Memtable → Flush → SSTable (TIERED)
Read Path:  Get() → Memtable → SSTable (with sparse index)
Compaction: Background merge (TIERED → LEVELED)
```

## Usage

### Run the main demo:
```bash
cd amethyst
go run ./cmd/amethystd
```

### Compare compaction strategies:
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

## Compaction Strategies

**Base LSM (Built-in Leveled)**:
- Triggers on read count > 10 or overlap
- Always compacts to LEVELED strategy

**Static Tiered** (benchmark):
- Triggers on write count > 50
- Stays TIERED after compaction

**Static Leveled** (benchmark):
- Triggers on read count > 10 or overlap
- Compacts to LEVELED strategy

## Extending for Benchmarking

To add new compaction strategies, implement the `Controller` interface:

```go
type Controller interface {
    ShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string)
}
```

Then use it with:
```go
director := compaction.NewDirector(meta, yourController)
```
