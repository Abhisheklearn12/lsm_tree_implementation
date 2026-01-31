# LSM Tree in Rust

A simple, educational implementation of a Log-Structured Merge Tree (LSM Tree) in Rust with Write-Ahead Log for crash recovery, Bloom filters for fast negative lookups, and a beautiful interactive TUI explorer.

## What is an LSM Tree?

LSM trees are write-optimized data structures used in databases like LevelDB, RocksDB, Cassandra, and HBase. They achieve high write throughput by:

1. **Batching writes in memory** (MemTable) - O(log n) insertion
2. **Flushing to immutable disk files** (SSTables) when memory fills
3. **Reading newest-to-oldest** - recent data shadows old data
4. **Using Bloom filters** - skip SSTables that definitely don't contain a key

This makes LSM trees ideal for write-heavy workloads like time-series data, logs, and event streams.

## Features

- In-memory MemTable with automatic flush
- Persistent SSTables on disk
- **Write-Ahead Log (WAL)** for crash recovery
- **Bloom filters** for fast negative lookups (skip unnecessary disk reads)
- **Interactive TUI** - beautiful terminal interface with ratatui
- Correct read semantics (newest wins)
- Simple binary format for SSTables
- Configurable Bloom filter false positive rate
- 1000+ lines of educational comments
- Comprehensive unit and integration tests

## Quick Start

```bash
# Interactive TUI Explorer (recommended!)
cargo run --bin lsm-cli

# Simple demo
cargo run --bin lsm-demo

# Run tests
cargo test
```

## Interactive TUI

The interactive TUI (`lsm-cli`) provides a beautiful interface to explore the LSM tree:

```
┌─────────────────────────────────────────────────────────────────┐
│              LSM Tree Explorer [Bloom Filters Enabled]          │
├─────────────────────────────────────────────────────────────────┤
│ [1] Dashboard │ [2] MemTable │ [3] SSTables │ [4] Bloom Filters │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────┐  ┌───────────────────────────────────┐ │
│  │ System Overview     │  │ Bloom Filter Stats                │ │
│  │ MemTable: 5 entries │  │ Skip Rate: 93.1%                  │ │
│  │ SSTables: 3         │  │ Reads Skipped: 312                │ │
│  │ Bloom Filters: 3    │  │ Reads Proceeded: 23               │ │
│  └─────────────────────┘  └───────────────────────────────────┘ │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ MemTable Fill Level  [████████░░░░░░░░░░] 45%               ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│ NORMAL │ p:put g:get f:flush d:demo h:help q:quit               │
└─────────────────────────────────────────────────────────────────┘
```

### TUI Features

- **Dashboard**: Real-time overview of system state, memtable fill gauge, Bloom filter effectiveness
- **MemTable View**: See all key-value pairs currently in memory
- **SSTables View**: Browse entries in each SSTable file, see associated Bloom filter stats
- **Bloom Filters View**: Detailed statistics for each filter including fill ratio and FPP

### TUI Keyboard Shortcuts (Still Under Active Development)

| Key | Action |
|-----|--------|
| `1-4` or `Tab` | Switch between tabs |
| `p` or `i` | Put a new key-value pair |
| `g` or `/` | Get/search for a key |
| `f` | Flush memtable to SSTable |
| `d` | Toggle auto-demo mode |
| `r` | Reset Bloom filter statistics |
| `j/k` or `↑/↓` | Scroll through entries |
| `←/→` | Switch between SSTables |
| `h` | Show help |
| `q` | Quit |

## Usage as Library

```rust
use lsm_tree::LSMTree;
use std::path::PathBuf;

fn main() {
    // Create LSM tree with 4MB memtable threshold
    let mut lsm = LSMTree::new(
        PathBuf::from("./data"),
        4 * 1024 * 1024
    ).expect("Failed to create LSM tree");

    // Insert key-value pairs
    lsm.put(b"user:123".to_vec(), b"Alice".to_vec())
        .expect("Failed to put");
    lsm.put(b"user:456".to_vec(), b"Bob".to_vec())
        .expect("Failed to put");

    // Retrieve values (Bloom filters optimize this!)
    if let Some(value) = lsm.get(b"user:123") {
        println!("Found: {}", String::from_utf8_lossy(&value));
    }

    // Updates override old values
    lsm.put(b"user:123".to_vec(), b"Alice Smith".to_vec())
        .expect("Failed to update");

    // Check Bloom filter statistics
    let stats = lsm.bloom_filter_stats();
    println!("Skip rate: {:.1}%", stats.skip_rate() * 100.0);
}
```

## Bloom Filters

### What is a Bloom Filter?

A Bloom filter is a space-efficient probabilistic data structure that tells you:
- **"Definitely not in set"** - 100% accurate, never wrong
- **"Possibly in set"** - might be a false positive

For LSM trees, this is perfect because:
- We can skip reading SSTable files for keys that definitely don't exist
- False positives only cost an extra file read (acceptable)
- False negatives never happen (data integrity preserved)

### How It Works

```
                  Key Lookup: "user:999"
                         |
                         v
    +--------------------------------------------+
    |              MemTable                      |
    |         (not found here)                   |
    +--------------------------------------------+
                         |
                         v
    +--------------------------------------------+
    |     Bloom Filter for SSTable 0             |
    |   might_contain("user:999") = false        |  --> SKIP! (saved disk read)
    +--------------------------------------------+
                         |
                         v
    +--------------------------------------------+
    |     Bloom Filter for SSTable 1             |
    |   might_contain("user:999") = false        |  --> SKIP! (saved disk read)
    +--------------------------------------------+
                         |
                         v
              Return None (key not found)
```

### Configuration

```rust
// Default: 1% false positive rate
let lsm = LSMTree::new(path, threshold)?;

// Custom: 0.1% false positive rate (larger filters, fewer false positives)
let lsm = LSMTree::with_bloom_filter_fpp(path, threshold, 0.001)?;
```

## Architecture

> **Architecture for detailed technical diagrams.**

### System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           LSM TREE SYSTEM                               │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                      APPLICATION LAYER                          │    │
│  │   put(key, value)    get(key)    flush()    bloom_filter_stats()│    │
│  └──────────────────────────────┬──────────────────────────────────┘    │
│                                 │                                       │
│  ┌──────────────────────────────▼──────────────────────────────────┐    │
│  │                        LSMTree                                  │    │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │    │
│  │  │    MemTable     │  │  Bloom Filters  │  │       WAL       │  │    │
│  │  │   (BTreeMap)    │  │   (Vec<BF>)     │  │   (Append-only) │  │    │
│  │  │   [In Memory]   │  │   [In Memory]   │  │     [Disk]      │  │    │
│  │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘  │    │
│  └───────────┼────────────────────┼────────────────────┼───────────┘    │
│              │                    │                    │                │
│              │    FLUSH           │                    │                │
│              ▼                    ▼                    ▼                │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                         DISK STORAGE                            │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │    │
│  │  │ sstable_0.db │  │ sstable_1.db │  │ sstable_2.db │  ...      │    │
│  │  │ sstable_0.   │  │ sstable_1.   │  │ sstable_2.   │           │    │
│  │  │    bloom     │  │    bloom     │  │    bloom     │           │    │
│  │  │   [NEWEST]   │  │              │  │   [OLDEST]   │           │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘           │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
WRITE PATH                              READ PATH
==========                              =========

    put(k,v)                                get(k)
        │                                      │
        ▼                                      ▼
   ┌─────────┐                           ┌─────────┐
   │   WAL   │ ◄── fsync (durability)    │MemTable │ ◄── O(log n) lookup
   └────┬────┘                           └────┬────┘
        │                                     │ miss
        ▼                                     ▼
   ┌─────────┐                           ┌─────────┐
   │MemTable │ ◄── O(log n) insert       │ Bloom   │ ◄── O(k) check
   └────┬────┘                           │ Filter 0│
        │ threshold                      └────┬────┘
        ▼                                     │ maybe
   ┌─────────┐                                ▼
   │  FLUSH  │ ───────────────────►     ┌─────────┐
   │         │  write SSTable + BF      │SSTable 0│ ◄── O(m) scan
   └─────────┘                          └────┬────┘
                                             │ miss
                                             ▼
                                        (repeat for
                                         SSTable 1,2...)
```

**Write Path:** WAL -> Memory -> Disk (when threshold exceeded)
**Read Path:** Memory -> Bloom Filter[i] -> SSTable[i] -> ... (until found)
**Recovery Path:** WAL -> Replay to MemTable (on startup)
**Bloom Filter:** Created during flush, loaded from .bloom files on startup

## File Formats

### SSTable Format
```
[key_len: u32][key: bytes][value_len: u32][value: bytes]...
```

### Bloom Filter Format (.bloom files)
```
[num_bits: u32][num_hashes: u32][num_items: u32][bit_array: bytes]
```

### WAL Format
```
[op_type: u8][key_len: u32][key: bytes][value_len: u32][value: bytes]...
```

Each entry is self-contained with length prefixes for easy parsing.

## Project Structure

```
lsm_tree/
├── src/
│   ├── lib.rs           <- LSM Tree library (core implementation)
│   ├── main.rs          <- Entry point
│   ├── bloom_filter.rs  <- Bloom filter implementation
│   ├── wal.rs           <- Write-Ahead Log implementation
│   └── bin/
│       ├── cli.rs       <- Interactive TUI (ratatui)
│       └── demo.rs      <- Simple demo
├── lsm_data/            <- Created at runtime
│   ├── wal.log          <- Write-Ahead Log file
│   ├── sstable_0.db     <- SSTable data files
│   ├── sstable_0.bloom  <- Bloom filter files
│   └── ...
├── Cargo.toml
└── README.md
```

## What's Missing (For Production)

This is an educational implementation. Real LSM trees add:

- ~~**Bloom filters**~~ - **Implemented!** Skip SSTables that definitely don't have a key
- ~~**WAL (Write-Ahead Log)**~~ - **Implemented!** Crash recovery for memtable
- **Compaction** - Merge old SSTables to reclaim space
- **Sparse indexes** - Jump to key ranges without full scan
- **Compression** - Reduce disk usage (Snappy, LZ4)
- **Multiple levels** - Tiered storage for better read performance
- **Range queries** - Scan keys in a range
- **Delete tombstones** - Mark keys as deleted

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Write | O(log n) | WAL write + memtable insert |
| Read (hit memtable) | O(log n) | Best case |
| Read (Bloom filter skip) | O(k) | k = number of hash functions |
| Read (hit SSTable) | O(k × m) | k = #SSTables, m = entries per file |
| Recovery | O(n) | n = WAL entries (typically small) |
| Space (Bloom filters) | ~1.2 bytes/key | For 1% false positive rate |

### Bloom Filter Performance

With Bloom filters, negative lookups (searching for keys that don't exist) are dramatically faster:

| Scenario | Without Bloom Filter | With Bloom Filter |
|----------|---------------------|-------------------|
| Key not in any SSTable | Read all SSTables | Skip all SSTables |
| Key in first SSTable | Read 1 SSTable | Read 1 SSTable |
| Key in last SSTable | Read all SSTables | Read 1-N SSTables |

**Example**: With 100 SSTables, each containing 10,000 entries:
- Without Bloom filters: 100 file reads for a non-existent key
- With Bloom filters: 0 file reads (all skipped)

## Testing

```bash
# Run all tests
cargo test

# Run specific module tests
cargo test bloom_filter
cargo test wal

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_bloom_filter_no_false_negatives
```

### Test Coverage

- **lib.rs**: 2 integration tests
- **bloom_filter.rs**: 12 unit tests
  - Basic operations, false positive rate, serialization, edge cases
- **wal.rs**: 5 unit tests
  - Append/recover, clear, order preservation

## Dependencies

```toml
[dependencies]
ratatui = "0.29"    # Terminal UI framework
crossterm = "0.28"  # Cross-platform terminal manipulation
```

## API Reference

### LSMTree

```rust
// Create new LSM tree
LSMTree::new(data_dir: PathBuf, memtable_size_threshold: usize) -> Result<Self>

// Create with custom Bloom filter false positive rate
LSMTree::with_bloom_filter_fpp(data_dir: PathBuf, threshold: usize, fpp: f64) -> Result<Self>

// Insert or update a key-value pair
fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>

// Retrieve a value (mutable for statistics tracking)
fn get(&mut self, key: &[u8]) -> Option<Vec<u8>>

// Retrieve a value (immutable, no statistics)
fn get_immut(&self, key: &[u8]) -> Option<Vec<u8>>

// Flush memtable to disk
fn flush(&mut self) -> Result<()>

// Get number of entries in memtable
fn len(&self) -> usize

// Check if tree is empty
fn is_empty(&self) -> bool

// Get number of SSTables
fn sstable_count(&self) -> usize

// Get Bloom filter statistics
fn bloom_filter_stats(&self) -> BloomFilterSummary

// Reset Bloom filter statistics
fn reset_bloom_filter_stats(&mut self)
```

### BloomFilter

```rust
// Create with expected items and false positive rate
BloomFilter::new(expected_items: usize, false_positive_rate: f64) -> Self

// Insert a key
fn insert(&mut self, key: &[u8])

// Check if key might exist
fn might_contain(&self, key: &[u8]) -> bool

// Statistics
fn stats(&self) -> BloomFilterStats
fn estimated_false_positive_rate(&self) -> f64
```

## Features

### Dashboard Tab
Shows system overview with memtable fill gauge and Bloom filter effectiveness metrics.

### MemTable Tab
Displays all key-value pairs currently in memory, with size tracking.

### SSTables Tab
Browse entries in each SSTable file with navigation between tables.

### Bloom Filters Tab
Detailed per-filter statistics including bits, hashes, fill ratio, and false positive probability.

## License

This is an educational project. Feel free to use, modify, and learn from it!
