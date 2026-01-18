# LSM Tree in Rust

A simple, educational implementation of a Log-Structured Merge Tree (LSM Tree) in Rust with Write-Ahead Log for crash recovery.

## What is an LSM Tree?

LSM trees are write-optimized data structures used in databases like LevelDB, RocksDB, Cassandra, and HBase. They achieve high write throughput by:

1. **Batching writes in memory** (MemTable) - O(log n) insertion
2. **Flushing to immutable disk files** (SSTables) when memory fills
3. **Reading newest-to-oldest** - recent data shadows old data

This makes LSM trees ideal for write-heavy workloads like time-series data, logs, and event streams.

## Features

- In-memory MemTable with automatic flush
- Persistent SSTables on disk
- **Write-Ahead Log (WAL)** for crash recovery
- Correct read semantics (newest wins)
- Simple binary format for SSTables
- 600+ lines of educational comments
- Unit tests included

## Quick Start

```bash
# Run the demo
cargo run

# Run tests
cargo test
```

## Usage Example

```rust
use std::path::PathBuf;

fn main() {
    // Create LSM tree with 4MB memtable threshold
    let mut lsm = LSMTree::new(
        PathBuf::from("./data"),
        4 * 1024 * 1024
    ).expect("Failed to create LSM tree");
    
    // Insert key-value pairs (now returns Result)
    lsm.put(b"user:123".to_vec(), b"Alice".to_vec())
        .expect("Failed to put");
    lsm.put(b"user:456".to_vec(), b"Bob".to_vec())
        .expect("Failed to put");
    
    // Retrieve values
    if let Some(value) = lsm.get(b"user:123") {
        println!("Found: {}", String::from_utf8_lossy(&value));
    }
    
    // Updates override old values
    lsm.put(b"user:123".to_vec(), b"Alice Smith".to_vec())
        .expect("Failed to update");
}
```

## Architecture

```
┌─────────────┐
│   WAL.log   │  ← All writes logged here FIRST (durability)
│   (Disk)    │
└──────┬──────┘
       │ Write to WAL before memtable
       ↓
┌─────────────┐
│  MemTable   │  ← Writes go here second (BTreeMap)
│  (Memory)   │
└──────┬──────┘
       │ Flush when full
       ↓
┌─────────────┐
│  SSTable 0  │  ← Newest data
├─────────────┤
│  SSTable 1  │
├─────────────┤
│  SSTable 2  │  ← Oldest data
└─────────────┘
     (Disk)
```

**Write Path:** WAL → Memory → Disk (when threshold exceeded)  
**Read Path:** Memory → SSTable 0 → SSTable 1 → ... (until found)  
**Recovery Path:** WAL → Replay to MemTable (on startup)

## File Formats

### SSTable Format
```
[key_len: u32][key: bytes][value_len: u32][value: bytes]...
```

### WAL Format
```
[op_type: u8][key_len: u32][key: bytes][value_len: u32][value: bytes]...
```

Each entry is self-contained with length prefixes for easy parsing.

## Write-Ahead Log (WAL)

The WAL ensures durability across crashes:

1. **Write Order**: Every operation is written to WAL first, then to memtable
2. **Durability**: WAL is fsynced to disk immediately (survives crashes)
3. **Recovery**: On startup, WAL is replayed to reconstruct lost memtable data
4. **Cleanup**: WAL is cleared after successful flush to SSTable

### WAL Example

```rust
// Crash scenario simulation
{
    let mut lsm = LSMTree::new("./data".into(), 4 * 1024 * 1024)?;
    lsm.put(b"key".to_vec(), b"value".to_vec())?;
    // Power loss here! Data only in WAL, not flushed to SSTable
}

// Recovery on restart
let lsm = LSMTree::new("./data".into(), 4 * 1024 * 1024)?;
// WAL automatically replayed - data recovered!
assert_eq!(lsm.get(b"key"), Some(b"value".to_vec()));
```

## Project Structure

```
lsm_tree/
├── src/
│   ├── main.rs      ← LSM Tree implementation + main()
│   └── wal.rs       ← Write-Ahead Log implementation
├── lsm_data/        ← Created at runtime
│   ├── wal.log      ← Write-Ahead Log file
│   ├── sstable_0.db ← SSTable files
│   ├── sstable_1.db
│   └── ...
├── Cargo.toml
└── README.md
```

## What's Missing (For Production)

This is an educational implementation. Real LSM trees add:

- **Bloom filters** - Skip SSTables that definitely don't have a key
- **Compaction** - Merge old SSTables to reclaim space
- ~~**WAL (Write-Ahead Log)**~~ - ✅ **Implemented!** Crash recovery for memtable
- **Sparse indexes** - Jump to key ranges without full scan
- **Compression** - Reduce disk usage (Snappy, LZ4)
- **Multiple levels** - Tiered storage for better read performance

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Write | O(log n) | WAL write + memtable insert |
| Read (hit memtable) | O(log n) | Best case |
| Read (hit SSTable) | O(k × m) | k = #SSTables, m = entries per file |
| Recovery | O(n) | n = WAL entries (typically small) |
| Space | O(n) | Duplicate keys across SSTables |

## Testing

```bash
# Run all tests
cargo test

# Run specific module tests
cargo test --lib wal

# Run with output
cargo test -- --nocapture

# Test WAL recovery specifically
cargo test test_wal_recovery
```

### Test Coverage

- **main.rs**: 4 tests (basic operations, updates, flush, WAL recovery)
- **wal.rs**: 5 tests (append/recover, clear, empty, order, write after clear)
