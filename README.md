# LSM Tree in Rust

A simple, educational implementation of a Log-Structured Merge Tree (LSM Tree) in Rust. 

## What is an LSM Tree?

LSM trees are write-optimized data structures used in databases like LevelDB, RocksDB, Cassandra, and HBase. They achieve high write throughput by:

1. **Batching writes in memory** (MemTable) - O(log n) insertion
2. **Flushing to immutable disk files** (SSTables) when memory fills
3. **Reading newest-to-oldest** - recent data shadows old data

This makes LSM trees ideal for write-heavy workloads like time-series data, logs, and event streams.

## Features

- In-memory MemTable with automatic flush
- Persistent SSTables on disk
- Correct read semantics (newest wins)
- Simple binary format for SSTables
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
    );
    
    // Insert key-value pairs
    lsm.put(b"user:123".to_vec(), b"Alice".to_vec());
    lsm.put(b"user:456".to_vec(), b"Bob".to_vec());
    
    // Retrieve values
    if let Some(value) = lsm.get(b"user:123") {
        println!("Found: {}", String::from_utf8_lossy(&value));
    }
    
    // Updates override old values
    lsm.put(b"user:123".to_vec(), b"Alice Smith".to_vec());
}
```

## Architecture

```
┌─────────────┐
│  MemTable   │  ← Writes go here first (BTreeMap)
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

**Write Path:** Memory → Disk (when threshold exceeded)  
**Read Path:** Memory → SSTable 0 → SSTable 1 → ... (until found)

## File Format

SSTables use a simple binary format:

```
[key_len: u32][key: bytes][value_len: u32][value: bytes]...
```

Each entry is self-contained with length prefixes for easy parsing.

## What's Missing (For Production)

This is an educational implementation. Real LSM trees add:

- **Bloom filters** - Skip SSTables that definitely don't have a key
- **Compaction** - Merge old SSTables to reclaim space
- **WAL (Write-Ahead Log)** - Crash recovery for memtable
- **Sparse indexes** - Jump to key ranges without full scan
- **Compression** - Reduce disk usage (Snappy, LZ4)
- **Multiple levels** - Tiered storage for better read performance

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Write | O(log n) | Always to memtable |
| Read (hit memtable) | O(log n) | Best case |
| Read (hit SSTable) | O(k × m) | k = #SSTables, m = entries per file |
| Space | O(n) | Duplicate keys across SSTables |
