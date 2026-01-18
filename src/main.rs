mod wal;
use wal::{WAL, WALOp};

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

/// Log-Structured Merge Tree (LSM Tree) implementation
///
/// An LSM tree is a write-optimized data structure that provides efficient
/// writes by batching them in memory (MemTable) and periodically flushing
/// to disk as immutable sorted files (SSTables). Reads check memory first,
/// then search through SSTables from newest to oldest.
pub struct LSMTree {
    /// In-memory write buffer using a BTreeMap for sorted key-value storage
    /// Keys and values are stored as byte vectors for flexibility
    memtable: BTreeMap<Vec<u8>, Vec<u8>>,

    /// Maximum size in bytes before memtable flushes to disk
    /// Typical values: 4MB-64MB depending on workload
    memtable_size_threshold: usize,

    /// Current approximate size of memtable in bytes
    /// Tracks sum of key and value lengths for flush decisions
    memtable_size: usize,

    /// Ordered list of SSTable file paths, newest first
    /// This ordering is critical: newer writes override older ones
    sstables: Vec<PathBuf>,

    /// Directory path where SSTable files are stored
    /// Each SSTable is a separate file with sorted key-value pairs
    data_dir: PathBuf,

    /// Counter for generating unique SSTable filenames
    /// Ensures each flush creates a distinct file (e.g., "sstable_0.db")
    sstable_counter: usize,

    /// Write-Ahead Log for crash recovery and durability
    wal: WAL,
}

impl LSMTree {
    /// Creates a new LSM tree with specified configuration
    ///
    /// # Arguments
    /// * `data_dir` - Directory path for storing SSTable files
    /// * `memtable_size_threshold` - Max bytes in memtable before flush (e.g., 4MB)
    ///
    /// # Example
    /// ```
    /// let lsm = LSMTree::new("./data".into(), 4 * 1024 * 1024);
    /// ```
    pub fn new(data_dir: PathBuf, memtable_size_threshold: usize) -> std::io::Result<Self> {
        // Create data directory if it doesn't exist
        // Panics on failure since we can't operate without storage
        std::fs::create_dir_all(&data_dir).expect("Failed to create data directory");

        // Initialize WAL for crash recovery
        let wal_path = data_dir.join("wal.log");
        let wal = WAL::new(wal_path)?;

        // Recover memtable from WAL if exists
        let mut memtable: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut memtable_size: usize = 0;

        let entries = wal.recover()?;
        for entry in entries {
            match entry.op {
                WALOp::Put => {
                    let size = entry.key.len() + entry.value.len();
                    if let Some(old_value) = memtable.get(&entry.key) {
                        memtable_size -= entry.key.len() + old_value.len();
                    }
                    memtable.insert(entry.key, entry.value);
                    memtable_size += size;
                }
                WALOp::Delete => {
                    if let Some(old_value) = memtable.remove(&entry.key) {
                        memtable_size -= entry.key.len() + old_value.len();
                    }
                }
            }
        }

        Ok(Self {
            memtable,
            memtable_size_threshold,
            memtable_size,
            sstables: Vec::new(),
            data_dir,
            sstable_counter: 0,
            wal,
        })
    }

    /// Inserts or updates a key-value pair
    ///
    /// Writes are always fast O(log n) operations that go to memory first.
    /// If memtable exceeds threshold, automatically flushes to disk.
    ///
    /// # Arguments
    /// * `key` - Byte vector key (can represent any data type)
    /// * `value` - Byte vector value
    ///
    /// # Example
    /// ```
    /// lsm.put(b"user:123".to_vec(), b"Alice".to_vec());
    /// ```
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> std::io::Result<()> {
        // Write to WAL first for durability
        self.wal.append_put(&key, &value)?;

        // Calculate size impact: key + value length
        // If key exists, we'll adjust for the old value size later
        let size_delta = key.len() + value.len();

        // Check if key already exists to properly track size
        if let Some(old_value) = self.memtable.get(&key) {
            // Subtract old entry size since we're replacing it
            self.memtable_size -= key.len() + old_value.len();
        }

        // Insert into sorted in-memory structure
        // BTreeMap maintains keys in sorted order automatically
        self.memtable.insert(key, value);

        // Update total memtable size
        self.memtable_size += size_delta;

        // Check if we need to flush to disk
        // This keeps memory usage bounded and ensures durability
        if self.memtable_size >= self.memtable_size_threshold {
            self.flush()?;
        }

        Ok(())
    }

    /// Retrieves value for a given key
    ///
    /// Implements LSM tree read path: check memtable first (O(log n)),
    /// then search SSTables newest-to-oldest until found or exhausted.
    /// Newer writes shadow older ones, so we stop at first match.
    ///
    /// # Arguments
    /// * `key` - Byte vector key to look up
    ///
    /// # Returns
    /// * `Some(Vec<u8>)` - Value if key exists
    /// * `None` - Key not found in tree
    ///
    /// # Example
    /// ```
    /// if let Some(value) = lsm.get(b"user:123") {
    ///     println!("Found: {:?}", value);
    /// }
    /// ```
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Step 1: Check memtable first (fastest path)
        // Recent writes are always here
        if let Some(value) = self.memtable.get(key) {
            return Some(value.clone());
        }

        // Step 2: Search SSTables from newest to oldest
        // SSTables are immutable, so we can safely read them
        // We stop at first match due to "newer wins" semantics
        for sstable_path in &self.sstables {
            if let Some(value) = self.read_from_sstable(sstable_path, key) {
                return Some(value);
            }
        }

        // Key not found anywhere in the tree
        None
    }

    /// Flushes memtable to disk as a new SSTable
    ///
    /// Creates an immutable sorted file containing all memtable entries.
    /// SSTable format: [key_len (4 bytes)][key][value_len (4 bytes)][value]...
    /// This simple format is easy to parse and keeps keys sorted for scanning.
    ///
    /// After flush, memtable is cleared to accept new writes.
    fn flush(&mut self) -> std::io::Result<()> {
        // Don't create empty files
        if self.memtable.is_empty() {
            return Ok(());
        }

        // Generate unique filename using counter
        let sstable_path = self
            .data_dir
            .join(format!("sstable_{}.db", self.sstable_counter));
        self.sstable_counter += 1;

        // Open file for writing with buffering for performance
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&sstable_path)?;
        let mut writer = BufWriter::new(file);

        // Write all entries in sorted order (BTreeMap iterator guarantees this)
        // Each entry: key_length (u32) + key_bytes + value_length (u32) + value_bytes
        for (key, value) in &self.memtable {
            // Write key length as 4-byte unsigned integer (little-endian)
            writer.write_all(&(key.len() as u32).to_le_bytes())?;
            // Write actual key bytes
            writer.write_all(key)?;
            // Write value length as 4-byte unsigned integer
            writer.write_all(&(value.len() as u32).to_le_bytes())?;
            // Write actual value bytes
            writer.write_all(value)?;
        }

        // Ensure all data is written to disk before proceeding
        writer.flush()?;

        // Add new SSTable to front of list (newest first)
        // This ordering is critical for read correctness
        self.sstables.insert(0, sstable_path);

        // Clear memtable now that data is safely on disk
        self.memtable.clear();
        self.memtable_size = 0;

        // Clear WAL since data is now durable in SSTable
        self.wal.clear()?;

        Ok(())
    }

    /// Reads a specific key from an SSTable file
    ///
    /// Performs linear scan through SSTable since we don't have an index.
    /// Production implementations would use bloom filters + sparse indexes
    /// to avoid reading entire files. Returns first matching key found.
    ///
    /// # Arguments
    /// * `path` - Path to SSTable file
    /// * `key` - Key to search for
    ///
    /// # Returns
    /// * `Some(Vec<u8>)` - Value if key found in this SSTable
    /// * `None` - Key not in this SSTable or read error occurred
    fn read_from_sstable(&self, path: &PathBuf, key: &[u8]) -> Option<Vec<u8>> {
        // Open file with buffering for efficient sequential reads
        let file = File::open(path).ok()?;
        let mut reader = BufReader::new(file);

        // Scan through all entries in the file
        loop {
            // Read key length (4 bytes)
            let mut key_len_buf = [0u8; 4];
            if reader.read_exact(&mut key_len_buf).is_err() {
                // End of file or read error
                break;
            }
            let key_len = u32::from_le_bytes(key_len_buf) as usize;

            // Read key bytes
            let mut key_buf = vec![0u8; key_len];
            if reader.read_exact(&mut key_buf).is_err() {
                break;
            }

            // Read value length (4 bytes)
            let mut value_len_buf = [0u8; 4];
            if reader.read_exact(&mut value_len_buf).is_err() {
                break;
            }
            let value_len = u32::from_le_bytes(value_len_buf) as usize;

            // Read value bytes
            let mut value_buf = vec![0u8; value_len];
            if reader.read_exact(&mut value_buf).is_err() {
                break;
            }

            // Check if this is the key we're looking for
            if key_buf == key {
                return Some(value_buf);
            }

            // Not a match, continue to next entry
            // In production, we could use key ordering to early-exit
        }

        None
    }

    /// Returns approximate number of key-value pairs in the tree
    ///
    /// This is only approximate because:
    /// 1. Same key may exist in multiple SSTables (older versions)
    /// 2. We don't scan SSTables to count entries (would be expensive)
    ///
    /// For exact count, would need to compact and deduplicate all data.
    pub fn len(&self) -> usize {
        self.memtable.len()
    }

    /// Checks if tree is empty
    ///
    /// Returns true only if memtable is empty and no SSTables exist.
    /// Note: Tree might still be "logically empty" if all SSTables are empty.
    pub fn is_empty(&self) -> bool {
        self.memtable.is_empty() && self.sstables.is_empty()
    }
}

// Implement Drop to ensure data is flushed on cleanup
// This provides durability guarantees even if user forgets to flush
impl Drop for LSMTree {
    fn drop(&mut self) {
        // Attempt to flush any remaining data in memtable
        // Ignore errors since we can't panic in Drop
        let _ = self.flush();
    }
}

fn main() {
    println!("=== LSM Tree ===\n");

    // Create LSM tree with 100 byte threshold (very small for demo purposes)
    // In production, use 4MB-64MB: 4 * 1024 * 1024
    let mut lsm =
        LSMTree::new(PathBuf::from("./lsm_data"), 100).expect("Failed to create LSM tree");

    // Example 1: Basic key-value operations
    println!("Example 1: Basic Operations");
    println!("---------------------------");

    // Insert some user data
    lsm.put(b"user:1".to_vec(), b"Alice".to_vec())
        .expect("Failed to put user:1");
    lsm.put(b"user:2".to_vec(), b"Bob".to_vec())
        .expect("Failed to put user:2");
    lsm.put(b"user:3".to_vec(), b"Charlie".to_vec())
        .expect("Failed to put user:3");

    // Retrieve and display values
    if let Some(value) = lsm.get(b"user:1") {
        println!("user:1 = {}", String::from_utf8_lossy(&value));
    }

    if let Some(value) = lsm.get(b"user:2") {
        println!("user:2 = {}", String::from_utf8_lossy(&value));
    }

    // Try to get a non-existent key
    match lsm.get(b"user:999") {
        Some(value) => println!("user:999 = {}", String::from_utf8_lossy(&value)),
        None => println!("user:999 = Not found"),
    }

    println!();

    // Example 2: Updates (newer values override older ones)
    println!("Example 2: Updates");
    println!("------------------");

    lsm.put(b"user:1".to_vec(), b"Alice Smith".to_vec())
        .expect("Failed to update user:1");
    if let Some(value) = lsm.get(b"user:1") {
        println!("Updated user:1 = {}", String::from_utf8_lossy(&value));
    }

    println!();

    // Example 3: Trigger flush by inserting many entries
    println!("Example 3: Automatic Flush");
    println!("--------------------------");
    println!("Inserting 20 entries to trigger flush...");

    for i in 0..20 {
        let key = format!("product:{}", i);
        let value = format!("Item {}", i);
        lsm.put(key.into_bytes(), value.into_bytes())
            .expect(&format!("Failed to put product:{}", i));
    }

    println!("Entries inserted. Check ./lsm_data/ directory for SSTable files.");
    println!("Number of entries in memtable: {}", lsm.len());

    println!();

    // Example 4: Reading after flush (data persists)
    println!("Example 4: Reading Persisted Data");
    println!("----------------------------------");

    // These reads will check SSTables on disk
    if let Some(value) = lsm.get(b"product:5") {
        println!("product:5 = {}", String::from_utf8_lossy(&value));
    }

    if let Some(value) = lsm.get(b"product:15") {
        println!("product:15 = {}", String::from_utf8_lossy(&value));
    }

    println!();

    // Example 5: Numeric data (store as bytes)
    println!("Example 5: Storing Numeric Data");
    println!("--------------------------------");

    let score: u64 = 9876543210;
    lsm.put(b"score:player1".to_vec(), score.to_le_bytes().to_vec())
        .expect("Failed to put score");

    if let Some(value) = lsm.get(b"score:player1") {
        // Convert bytes back to u64
        if value.len() == 8 {
            let retrieved_score = u64::from_le_bytes([
                value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
            ]);
            println!("score:player1 = {}", retrieved_score);
        }
    }

    println!();

    // Example 6: Batch operations
    println!("Example 6: Batch Operations");
    println!("----------------------------");

    let users = vec![
        ("user:100", "John"),
        ("user:101", "Jane"),
        ("user:102", "Jim"),
    ];

    for (key, name) in users {
        lsm.put(key.as_bytes().to_vec(), name.as_bytes().to_vec())
            .expect(&format!("Failed to put {}", key));
    }

    println!("Inserted {} users", 3);

    // Retrieve all
    for i in 100..103 {
        let key = format!("user:{}", i);
        if let Some(value) = lsm.get(key.as_bytes()) {
            println!("{} = {}", key, String::from_utf8_lossy(&value));
        }
    }

    println!();
    println!("=== Demo Complete ===");
    println!("\nNote: Data persisted in './lsm_data/' directory");
    println!("LSM tree will auto-flush remaining data when dropped.");

    // When lsm goes out of scope, Drop trait ensures final flush
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_basic_put_get() {
        let dir = PathBuf::from("./test_lsm_basic");
        let mut lsm = LSMTree::new(dir.clone(), 1024).unwrap();

        lsm.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        assert_eq!(lsm.get(b"key1"), Some(b"value1".to_vec()));

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn test_update() {
        let dir = PathBuf::from("./test_lsm_update");
        let mut lsm = LSMTree::new(dir.clone(), 1024).unwrap();

        lsm.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        lsm.put(b"key1".to_vec(), b"value2".to_vec()).unwrap();
        assert_eq!(lsm.get(b"key1"), Some(b"value2".to_vec()));

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn test_flush() {
        let dir = PathBuf::from("./test_lsm_flush");
        let mut lsm = LSMTree::new(dir.clone(), 10).unwrap();

        lsm.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        lsm.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();

        assert_eq!(lsm.get(b"key1"), Some(b"value1".to_vec()));

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn test_wal_recovery() {
        let dir = PathBuf::from("./test_lsm_wal_recovery");

        // Write data without flush
        {
            let mut lsm = LSMTree::new(dir.clone(), 10000).unwrap();
            lsm.put(b"recover_key".to_vec(), b"recover_value".to_vec())
                .unwrap();

            // Prevent automatic flush by using forget
            std::mem::forget(lsm);
        }

        // Recover from WAL
        let lsm = LSMTree::new(dir.clone(), 10000).unwrap();
        assert_eq!(lsm.get(b"recover_key"), Some(b"recover_value".to_vec()));

        fs::remove_dir_all(dir).ok();
    }
}
