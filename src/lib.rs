//! # LSM Tree Library
//!
//! A Log-Structured Merge Tree implementation in Rust with:
//! - Write-Ahead Log (WAL) for crash recovery
//! - Bloom filters for fast negative lookups
//!
//! ## Example
//!
//! ```rust,no_run
//! use lsm_tree::LSMTree;
//! use std::path::PathBuf;
//!
//! let mut lsm = LSMTree::new(PathBuf::from("./data"), 4 * 1024 * 1024).unwrap();
//! lsm.put(b"key".to_vec(), b"value".to_vec()).unwrap();
//! let value = lsm.get(b"key");
//! ```

pub mod bloom_filter;
pub mod wal;

// Re-export key types for public API
pub use bloom_filter::BloomFilterStats;

use bloom_filter::BloomFilter;
use wal::{WAL, WALOp};

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

/// Default false positive probability for Bloom filters (1%)
const DEFAULT_BLOOM_FILTER_FPP: f64 = 0.01;

/// Log-Structured Merge Tree (LSM Tree) implementation
///
/// An LSM tree is a write-optimized data structure that provides efficient
/// writes by batching them in memory (MemTable) and periodically flushing
/// to disk as immutable sorted files (SSTables). Reads check memory first,
/// then search through SSTables from newest to oldest.
pub struct LSMTree {
    /// In-memory write buffer using a BTreeMap for sorted key-value storage
    memtable: BTreeMap<Vec<u8>, Vec<u8>>,

    /// Maximum size in bytes before memtable flushes to disk
    memtable_size_threshold: usize,

    /// Current approximate size of memtable in bytes
    memtable_size: usize,

    /// Ordered list of SSTable file paths, newest first
    sstables: Vec<PathBuf>,

    /// Directory path where SSTable files are stored
    data_dir: PathBuf,

    /// Counter for generating unique SSTable filenames
    sstable_counter: usize,

    /// Write-Ahead Log for crash recovery and durability
    wal: WAL,

    /// Bloom filters for each SSTable (indexed same as sstables vector)
    bloom_filters: Vec<BloomFilter>,

    /// Target false positive rate for Bloom filters
    bloom_filter_fpp: f64,

    /// Statistics: number of Bloom filter checks that returned "definitely not"
    bloom_filter_negatives: usize,

    /// Statistics: number of Bloom filter checks that returned "maybe yes"
    bloom_filter_positives: usize,
}

impl LSMTree {
    /// Creates a new LSM tree with specified configuration
    pub fn new(data_dir: PathBuf, memtable_size_threshold: usize) -> std::io::Result<Self> {
        Self::with_bloom_filter_fpp(data_dir, memtable_size_threshold, DEFAULT_BLOOM_FILTER_FPP)
    }

    /// Creates a new LSM tree with custom Bloom filter false positive probability
    pub fn with_bloom_filter_fpp(
        data_dir: PathBuf,
        memtable_size_threshold: usize,
        bloom_filter_fpp: f64,
    ) -> std::io::Result<Self> {
        std::fs::create_dir_all(&data_dir).expect("Failed to create data directory");

        let wal_path = data_dir.join("wal.log");
        let wal = WAL::new(wal_path)?;

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

        let (sstables, bloom_filters, sstable_counter) =
            Self::load_existing_sstables(&data_dir, bloom_filter_fpp)?;

        Ok(Self {
            memtable,
            memtable_size_threshold,
            memtable_size,
            sstables,
            data_dir,
            sstable_counter,
            wal,
            bloom_filters,
            bloom_filter_fpp,
            bloom_filter_negatives: 0,
            bloom_filter_positives: 0,
        })
    }

    fn load_existing_sstables(
        data_dir: &PathBuf,
        bloom_filter_fpp: f64,
    ) -> std::io::Result<(Vec<PathBuf>, Vec<BloomFilter>, usize)> {
        let mut sstables = Vec::new();
        let mut bloom_filters = Vec::new();
        let mut max_counter = 0usize;

        if let Ok(entries) = std::fs::read_dir(data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    if filename.starts_with("sstable_") && filename.ends_with(".db") {
                        if let Some(num_str) = filename
                            .strip_prefix("sstable_")
                            .and_then(|s| s.strip_suffix(".db"))
                        {
                            if let Ok(num) = num_str.parse::<usize>() {
                                sstables.push((num, path));
                                max_counter = max_counter.max(num + 1);
                            }
                        }
                    }
                }
            }
        }

        sstables.sort_by(|a, b| b.0.cmp(&a.0));

        let sstable_paths: Vec<PathBuf> = sstables.iter().map(|(_, p)| p.clone()).collect();

        for (_, sstable_path) in &sstables {
            let bloom_path = sstable_path.with_extension("bloom");
            let bloom_filter = if bloom_path.exists() {
                Self::load_bloom_filter(&bloom_path).unwrap_or_else(|| {
                    Self::rebuild_bloom_filter(sstable_path, bloom_filter_fpp)
                        .unwrap_or_else(|| BloomFilter::new(1, bloom_filter_fpp))
                })
            } else {
                Self::rebuild_bloom_filter(sstable_path, bloom_filter_fpp)
                    .unwrap_or_else(|| BloomFilter::new(1, bloom_filter_fpp))
            };
            bloom_filters.push(bloom_filter);
        }

        Ok((sstable_paths, bloom_filters, max_counter))
    }

    fn load_bloom_filter(path: &PathBuf) -> Option<BloomFilter> {
        let file = File::open(path).ok()?;
        let mut reader = BufReader::new(file);
        BloomFilter::read_from(&mut reader).ok()
    }

    fn rebuild_bloom_filter(sstable_path: &PathBuf, fpp: f64) -> Option<BloomFilter> {
        let file = File::open(sstable_path).ok()?;
        let mut reader = BufReader::new(file);

        let mut keys = Vec::new();
        loop {
            let mut key_len_buf = [0u8; 4];
            if reader.read_exact(&mut key_len_buf).is_err() {
                break;
            }
            let key_len = u32::from_le_bytes(key_len_buf) as usize;

            let mut key = vec![0u8; key_len];
            if reader.read_exact(&mut key).is_err() {
                break;
            }
            keys.push(key);

            let mut value_len_buf = [0u8; 4];
            if reader.read_exact(&mut value_len_buf).is_err() {
                break;
            }
            let value_len = u32::from_le_bytes(value_len_buf) as usize;

            let mut value = vec![0u8; value_len];
            if reader.read_exact(&mut value).is_err() {
                break;
            }
        }

        let mut bf = BloomFilter::new(keys.len().max(1), fpp);
        for key in keys {
            bf.insert(&key);
        }

        let bloom_path = sstable_path.with_extension("bloom");
        if let Ok(file) = File::create(&bloom_path) {
            let mut writer = BufWriter::new(file);
            let _ = bf.write_to(&mut writer);
            let _ = writer.flush();
        }

        Some(bf)
    }

    /// Inserts or updates a key-value pair
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> std::io::Result<()> {
        self.wal.append_put(&key, &value)?;

        let size_delta = key.len() + value.len();

        if let Some(old_value) = self.memtable.get(&key) {
            self.memtable_size -= key.len() + old_value.len();
        }

        self.memtable.insert(key, value);
        self.memtable_size += size_delta;

        if self.memtable_size >= self.memtable_size_threshold {
            self.flush()?;
        }

        Ok(())
    }

    /// Retrieves value for a given key
    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(value) = self.memtable.get(key) {
            return Some(value.clone());
        }

        for (i, sstable_path) in self.sstables.iter().enumerate() {
            if i < self.bloom_filters.len() {
                if !self.bloom_filters[i].might_contain(key) {
                    self.bloom_filter_negatives += 1;
                    continue;
                }
                self.bloom_filter_positives += 1;
            }

            if let Some(value) = self.read_from_sstable(sstable_path, key) {
                return Some(value);
            }
        }

        None
    }

    /// Non-mutable version of get
    pub fn get_immut(&self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(value) = self.memtable.get(key) {
            return Some(value.clone());
        }

        for (i, sstable_path) in self.sstables.iter().enumerate() {
            if i < self.bloom_filters.len() && !self.bloom_filters[i].might_contain(key) {
                continue;
            }
            if let Some(value) = self.read_from_sstable(sstable_path, key) {
                return Some(value);
            }
        }

        None
    }

    /// Flushes memtable to disk as a new SSTable with Bloom filter
    pub fn flush(&mut self) -> std::io::Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        let sstable_path = self
            .data_dir
            .join(format!("sstable_{}.db", self.sstable_counter));
        self.sstable_counter += 1;

        let mut bloom_filter = BloomFilter::new(self.memtable.len(), self.bloom_filter_fpp);

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&sstable_path)?;
        let mut writer = BufWriter::new(file);

        for (key, value) in &self.memtable {
            bloom_filter.insert(key);
            writer.write_all(&(key.len() as u32).to_le_bytes())?;
            writer.write_all(key)?;
            writer.write_all(&(value.len() as u32).to_le_bytes())?;
            writer.write_all(value)?;
        }

        writer.flush()?;

        let bloom_path = sstable_path.with_extension("bloom");
        let bloom_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&bloom_path)?;
        let mut bloom_writer = BufWriter::new(bloom_file);
        bloom_filter.write_to(&mut bloom_writer)?;
        bloom_writer.flush()?;

        self.sstables.insert(0, sstable_path);
        self.bloom_filters.insert(0, bloom_filter);

        self.memtable.clear();
        self.memtable_size = 0;

        self.wal.clear()?;

        Ok(())
    }

    fn read_from_sstable(&self, path: &PathBuf, key: &[u8]) -> Option<Vec<u8>> {
        let file = File::open(path).ok()?;
        let mut reader = BufReader::new(file);

        loop {
            let mut key_len_buf = [0u8; 4];
            if reader.read_exact(&mut key_len_buf).is_err() {
                break;
            }
            let key_len = u32::from_le_bytes(key_len_buf) as usize;

            let mut key_buf = vec![0u8; key_len];
            if reader.read_exact(&mut key_buf).is_err() {
                break;
            }

            let mut value_len_buf = [0u8; 4];
            if reader.read_exact(&mut value_len_buf).is_err() {
                break;
            }
            let value_len = u32::from_le_bytes(value_len_buf) as usize;

            let mut value_buf = vec![0u8; value_len];
            if reader.read_exact(&mut value_buf).is_err() {
                break;
            }

            if key_buf == key {
                return Some(value_buf);
            }
        }

        None
    }

    /// Returns number of entries in memtable
    pub fn len(&self) -> usize {
        self.memtable.len()
    }

    /// Returns true if memtable is empty and no SSTables exist
    pub fn is_empty(&self) -> bool {
        self.memtable.is_empty() && self.sstables.is_empty()
    }

    /// Returns number of SSTables on disk
    pub fn sstable_count(&self) -> usize {
        self.sstables.len()
    }

    /// Returns current memtable size in bytes
    pub fn memtable_size(&self) -> usize {
        self.memtable_size
    }

    /// Returns memtable size threshold
    pub fn memtable_threshold(&self) -> usize {
        self.memtable_size_threshold
    }

    /// Returns data directory path
    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    /// Returns Bloom filter statistics
    pub fn bloom_filter_stats(&self) -> BloomFilterSummary {
        let individual_stats: Vec<BloomFilterStats> =
            self.bloom_filters.iter().map(|bf| bf.stats()).collect();

        let total_size_bytes: usize = individual_stats.iter().map(|s| s.size_bytes).sum();
        let total_items: usize = individual_stats.iter().map(|s| s.num_items).sum();

        BloomFilterSummary {
            num_filters: self.bloom_filters.len(),
            total_size_bytes,
            total_items,
            checks_negative: self.bloom_filter_negatives,
            checks_positive: self.bloom_filter_positives,
            individual_stats,
        }
    }

    /// Returns number of reads skipped by Bloom filters
    pub fn bloom_filter_skipped_reads(&self) -> usize {
        self.bloom_filter_negatives
    }

    /// Resets Bloom filter statistics
    pub fn reset_bloom_filter_stats(&mut self) {
        self.bloom_filter_negatives = 0;
        self.bloom_filter_positives = 0;
    }

    /// Returns all keys in memtable (for display purposes)
    pub fn memtable_keys(&self) -> Vec<Vec<u8>> {
        self.memtable.keys().cloned().collect()
    }

    /// Returns all key-value pairs in memtable
    pub fn memtable_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.memtable
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Returns SSTable paths
    pub fn sstable_paths(&self) -> &[PathBuf] {
        &self.sstables
    }

    /// Reads all entries from an SSTable (for display)
    pub fn read_sstable_entries(&self, index: usize) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        let path = self.sstables.get(index)?;
        let file = File::open(path).ok()?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut key_len_buf = [0u8; 4];
            if reader.read_exact(&mut key_len_buf).is_err() {
                break;
            }
            let key_len = u32::from_le_bytes(key_len_buf) as usize;

            let mut key = vec![0u8; key_len];
            if reader.read_exact(&mut key).is_err() {
                break;
            }

            let mut value_len_buf = [0u8; 4];
            if reader.read_exact(&mut value_len_buf).is_err() {
                break;
            }
            let value_len = u32::from_le_bytes(value_len_buf) as usize;

            let mut value = vec![0u8; value_len];
            if reader.read_exact(&mut value).is_err() {
                break;
            }

            entries.push((key, value));
        }

        Some(entries)
    }
}

impl Drop for LSMTree {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

/// Summary of Bloom filter effectiveness
#[derive(Debug, Clone)]
pub struct BloomFilterSummary {
    pub num_filters: usize,
    pub total_size_bytes: usize,
    pub total_items: usize,
    pub checks_negative: usize,
    pub checks_positive: usize,
    pub individual_stats: Vec<BloomFilterStats>,
}

impl BloomFilterSummary {
    pub fn skip_rate(&self) -> f64 {
        let total = self.checks_negative + self.checks_positive;
        if total == 0 {
            0.0
        } else {
            self.checks_negative as f64 / total as f64
        }
    }

    pub fn total_checks(&self) -> usize {
        self.checks_negative + self.checks_positive
    }
}

impl std::fmt::Display for BloomFilterSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Bloom Filter Summary:")?;
        writeln!(f, "  Filters: {}", self.num_filters)?;
        writeln!(f, "  Total Size: {} bytes", self.total_size_bytes)?;
        writeln!(f, "  Total Items: {}", self.total_items)?;
        writeln!(
            f,
            "  Checks (skipped/proceeded): {}/{}",
            self.checks_negative, self.checks_positive
        )?;
        writeln!(f, "  Skip Rate: {:.1}%", self.skip_rate() * 100.0)?;
        Ok(())
    }
}

// BloomFilterStats is already imported and used above

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_basic_put_get() {
        let dir = PathBuf::from("./test_lib_basic");
        let mut lsm = LSMTree::new(dir.clone(), 1024).unwrap();

        lsm.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        assert_eq!(lsm.get(b"key1"), Some(b"value1".to_vec()));

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn test_bloom_filter_integration() {
        let dir = PathBuf::from("./test_lib_bloom");
        let mut lsm = LSMTree::new(dir.clone(), 10).unwrap();

        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            lsm.put(key.into_bytes(), value.into_bytes()).unwrap();
        }

        lsm.reset_bloom_filter_stats();

        // Query non-existent keys
        for i in 100..200 {
            let key = format!("nonexistent{}", i);
            let _ = lsm.get(key.as_bytes());
        }

        let stats = lsm.bloom_filter_stats();
        assert!(stats.checks_negative > 0);

        fs::remove_dir_all(dir).ok();
    }
}
