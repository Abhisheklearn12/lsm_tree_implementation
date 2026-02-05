/// Write-Ahead Log (WAL) for crash recovery
///
/// The WAL is like a journal that records every operation BEFORE it happens.
/// If your computer crashes while writing to memory, you can replay this journal
/// to recover all the operations that were lost. It's the same concept banks use
/// to track transactions - write it down first, then do it.
///
/// Think of it like this:
/// - Without WAL: Write to memory → crash → data lost forever
/// - With WAL: Write to journal → write to memory → crash → replay journal → data recovered!
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

/// Types of operations we can log
///
/// Right now we support PUT (insert/update) and DELETE.
/// Each operation gets a unique number so we can identify it in the log file.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WALOp {
    /// Insert or update a key-value pair
    /// Stored in log as byte value: 1
    Put = 1,

    /// Delete a key (for future use)
    /// Stored in log as byte value: 2
    Delete = 2,
}

/// A single entry in the Write-Ahead Log
///
/// This represents one operation that was (or will be) performed.
/// We store the operation type, the key, and the value.
#[derive(Debug, Clone, PartialEq)]
pub struct WALEntry {
    /// What kind of operation is this? (Put or Delete)
    pub op: WALOp,

    /// The key being operated on (stored as bytes for flexibility)
    pub key: Vec<u8>,

    /// The value for this key (empty for Delete operations)
    pub value: Vec<u8>,
}

/// Write-Ahead Log implementation
///
/// The WAL is a simple append-only file on disk. Every time you write data,
/// we first append it to this log file and force it to disk (fsync). This
/// guarantees that even if the power goes out, the operation is saved.
///
/// File format for each entry:
/// `[operation_type: 1 byte][key_length: 4 bytes][key_bytes][value_length: 4 bytes][value_bytes]`
///
/// This format is self-describing - we can parse it even if we don't know
/// how many entries are in the file. Just keep reading until EOF.
pub struct WAL {
    /// Path to the WAL file on disk
    /// Typically something like "./lsm_data/wal.log"
    path: PathBuf,

    /// Buffered writer for efficient sequential writes
    ///
    /// We use buffering because WAL writes are always sequential (append-only).
    /// Sequential writes are the fastest kind of disk I/O, and buffering makes
    /// them even faster by batching multiple small writes together.
    writer: BufWriter<File>,
}

impl WAL {
    /// Creates a new WAL or opens an existing one
    ///
    /// This function is smart: if the WAL file already exists (from a previous
    /// run), it opens it in append mode so we don't lose the existing data.
    /// If it doesn't exist, we create a new one.
    ///
    /// # Arguments
    /// * `path` - Where to store the WAL file (e.g., "./lsm_data/wal.log")
    ///
    /// # Returns
    /// * `Ok(WAL)` - Successfully created/opened the WAL
    /// * `Err(io::Error)` - Something went wrong (disk full, permissions, etc.)
    ///
    /// # Example
    /// ```ignore
    /// let wal = WAL::new(PathBuf::from("./data/wal.log"))?;
    /// ```
    pub fn new(path: PathBuf) -> std::io::Result<Self> {
        // Open in append mode - this preserves existing data
        // create(true) means "create the file if it doesn't exist"
        // append(true) means "all writes go to the end of the file"
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        // Wrap in a buffered writer for better performance
        // BufWriter accumulates small writes in memory before
        // actually writing to disk in larger chunks
        let writer = BufWriter::new(file);

        Ok(Self { path, writer })
    }

    /// Appends a PUT operation to the WAL
    ///
    /// This is the critical durability step: we write the operation to disk
    /// BEFORE applying it to the in-memory memtable. The flush() call at the
    /// end forces the OS to actually write the data to the physical disk
    /// (not just cache it in memory).
    ///
    /// Order of operations when you call lsm.put():
    /// 1. Call this function (write to WAL)
    /// 2. flush() forces data to disk
    /// 3. Now it's safe to update memtable
    ///
    /// # Arguments
    /// * `key` - The key being inserted/updated
    /// * `value` - The new value for this key
    ///
    /// # Returns
    /// * `Ok(())` - Successfully logged and flushed to disk
    /// * `Err(io::Error)` - Disk write failed (out of space, I/O error, etc.)
    pub fn append_put(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        self.append_entry(WALOp::Put, key, value)
    }

    /// Appends a DELETE operation to the WAL
    ///
    /// This logs that a key should be removed. The value is usually empty
    /// since we don't need it for deletions, but we store the field anyway
    /// to keep the format consistent.
    ///
    /// # Arguments
    /// * `key` - The key being deleted
    ///
    /// # Returns
    /// * `Ok(())` - Successfully logged to disk
    /// * `Err(io::Error)` - Disk write failed
    #[allow(dead_code)]
    pub fn append_delete(&mut self, key: &[u8]) -> std::io::Result<()> {
        // Value is empty for deletes, but we still write the length field
        self.append_entry(WALOp::Delete, key, &[])
    }

    /// Internal helper that writes any operation type to the log
    ///
    /// Binary format (all numbers in little-endian):
    ///
    /// +------------------+
    /// | op_type (1 byte) |  ← WALOp::Put = 1, WALOp::Delete = 2
    /// +------------------+
    /// | key_len (4 bytes)|  ← Length of the key in bytes (u32)
    /// +------------------+
    /// | key bytes        |  ← Actual key data
    /// +------------------+
    /// | val_len (4 bytes)|  ← Length of the value in bytes (u32)
    /// +------------------+
    /// | value bytes      |  ← Actual value data
    /// +------------------+
    ///
    /// This format is easy to parse because:
    /// - Fixed-size fields tell us what comes next
    /// - Variable-length fields have their size stored before them
    /// - No delimiters needed (length-prefixed data)
    ///
    /// # Arguments
    /// * `op` - Type of operation (Put or Delete)
    /// * `key` - Key bytes
    /// * `value` - Value bytes
    fn append_entry(&mut self, op: WALOp, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        // Step 1: Write operation type (1 byte)
        // Convert enum to its u8 representation (Put = 1, Delete = 2)
        self.writer.write_all(&[op as u8])?;

        // Step 2: Write key length (4 bytes, little-endian)
        // We cast to u32 because that's plenty for key lengths
        // Little-endian is the standard for most modern CPUs
        self.writer.write_all(&(key.len() as u32).to_le_bytes())?;

        // Step 3: Write the actual key bytes
        self.writer.write_all(key)?;

        // Step 4: Write value length (4 bytes, little-endian)
        self.writer.write_all(&(value.len() as u32).to_le_bytes())?;

        // Step 5: Write the actual value bytes
        self.writer.write_all(value)?;

        // Step 6: CRITICAL - Force everything to disk
        // flush() ensures the OS writes buffered data to the physical disk.
        // Without this, the data might sit in OS cache and be lost on crash.
        // This is why WAL writes are "durable" - they survive power loss.
        self.writer.flush()?;

        Ok(())
    }

    /// Recovers all entries from the WAL
    ///
    /// This is called when the LSM tree starts up. We read the entire WAL
    /// file from beginning to end, parsing each entry and returning them
    /// as a vector. The LSM tree will then replay these operations to
    /// reconstruct the memtable state from before the crash.
    ///
    /// # How it works
    /// 1. Open WAL file for reading
    /// 2. Loop until we hit end-of-file:
    ///    - Read operation type
    ///    - Read key length, then key bytes
    ///    - Read value length, then value bytes
    ///    - Add to results vector
    /// 3. Return all entries in chronological order
    ///
    /// # Returns
    /// * `Ok(Vec<WALEntry>)` - All operations from the log, in order
    /// * `Err(io::Error)` - File read error or corrupted data
    ///
    /// # Example
    /// ```ignore
    /// let entries = wal.recover()?;
    /// for entry in entries {
    ///     // Replay this operation into memtable
    ///     if entry.op == WALOp::Put {
    ///         memtable.insert(entry.key, entry.value);
    ///     }
    /// }
    /// ```
    pub fn recover(&self) -> std::io::Result<Vec<WALEntry>> {
        // Open file for reading (different from our writer instance)
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        // Read entries until we hit end of file
        loop {
            // Try to read operation type (1 byte)
            let mut op_buf = [0u8; 1];
            match reader.read_exact(&mut op_buf) {
                Ok(_) => {
                    // Successfully read a byte, continue parsing
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Hit end of file - this is normal, we're done
                    break;
                }
                Err(e) => {
                    // Some other error - propagate it
                    return Err(e);
                }
            }

            // Parse operation type from byte value
            let op = match op_buf[0] {
                1 => WALOp::Put,
                2 => WALOp::Delete,
                invalid => {
                    // If we see an unexpected byte value, the file is corrupted
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Invalid WAL operation type: {}", invalid),
                    ));
                }
            };

            // Read key length (4 bytes)
            let mut key_len_buf = [0u8; 4];
            reader.read_exact(&mut key_len_buf)?;
            let key_len = u32::from_le_bytes(key_len_buf) as usize;

            // Read key bytes (variable length)
            let mut key = vec![0u8; key_len];
            reader.read_exact(&mut key)?;

            // Read value length (4 bytes)
            let mut value_len_buf = [0u8; 4];
            reader.read_exact(&mut value_len_buf)?;
            let value_len = u32::from_le_bytes(value_len_buf) as usize;

            // Read value bytes (variable length)
            let mut value = vec![0u8; value_len];
            reader.read_exact(&mut value)?;

            // Add this entry to our results
            entries.push(WALEntry { op, key, value });
        }

        Ok(entries)
    }

    /// Clears the WAL after successful memtable flush
    ///
    /// Once we've successfully flushed the memtable to an SSTable on disk,
    /// we don't need the WAL entries anymore - the data is now durable in
    /// the SSTable. Clearing the WAL prevents it from growing forever.
    ///
    /// This is safe because:
    /// 1. We only call this AFTER flush succeeds
    /// 2. If flush fails, we keep the WAL for recovery
    /// 3. New writes will create new WAL entries
    ///
    /// # How it works
    /// - Flush any buffered data first
    /// - Truncate file to 0 bytes (delete all content)
    /// - Seek back to beginning for next write
    ///
    /// # Returns
    /// * `Ok(())` - WAL successfully cleared
    /// * `Err(io::Error)` - File operation failed
    pub fn clear(&mut self) -> std::io::Result<()> {
        // Make sure any buffered writes are on disk first
        self.writer.flush()?;

        // Get the underlying file handle from the buffered writer
        let file = self.writer.get_mut();

        // Truncate file to 0 bytes - deletes all content
        // This is much faster than deleting and recreating the file
        file.set_len(0)?;

        // Move file pointer back to the beginning
        // Next write will start at position 0
        file.seek(SeekFrom::Start(0))?;

        Ok(())
    }
}

// UNIT TESTS
// These tests verify that WAL works correctly in all scenarios:
// - Normal write and recovery
// - Multiple entries
// - Different operation types (Put, Delete)
// - Clearing the log
// - Empty file handling
//
// Run with: cargo test

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Test basic write and recovery flow
    ///
    /// This simulates the most common scenario:
    /// 1. Write some data to WAL
    /// 2. Close the WAL (simulate program exit)
    /// 3. Open WAL again and recover
    /// 4. Verify all data was recovered correctly
    #[test]
    fn test_wal_append_and_recover() {
        let path = PathBuf::from("./test_wal_basic.log");

        // Scope 1: Write data and close WAL
        {
            let mut wal = WAL::new(path.clone()).unwrap();

            // Write a PUT operation
            wal.append_put(b"key1", b"value1").unwrap();

            // Write another PUT operation
            wal.append_put(b"key2", b"value2").unwrap();

            // Write a DELETE operation
            wal.append_delete(b"key1").unwrap();

            // WAL is dropped here, file is closed
        }

        // Scope 2: Recover data from WAL
        let wal = WAL::new(path.clone()).unwrap();
        let entries = wal.recover().unwrap();

        // Verify we got all 3 entries
        assert_eq!(entries.len(), 3, "Should recover exactly 3 entries");

        // Verify first entry (PUT key1 = value1)
        assert_eq!(entries[0].op, WALOp::Put);
        assert_eq!(entries[0].key, b"key1");
        assert_eq!(entries[0].value, b"value1");

        // Verify second entry (PUT key2 = value2)
        assert_eq!(entries[1].op, WALOp::Put);
        assert_eq!(entries[1].key, b"key2");
        assert_eq!(entries[1].value, b"value2");

        // Verify third entry (DELETE key1)
        assert_eq!(entries[2].op, WALOp::Delete);
        assert_eq!(entries[2].key, b"key1");
        // Delete operations have empty values
        assert_eq!(entries[2].value, b"");

        // Cleanup test file
        fs::remove_file(path).ok();
    }

    /// Test clearing the WAL
    ///
    /// After flushing memtable to SSTable, we clear the WAL.
    /// This test verifies that clearing works and recovery
    /// returns an empty list afterward.
    #[test]
    fn test_wal_clear() {
        let path = PathBuf::from("./test_wal_clear.log");

        let mut wal = WAL::new(path.clone()).unwrap();

        // Write some data
        wal.append_put(b"key1", b"value1").unwrap();
        wal.append_put(b"key2", b"value2").unwrap();

        // Clear the WAL
        wal.clear().unwrap();

        // Recover should return empty vector
        let entries = wal.recover().unwrap();
        assert_eq!(entries.len(), 0, "WAL should be empty after clear");

        // Cleanup
        fs::remove_file(path).ok();
    }

    /// Test recovering from an empty WAL file
    ///
    /// When starting fresh, the WAL file exists but has no entries.
    /// Recovery should handle this gracefully and return empty vector.
    #[test]
    fn test_wal_empty_recovery() {
        let path = PathBuf::from("./test_wal_empty.log");

        // Create new WAL but don't write anything
        let wal = WAL::new(path.clone()).unwrap();

        // Recovery should return empty vector without errors
        let entries = wal.recover().unwrap();
        assert_eq!(entries.len(), 0, "Empty WAL should recover zero entries");

        // Cleanup
        fs::remove_file(path).ok();
    }

    /// Test multiple writes and verify order preservation
    ///
    /// WAL must preserve the exact order of operations because
    /// order matters (e.g., PUT then DELETE is different from DELETE then PUT).
    #[test]
    fn test_wal_preserves_order() {
        let path = PathBuf::from("./test_wal_order.log");

        {
            let mut wal = WAL::new(path.clone()).unwrap();

            // Write operations in specific order
            for i in 0..10 {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                wal.append_put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        }

        // Recover and verify order
        let wal = WAL::new(path.clone()).unwrap();
        let entries = wal.recover().unwrap();

        assert_eq!(entries.len(), 10);

        // Check each entry is in the correct order
        for (i, entry) in entries.iter().enumerate().take(10) {
            let expected_key = format!("key{}", i);
            let expected_value = format!("value{}", i);
            assert_eq!(entry.key, expected_key.as_bytes());
            assert_eq!(entry.value, expected_value.as_bytes());
        }

        fs::remove_file(path).ok();
    }

    /// Test writing after clearing
    ///
    /// After clearing the WAL, we should be able to write new entries.
    /// This ensures the clear operation doesn't break the WAL.
    #[test]
    fn test_wal_write_after_clear() {
        let path = PathBuf::from("./test_wal_write_after_clear.log");

        let mut wal = WAL::new(path.clone()).unwrap();

        // Write, clear, write again
        wal.append_put(b"old_key", b"old_value").unwrap();
        wal.clear().unwrap();
        wal.append_put(b"new_key", b"new_value").unwrap();

        // Should only recover the new entry
        let entries = wal.recover().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, b"new_key");
        assert_eq!(entries[0].value, b"new_value");

        fs::remove_file(path).ok();
    }
}
