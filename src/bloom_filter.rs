/// Bloom Filter Implementation
///
/// A Bloom filter is a space-efficient probabilistic data structure that tells you
/// whether an element is POSSIBLY in a set, or DEFINITELY NOT in the set.
///
/// Key properties:
/// - False positives are possible (says "maybe yes" when actually "no")
/// - False negatives are IMPOSSIBLE (never says "no" when actually "yes")
/// - Space efficient: uses ~1.44 bits per element for 10% false positive rate
/// - Fast: O(k) operations where k is number of hash functions
///
/// For LSM trees, Bloom filters are perfect because:
/// - We can skip reading entire SSTable files for keys that definitely don't exist
/// - False positives only cost an extra file read (acceptable trade-off)
/// - False negatives would cause data loss (but they never happen!)
///
/// Example:
/// ```ignore
/// let mut bf = BloomFilter::new(1000, 0.01); // 1000 items, 1% false positive rate
/// bf.insert(b"user:123");
/// assert!(bf.might_contain(b"user:123"));   // true (definitely or possibly)
/// assert!(!bf.might_contain(b"user:999"));  // false (definitely not)
/// ```

use std::io::{Read, Write};

/// A Bloom filter for efficient set membership testing
///
/// Uses multiple hash functions to map keys to positions in a bit array.
/// When inserting, all positions are set to 1.
/// When querying, if ALL positions are 1, the key MIGHT exist.
/// If ANY position is 0, the key DEFINITELY doesn't exist.
#[derive(Clone)]
pub struct BloomFilter {
    /// Bit array stored as bytes (8 bits per byte)
    /// We use a Vec<u8> instead of a proper bit vector for simplicity
    bits: Vec<u8>,

    /// Number of bits in the filter (bits.len() * 8)
    num_bits: usize,

    /// Number of hash functions to use
    /// More hashes = lower false positive rate, but slower operations
    num_hashes: usize,

    /// Number of items inserted (for statistics)
    num_items: usize,
}

impl BloomFilter {
    /// Creates a new Bloom filter optimized for the expected number of items
    /// and desired false positive probability.
    ///
    /// # Arguments
    /// * `expected_items` - How many items you expect to insert
    /// * `false_positive_rate` - Desired probability of false positives (e.g., 0.01 for 1%)
    ///
    /// # Optimal Parameters
    /// The optimal number of bits (m) and hash functions (k) are:
    /// - m = -n * ln(p) / (ln(2)^2)  where n=items, p=false_positive_rate
    /// - k = (m/n) * ln(2)
    ///
    /// # Example
    /// ```ignore
    /// // For 1000 items with 1% false positive rate
    /// let bf = BloomFilter::new(1000, 0.01);
    /// ```
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        // Ensure reasonable parameters
        let expected_items = expected_items.max(1);
        let false_positive_rate = false_positive_rate.clamp(0.0001, 0.5);

        // Calculate optimal number of bits using formula:
        // m = -n * ln(p) / (ln(2)^2)
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let num_bits_f64 =
            -(expected_items as f64) * false_positive_rate.ln() / ln2_squared;
        let num_bits = (num_bits_f64.ceil() as usize).max(8); // Minimum 8 bits

        // Calculate optimal number of hash functions:
        // k = (m/n) * ln(2)
        let num_hashes_f64 = (num_bits as f64 / expected_items as f64) * std::f64::consts::LN_2;
        let num_hashes = (num_hashes_f64.ceil() as usize).clamp(1, 16); // Between 1 and 16

        // Allocate bit array (round up to nearest byte)
        let num_bytes = (num_bits + 7) / 8;
        let bits = vec![0u8; num_bytes];

        Self {
            bits,
            num_bits,
            num_hashes,
            num_items: 0,
        }
    }

    /// Creates a Bloom filter with explicit parameters
    ///
    /// Use this when you need precise control over the filter size
    /// (e.g., when deserializing from disk).
    ///
    /// # Arguments
    /// * `num_bits` - Total number of bits in the filter
    /// * `num_hashes` - Number of hash functions to use
    pub fn with_params(num_bits: usize, num_hashes: usize) -> Self {
        let num_bytes = (num_bits + 7) / 8;
        let bits = vec![0u8; num_bytes];

        Self {
            bits,
            num_bits: num_bits.max(8),
            num_hashes: num_hashes.clamp(1, 16),
            num_items: 0,
        }
    }

    /// Inserts a key into the Bloom filter
    ///
    /// This sets k bits in the bit array, where k is the number of hash functions.
    /// After insertion, `might_contain(key)` will always return true.
    ///
    /// # Arguments
    /// * `key` - The key to insert (as bytes)
    ///
    /// # Time Complexity
    /// O(k) where k is the number of hash functions
    pub fn insert(&mut self, key: &[u8]) {
        // Generate k hash values and set corresponding bits
        for i in 0..self.num_hashes {
            let bit_index = self.hash(key, i);
            self.set_bit(bit_index);
        }
        self.num_items += 1;
    }

    /// Checks if a key might be in the set
    ///
    /// Returns:
    /// - `true` if the key MIGHT be in the set (could be a false positive)
    /// - `false` if the key is DEFINITELY NOT in the set (never wrong)
    ///
    /// # Arguments
    /// * `key` - The key to check (as bytes)
    ///
    /// # Time Complexity
    /// O(k) where k is the number of hash functions
    ///
    /// # Example
    /// ```ignore
    /// if !bf.might_contain(b"user:123") {
    ///     // Key definitely not in this SSTable - skip reading file!
    ///     return None;
    /// }
    /// // Key might be here, need to actually read the file
    /// ```
    pub fn might_contain(&self, key: &[u8]) -> bool {
        // Check all k hash positions - ALL must be set
        for i in 0..self.num_hashes {
            let bit_index = self.hash(key, i);
            if !self.get_bit(bit_index) {
                return false; // Definitely not in set
            }
        }
        true // Possibly in set (might be false positive)
    }

    /// Computes the i-th hash value for a key
    ///
    /// Uses double hashing: h(key, i) = (h1(key) + i * h2(key)) mod m
    /// This technique generates k hash values from just 2 base hashes,
    /// which is faster than computing k independent hashes.
    ///
    /// We use FNV-1a and a modified FNV for h1 and h2 respectively.
    fn hash(&self, key: &[u8], index: usize) -> usize {
        // Use double hashing technique: h(key, i) = h1(key) + i * h2(key)
        let h1 = self.fnv1a_hash(key);
        let h2 = self.fnv1a_hash_variant(key);

        // Combine hashes with index to get the i-th hash value
        let combined = h1.wrapping_add(index.wrapping_mul(h2));

        // Map to bit array position
        combined % self.num_bits
    }

    /// FNV-1a hash function (primary hash)
    ///
    /// FNV-1a is a fast, non-cryptographic hash function with good distribution.
    /// It's ideal for Bloom filters because:
    /// - Fast to compute
    /// - Good avalanche effect (small input changes -> large output changes)
    /// - Works well with arbitrary byte sequences
    fn fnv1a_hash(&self, key: &[u8]) -> usize {
        // FNV-1a parameters for 64-bit
        const FNV_OFFSET_BASIS: u64 = 14695981039346656037;
        const FNV_PRIME: u64 = 1099511628211;

        let mut hash = FNV_OFFSET_BASIS;
        for byte in key {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash as usize
    }

    /// Variant FNV hash (secondary hash for double hashing)
    ///
    /// Similar to FNV-1a but with different initial value
    /// to ensure independence from the primary hash.
    fn fnv1a_hash_variant(&self, key: &[u8]) -> usize {
        // Use different offset basis for independence
        const FNV_OFFSET_BASIS_ALT: u64 = 12345678901234567890;
        const FNV_PRIME: u64 = 1099511628211;

        let mut hash = FNV_OFFSET_BASIS_ALT;
        for byte in key {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        // Ensure h2 is never 0 (would make all hashes the same)
        (hash as usize) | 1
    }

    /// Sets a bit at the given index
    fn set_bit(&mut self, index: usize) {
        let byte_index = index / 8;
        let bit_offset = index % 8;
        if byte_index < self.bits.len() {
            self.bits[byte_index] |= 1 << bit_offset;
        }
    }

    /// Gets a bit at the given index
    fn get_bit(&self, index: usize) -> bool {
        let byte_index = index / 8;
        let bit_offset = index % 8;
        if byte_index < self.bits.len() {
            (self.bits[byte_index] & (1 << bit_offset)) != 0
        } else {
            false
        }
    }

    /// Returns the number of items inserted
    pub fn len(&self) -> usize {
        self.num_items
    }

    /// Returns true if no items have been inserted
    pub fn is_empty(&self) -> bool {
        self.num_items == 0
    }

    /// Returns the size of the filter in bytes
    pub fn size_bytes(&self) -> usize {
        self.bits.len()
    }

    /// Returns the number of bits in the filter
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Returns the number of hash functions used
    pub fn num_hashes(&self) -> usize {
        self.num_hashes
    }

    /// Estimates the current false positive probability
    ///
    /// Formula: (1 - e^(-kn/m))^k
    /// where k=num_hashes, n=num_items, m=num_bits
    pub fn estimated_false_positive_rate(&self) -> f64 {
        if self.num_items == 0 {
            return 0.0;
        }

        let k = self.num_hashes as f64;
        let n = self.num_items as f64;
        let m = self.num_bits as f64;

        // Probability that a bit is still 0 after n insertions
        let prob_bit_zero = (-k * n / m).exp();

        // Probability of false positive
        (1.0 - prob_bit_zero).powf(k)
    }

    /// Serializes the Bloom filter to bytes
    ///
    /// Format:
    /// [num_bits: u32][num_hashes: u32][num_items: u32][bits: bytes]
    ///
    /// This allows storing the Bloom filter alongside SSTable data.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(12 + self.bits.len());

        // Write header
        bytes.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.num_hashes as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.num_items as u32).to_le_bytes());

        // Write bit array
        bytes.extend_from_slice(&self.bits);

        bytes
    }

    /// Deserializes a Bloom filter from bytes
    ///
    /// Returns None if the data is invalid or corrupted.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 12 {
            return None;
        }

        // Read header
        let num_bits = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let num_hashes = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        let num_items = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;

        // Calculate expected bit array size
        let expected_bytes = (num_bits + 7) / 8;
        if data.len() < 12 + expected_bytes {
            return None;
        }

        // Read bit array
        let bits = data[12..12 + expected_bytes].to_vec();

        Some(Self {
            bits,
            num_bits,
            num_hashes,
            num_items,
        })
    }

    /// Writes the Bloom filter to a writer (file)
    pub fn write_to<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let bytes = self.to_bytes();
        writer.write_all(&bytes)?;
        Ok(())
    }

    /// Reads a Bloom filter from a reader (file)
    pub fn read_from<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        // Read header first
        let mut header = [0u8; 12];
        reader.read_exact(&mut header)?;

        let num_bits = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let num_hashes = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
        let num_items = u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;

        // Read bit array
        let num_bytes = (num_bits + 7) / 8;
        let mut bits = vec![0u8; num_bytes];
        reader.read_exact(&mut bits)?;

        Ok(Self {
            bits,
            num_bits,
            num_hashes,
            num_items,
        })
    }

    /// Returns statistics about the Bloom filter
    pub fn stats(&self) -> BloomFilterStats {
        let bits_set = self.bits.iter().map(|b| b.count_ones() as usize).sum();
        let fill_ratio = bits_set as f64 / self.num_bits as f64;

        BloomFilterStats {
            num_bits: self.num_bits,
            num_hashes: self.num_hashes,
            num_items: self.num_items,
            size_bytes: self.bits.len(),
            bits_set,
            fill_ratio,
            estimated_fpp: self.estimated_false_positive_rate(),
        }
    }
}

/// Statistics about a Bloom filter
#[derive(Debug, Clone)]
pub struct BloomFilterStats {
    pub num_bits: usize,
    pub num_hashes: usize,
    pub num_items: usize,
    pub size_bytes: usize,
    pub bits_set: usize,
    pub fill_ratio: f64,
    pub estimated_fpp: f64,
}

impl std::fmt::Display for BloomFilterStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BloomFilter {{ bits: {}, hashes: {}, items: {}, size: {} bytes, fill: {:.1}%, fpp: {:.4}% }}",
            self.num_bits,
            self.num_hashes,
            self.num_items,
            self.size_bytes,
            self.fill_ratio * 100.0,
            self.estimated_fpp * 100.0
        )
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insert_and_query() {
        let mut bf = BloomFilter::new(100, 0.01);

        // Insert some keys
        bf.insert(b"hello");
        bf.insert(b"world");
        bf.insert(b"rust");

        // Should definitely find inserted keys
        assert!(bf.might_contain(b"hello"), "Should find 'hello'");
        assert!(bf.might_contain(b"world"), "Should find 'world'");
        assert!(bf.might_contain(b"rust"), "Should find 'rust'");

        // Should probably not find non-inserted keys
        // (could be false positive, but unlikely with 1% rate)
        // We don't assert on this because false positives are valid
    }

    #[test]
    fn test_no_false_negatives() {
        let mut bf = BloomFilter::new(1000, 0.01);

        // Insert many keys
        let keys: Vec<String> = (0..1000).map(|i| format!("key_{}", i)).collect();
        for key in &keys {
            bf.insert(key.as_bytes());
        }

        // MUST find all inserted keys (no false negatives ever)
        for key in &keys {
            assert!(
                bf.might_contain(key.as_bytes()),
                "Must find inserted key: {}",
                key
            );
        }
    }

    #[test]
    fn test_false_positive_rate() {
        let mut bf = BloomFilter::new(1000, 0.01);

        // Insert 1000 keys
        for i in 0..1000 {
            let key = format!("inserted_{}", i);
            bf.insert(key.as_bytes());
        }

        // Test 10000 non-inserted keys and count false positives
        let mut false_positives = 0;
        for i in 0..10000 {
            let key = format!("not_inserted_{}", i);
            if bf.might_contain(key.as_bytes()) {
                false_positives += 1;
            }
        }

        // False positive rate should be roughly around 1%
        // Allow for some variance (0.1% to 5%)
        let fpp = false_positives as f64 / 10000.0;
        assert!(
            fpp < 0.05,
            "False positive rate {} is too high (expected < 5%)",
            fpp
        );
    }

    #[test]
    fn test_empty_filter() {
        let bf = BloomFilter::new(100, 0.01);

        assert!(bf.is_empty());
        assert_eq!(bf.len(), 0);

        // Empty filter should report nothing contained
        assert!(!bf.might_contain(b"any_key"));
    }

    #[test]
    fn test_serialization() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.insert(b"key1");
        bf.insert(b"key2");
        bf.insert(b"key3");

        // Serialize
        let bytes = bf.to_bytes();

        // Deserialize
        let bf2 = BloomFilter::from_bytes(&bytes).expect("Should deserialize");

        // Should find same keys
        assert!(bf2.might_contain(b"key1"));
        assert!(bf2.might_contain(b"key2"));
        assert!(bf2.might_contain(b"key3"));

        // Should have same stats
        assert_eq!(bf.num_bits(), bf2.num_bits());
        assert_eq!(bf.num_hashes(), bf2.num_hashes());
        assert_eq!(bf.len(), bf2.len());
    }

    #[test]
    fn test_with_params() {
        let bf = BloomFilter::with_params(1024, 7);

        assert_eq!(bf.num_bits(), 1024);
        assert_eq!(bf.num_hashes(), 7);
        assert!(bf.is_empty());
    }

    #[test]
    fn test_stats() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.insert(b"test");

        let stats = bf.stats();
        assert_eq!(stats.num_items, 1);
        assert!(stats.fill_ratio > 0.0);
        assert!(stats.estimated_fpp >= 0.0);
    }

    #[test]
    fn test_large_keys() {
        let mut bf = BloomFilter::new(100, 0.01);

        // Test with large keys
        let large_key = vec![0u8; 10000];
        bf.insert(&large_key);
        assert!(bf.might_contain(&large_key));

        // Different large key should probably not match
        let _different_key = vec![1u8; 10000];
        // Note: might_contain could return true (false positive) - that's valid
    }

    #[test]
    fn test_binary_keys() {
        let mut bf = BloomFilter::new(100, 0.01);

        // Test with binary data (including null bytes)
        let binary_key = vec![0, 1, 2, 0, 255, 128, 64, 0];
        bf.insert(&binary_key);
        assert!(bf.might_contain(&binary_key));
    }

    #[test]
    fn test_clone() {
        let mut bf1 = BloomFilter::new(100, 0.01);
        bf1.insert(b"key1");
        bf1.insert(b"key2");

        let bf2 = bf1.clone();

        // Both should contain the same keys
        assert!(bf1.might_contain(b"key1"));
        assert!(bf2.might_contain(b"key1"));
        assert!(bf1.might_contain(b"key2"));
        assert!(bf2.might_contain(b"key2"));

        // They should be independent
        assert_eq!(bf1.len(), bf2.len());
    }

    #[test]
    fn test_edge_case_small_filter() {
        // Test with minimum size
        let mut bf = BloomFilter::new(1, 0.5);
        bf.insert(b"key");
        assert!(bf.might_contain(b"key"));
    }

    #[test]
    fn test_many_insertions() {
        let mut bf = BloomFilter::new(10000, 0.01);

        // Insert 10000 keys
        for i in 0..10000 {
            let key = format!("batch_key_{}", i);
            bf.insert(key.as_bytes());
        }

        // All should be found
        for i in 0..10000 {
            let key = format!("batch_key_{}", i);
            assert!(bf.might_contain(key.as_bytes()));
        }

        assert_eq!(bf.len(), 10000);
    }
}
