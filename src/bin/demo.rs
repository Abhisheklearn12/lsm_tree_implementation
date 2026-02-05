//! Simple demo of LSM Tree functionality
//!
//! Run with: cargo run --bin lsm-demo

use lsm_tree::LSMTree;
use std::path::PathBuf;

fn main() {
    println!("=== LSM Tree with Bloom Filters Demo ===\n");

    // Clean up any existing data for a fresh demo
    let _ = std::fs::remove_dir_all("./lsm_data");

    let mut lsm =
        LSMTree::new(PathBuf::from("./lsm_data"), 100).expect("Failed to create LSM tree");

    // Example 1: Basic key-value operations
    println!("Example 1: Basic Operations");
    println!("---------------------------");

    lsm.put(b"user:1".to_vec(), b"Alice".to_vec())
        .expect("Failed to put user:1");
    lsm.put(b"user:2".to_vec(), b"Bob".to_vec())
        .expect("Failed to put user:2");
    lsm.put(b"user:3".to_vec(), b"Charlie".to_vec())
        .expect("Failed to put user:3");

    if let Some(value) = lsm.get(b"user:1") {
        println!("user:1 = {}", String::from_utf8_lossy(&value));
    }

    if let Some(value) = lsm.get(b"user:2") {
        println!("user:2 = {}", String::from_utf8_lossy(&value));
    }

    match lsm.get(b"user:999") {
        Some(value) => println!("user:999 = {}", String::from_utf8_lossy(&value)),
        None => println!("user:999 = Not found"),
    }

    println!();

    // Example 2: Updates
    println!("Example 2: Updates");
    println!("------------------");

    lsm.put(b"user:1".to_vec(), b"Alice Smith".to_vec())
        .expect("Failed to update user:1");
    if let Some(value) = lsm.get(b"user:1") {
        println!("Updated user:1 = {}", String::from_utf8_lossy(&value));
    }

    println!();

    // Example 3: Trigger flush
    println!("Example 3: Automatic Flush with Bloom Filters");
    println!("----------------------------------------------");
    println!("Inserting 20 entries to trigger flush...");

    for i in 0..20 {
        let key = format!("product:{}", i);
        let value = format!("Item {}", i);
        lsm.put(key.into_bytes(), value.into_bytes())
            .unwrap_or_else(|_| panic!("Failed to put product:{}", i));
    }

    println!("Number of entries in memtable: {}", lsm.len());
    println!("Number of SSTables on disk: {}", lsm.sstable_count());

    println!();

    // Example 4: Bloom Filter effectiveness
    println!("Example 4: Bloom Filter Effectiveness");
    println!("--------------------------------------");

    lsm.reset_bloom_filter_stats();

    println!("Searching for 100 non-existent keys...");
    for i in 1000..1100 {
        let key = format!("nonexistent:{}", i);
        let _ = lsm.get(key.as_bytes());
    }

    println!("Searching for 20 existing keys...");
    for i in 0..20 {
        let key = format!("product:{}", i);
        let _ = lsm.get(key.as_bytes());
    }

    let stats = lsm.bloom_filter_stats();
    println!("\n{}", stats);

    println!("=== Demo Complete ===");
    println!("\nRun 'cargo run --bin lsm-cli' for interactive TUI!");
}
