//! LSM Tree - Main module tests
//!
//! The actual implementation is in lib.rs.
//! This file contains comprehensive tests for the LSM Tree.

// Re-export everything from lib for backwards compatibility
pub use lsm_tree::*;

fn main() {
    println!("LSM Tree Library");
    println!("================");
    println!();
    println!("Available binaries:");
    println!("  cargo run --bin lsm-cli   - Interactive TUI explorer");
    println!("  cargo run --bin lsm-demo  - Simple demo");
    println!();
    println!("Run tests with: cargo test");
}
