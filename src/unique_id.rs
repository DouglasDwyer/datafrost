use std::sync::atomic::*;

/// A monotonic counter for generating unique IDs.
static ID_GENERATOR: AtomicU64 = AtomicU64::new(0);

/// Returns a value that is guaranteed to be unique
/// (only returned once) within this program's runtime.
pub fn unique_id() -> u64 {
    ID_GENERATOR.fetch_add(1, Ordering::Relaxed)
}