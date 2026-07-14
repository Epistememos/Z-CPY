use crate::ffi::TelemetryPacket;

/// Processes a batch without allocating. `packets` is a view into C++ memory.
///
/// This is the stub for the hot ingestion path. Real implementation will:
///   1. Validate monotonicity of `timestamp_ns` within the batch.
///   2. Append to a lock-free WAL ring buffer.
///   3. Signal the compaction thread when the MemTable high-water mark is hit.
pub fn process_batch(packets: &[TelemetryPacket], last_ts: u64) -> usize {
    let mut prev = last_ts;
    for packet in packets {
        // Validate monotonicity of timestamps within the batch.
        // This is a cheap sanity check for the zero-copy proof in main.cpp.
        if packet.timestamp_ns <= prev {
            eprintln!(
                "[Rust] ERROR: non-monotonic timestamp {} < {}",
                packet.timestamp_ns, prev
            );
            return 0;
        }
        prev = packet.timestamp_ns;
    }
    packets.len()
}
