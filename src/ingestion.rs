use crate::ffi::TelemetryPacket;

/// Processes a batch without allocating. `packets` is a view into C++ memory.
///
/// This is the stub for the hot ingestion path. Real implementation will:
///   1. Validate monotonicity of `timestamp_ns` within the batch.
///   2. Append to a lock-free WAL ring buffer.
///   3. Signal the compaction thread when the MemTable high-water mark is hit.
pub fn process_batch(packets: &[TelemetryPacket]) -> usize {
    for packet in packets {
        // Placeholder: future WAL write / MemTable flush.
        let _ = (packet.timestamp_ns, packet.value);
    }
    packets.len()
}
