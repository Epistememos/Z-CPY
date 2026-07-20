#[cxx::bridge(namespace = "zcpy")]
mod ffi {
    /// Trivially copyable telemetry record — layout is identical in C++ and Rust.
    ///
    /// 16 bytes total: two fit per 64-byte cache line at peak density.
    /// `#[repr(C)]` is enforced automatically by cxx for all shared structs.
    #[derive(Debug, Clone, Copy, PartialEq)]
    struct TelemetryPacket {
        /// Nanoseconds since Unix epoch.
        timestamp_ns: u64,
        /// Measured signal value (IEEE 754 double).
        value: f64,
    }

    extern "Rust" {
        /// Ingests a batch passed from C++ without copying any packet data.
        ///
        /// On the C++ side this signature becomes:
        ///   `::std::size_t zcpy::ingest_packets(::rust::Slice<const TelemetryPacket>);`
        ///
        /// `rust::Slice<T>` is a fat pointer `{ ptr: *const T, len: usize }` —
        /// 16 bytes on the stack. The TelemetryPacket slab in the C++ MemTable
        /// is never touched by the allocator on the Rust side.
        fn ingest_packets(packets: &[TelemetryPacket]) -> usize;
        fn seed_last_ts(ts: u64);
        fn wal_startup_check() -> bool;
    }
}

mod ingestion;
mod wal;

pub use ffi::TelemetryPacket;

use std::sync::atomic::{AtomicU64, Ordering};
static LAST_TS: AtomicU64 = AtomicU64::new(0);


/// Called by C++ via the cxx bridge. `packets` is a borrowed view into the
/// C++ MemTable buffer; this frame allocates nothing.
pub fn ingest_packets(packets: &[ffi::TelemetryPacket]) -> usize {
    let last_ts = LAST_TS.load(Ordering::Relaxed);
    // Print the Rust-side pointer address for the zero-copy proof in main.cpp.
    eprintln!(
        "[Rust] ingest_packets  @ {:p}  ({} packets)",
        packets.as_ptr(),
        packets.len(),
    );
    let accepted = ingestion::process_batch(packets, last_ts);
    if accepted > 0 {
        let newest_ts = packets.last().unwrap();
        if !wal::append(&packets[..accepted]) {
            eprintln!("[Rust] ERROR: failed to append to WAL");
            return 0;
        }
        LAST_TS.store(newest_ts.timestamp_ns, Ordering::Relaxed);
    
    }
    accepted
}

pub fn seed_last_ts(ts: u64) {
    LAST_TS.store(ts, Ordering::Relaxed);
}

pub fn wal_startup_check() -> bool {
    wal::torn_tail_detection()
}
