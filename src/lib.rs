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
        //fn ingest_packets(packets: &[TelemetryPacket]) -> usize;
        fn ingest_packets(stream_id: u32, packets: &[TelemetryPacket]) -> usize;
        //fn seed_last_ts(ts: u64);
        fn seed_last_ts(stream_id: u32, ts: u64);
        fn wal_startup_check(stream_id: u32) -> bool;
        fn wal_replay_len(stream_id: u32, memtable_count: usize) -> usize;
        fn wal_replay_packet(stream_id: u32, index: usize) -> TelemetryPacket;
        
    }
}

mod ingestion;
mod wal;

pub use ffi::TelemetryPacket;

use std::collections::HashMap;
use std::sync::{Mutex, LazyLock};


static WAL_REPLAY_MAP: LazyLock<Mutex<HashMap<u32, Vec<TelemetryPacket>>>> = LazyLock::new(|| {Mutex::new(HashMap::new())});
static LAST_TS_MAP: LazyLock<Mutex<HashMap<u32, u64>>> = LazyLock::new(|| { Mutex::new(HashMap::new())});

/// Called by C++ via the cxx bridge. `packets` is a borrowed view into the
/// C++ MemTable buffer; this frame allocates nothing.
pub fn ingest_packets(stream_id: u32, packets: &[ffi::TelemetryPacket]) -> usize {
    // Acquire the lock for reading
    let last_ts = {
        let map = LAST_TS_MAP.lock().expect("Lock poisoned");
        map.get(&stream_id).copied().unwrap_or(0)
    };
   

    let accepted = ingestion::process_batch(packets, last_ts);
    if accepted > 0 {
        let newest_ts = packets.last().unwrap();
        if !wal::append(stream_id, &packets[..accepted]) {
            eprintln!("[Rust] ERROR: failed to append to WAL");
            return 0;
        }
    
        let mut map = LAST_TS_MAP.lock().expect("Lock poisoned");
        map.insert(stream_id, newest_ts.timestamp_ns);
    }
        
    accepted
}

pub fn seed_last_ts(stream_id: u32, ts: u64) {
    let mut map = LAST_TS_MAP.lock().expect("Lock poisoned");
    map.insert(stream_id, ts);
}

pub fn wal_startup_check(stream_id: u32) -> bool {
    wal::torn_tail_detection(stream_id)
}

pub fn wal_replay_len(stream_id: u32, memtable_count: usize) -> usize {
    
    let replayed = match wal::replay(stream_id, memtable_count) {
        Some(packets) => packets,
        None => return 0,
    };
    let mut replay_map = WAL_REPLAY_MAP.lock().expect("Lock poisoned.");
    
    
    let len = replayed.len();
    replay_map.insert(stream_id, replayed);
    len
}

pub fn wal_replay_packet(stream_id: u32, index: usize) -> TelemetryPacket {
    let replay_map = WAL_REPLAY_MAP.lock().expect("Lock poisoned.");
    let replay = replay_map.get(&stream_id).unwrap();
    replay[index]
}
