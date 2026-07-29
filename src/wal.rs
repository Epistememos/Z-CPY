use crate::ffi::TelemetryPacket;
use std::io::Write;
use std::collections::HashMap;
use std::sync::{Mutex, LazyLock};
use std::fs::{File, OpenOptions};

static WAL_MAP: LazyLock<Mutex<HashMap<u32, File>>> = LazyLock::new(|| { Mutex::new(HashMap::new())});

pub fn append(stream_id: u32, packets: &[TelemetryPacket]) -> bool {
    // Appends a batch of packets to the WAL file. Returns true on success, false on failure.
    //
    let mut map = WAL_MAP.lock().expect("Lock poisoned");
    if !map.contains_key(&stream_id) {
        let filename = format!("wal_{}.bin", stream_id);
        // Create file and insert into wal map
        match OpenOptions::new().create(true).append(true).open(&filename) {
            Ok(f) => {map.insert(stream_id,f); }
            Err(_) => return false,
        }
    }

    let file = map.get_mut(&stream_id).unwrap();
    

   
    // TelemetryPacket is #[repr(C)] and trivially copyable, so we can safely treat the slice as a byte slice.
    let bytes = unsafe {
        std::slice::from_raw_parts(
            packets.as_ptr() as *const u8,
            std::mem::size_of_val(packets)
        )
    };
    if file.write_all(bytes).is_err() {
        return false;
    }
    // Main performance bottleneck, for future versions, either batch writes (group commit) or just replicate on another machine (like Kafka)
    if file.sync_all().is_err() {
        return false;
    }
    true
}

pub fn torn_tail_detection(stream_id: u32) -> bool {
    let filename = format!("wal_{}.bin", stream_id);
    let file = match OpenOptions::new().write(true).open(&filename) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return true,
        Err(_) => return false,
    };
    let metadata = match file.metadata() {
        Ok(m) => m,
        Err(_) => return false,
    };
    let file_size = metadata.len();
    let size_of_packet = std::mem::size_of::<TelemetryPacket>();
    // Check for torn tail: if the file size is not a multiple of the size of TelemetryPacket, truncate it to the last complete packet.
    if file_size % size_of_packet as u64 != 0 {
        let clean_size = (file_size / size_of_packet as u64) * size_of_packet as u64;
        
        if file.set_len(clean_size).is_err() {
            return false;
        }
    }
    true
}

pub fn replay(stream_id: u32, memtable_count: usize) -> Option<Vec<TelemetryPacket>> {
    let filename = format!("wal_{}.bin", stream_id);
    let bytes: Vec<u8> = match std::fs::read(&filename) {
    Ok(b) => b,
    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Some(vec![]),
    Err(_) => return None,
    };
    // Interpret the bytes as a slice of TelemetryPacket.
    let packet_count = bytes.len() / std::mem::size_of::<TelemetryPacket>();
    let packets: &[TelemetryPacket] = unsafe {
        std::slice::from_raw_parts(
            bytes.as_ptr() as *const TelemetryPacket,
            packet_count,
        )
    };
    // Return the packets that have not yet been ingested into the MemTable.
    if memtable_count >= packets.len() {
        return Some(vec![]);
    }

    Some(packets[memtable_count..].to_vec())
    
}
