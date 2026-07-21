use crate::ffi::TelemetryPacket;
use std::fs::OpenOptions;
use std::io::Write;

pub fn append(packets: &[TelemetryPacket]) -> bool {
    // Appends a batch of packets to the WAL file. Returns true on success, false on failure.
    let mut file = match OpenOptions::new()
        .create(true)
        .append(true)
        .open("wal.bin")
    {
        Ok(f) => f,
        Err(_) => return false,
    };
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
    if file.sync_all().is_err() {
        return false;
    }
    true
}

pub fn torn_tail_detection() -> bool {
    let file = match OpenOptions::new().write(true).open("wal.bin") {
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
