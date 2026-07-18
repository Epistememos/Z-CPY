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
    true
}