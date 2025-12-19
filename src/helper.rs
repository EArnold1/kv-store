use std::time::{SystemTime, UNIX_EPOCH};

pub fn system_time_to_bytes(sys_time: &SystemTime) -> [u8; 8] {
    let duration = sys_time
        .duration_since(UNIX_EPOCH)
        .expect("System time should not be earlier than UNIX_EPOCH")
        .as_secs() as i64;

    duration.to_le_bytes()
}
