use std::path::PathBuf;

const MAX_LOG_SIZE: u64 = 1024; // 1KB
// const MAX_LOG_SIZE: usize = 50 * 1024 * 1024; // 10MB

/// a function to check when to rotate the file_id
pub fn should_rotate(active_path: &PathBuf) -> bool {
    let current_size = std::fs::metadata(active_path).map_or(0, |m| m.len());

    current_size > MAX_LOG_SIZE
}
