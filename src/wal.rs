use std::path::PathBuf;

const MAX_LOG_SIZE: u64 = 1024;
// const MAX_LOG_SIZE: usize = 50 * 1024 * 1024; // 10MB

/// a function to check when to rotate the file_id
pub fn should_rotate(active_path: &PathBuf) -> bool {
    let current_size = std::fs::metadata(active_path).map(|m| m.len()).unwrap_or(0);

    current_size > MAX_LOG_SIZE
}
