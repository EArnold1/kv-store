//! Defines the structure and types for records stored in the log-structured key-value store.
//!
//! # Record Format
//! Each record is serialized as:
//! - Header: (record_type: 1 byte, timestamp: 8 bytes, key_size: 4 bytes, value_size: 4 bytes)
//! - Payload: (key: n bytes, value: n bytes)
//!
//! Buffer layout:
//! `record_type | timestamp | key_size | value_size | key | value`

use std::time::SystemTime;

/// The type of operation represented by a record in the log.
///
/// - `Put`: Insert or update a key-value pair.
/// - `Delete`: Remove a key-value pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    /// Insert or update a key-value pair.
    Put = 0,
    /// Remove a key-value pair.
    Delete = 1,
}

/// Represents a single log record for a key-value operation.
///
/// A record contains the operation type, a timestamp, and the key-value data.
///
/// # Fields
/// - `record_type`: The type of operation (Put or Delete).
/// - `timestamp`: The time the operation was performed.
/// - `key`: The key affected by the operation.
/// - `value`: The value to store (empty for Delete operations).
pub struct Record<'a> {
    /// The type of operation (Put or Delete).
    pub record_type: RecordType,
    /// The time the operation was performed.
    pub timestamp: SystemTime,
    /// The key affected by the operation.
    pub key: &'a [u8],
    /// The value to store (empty for Delete operations).
    pub value: &'a [u8],
}
