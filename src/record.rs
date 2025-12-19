//   buffer contents: record_type 1byte | timestamp 8bytes | key_size 4bytes | value_size 4bytes | key n-bytes | value n-bytes

// Record
// -Header(record_type, timestamp, key_size, value_size)
// -Payload(key, value)
//

use std::time::SystemTime;

pub enum RecordType {
    Put = 0,
    Delete = 1,
}

pub struct Record<'a> {
    pub record_type: RecordType,
    pub timestamp: SystemTime,
    pub key: &'a [u8],
    pub value: &'a [u8],
}
