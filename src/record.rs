//   buffer contents: record_type 1byte | key_size 4bytes | value_size 4bytes | key n-bytes | value n-bytes

// Record
// -Header(record_type, key_size, value_size)
// -Payload(key, value)
//

pub enum RecordType {
    Put = 0,
    Delete = 1,
}

pub struct Record<'a> {
    pub record_type: RecordType,
    pub key: &'a [u8],
    pub value: &'a [u8],
}
