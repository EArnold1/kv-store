use std::io::Error as IoError;

#[derive(Debug)]
pub enum KvError {
    Io(IoError),

    NotFound,
}

impl From<IoError> for KvError {
    fn from(err: IoError) -> Self {
        KvError::Io(err)
    }
}
