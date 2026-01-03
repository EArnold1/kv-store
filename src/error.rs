use std::{fmt::Debug, io::Error as IoError};

pub enum KvError {
    Io(IoError),
    InvalidDir,
}

impl From<IoError> for KvError {
    fn from(err: IoError) -> Self {
        KvError::Io(err)
    }
}

impl Debug for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KvError::InvalidDir => write!(f, "Invalid directory"),
            KvError::Io(err) => write!(f, "IO error: {}", err),
        }
    }
}
