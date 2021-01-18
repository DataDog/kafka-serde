use endianness::EndiannessError;
use serde::de;
use serde::ser;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io;

/// Error returned by Kafka serde
pub type Error = Box<ErrorKind>;

/// Result alias where the Error component is a kafka_serde::Error
pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug)]
/// Errors that may happen when parsing a kafka payload (reader or writer)
pub enum ErrorKind {
    /// Wraps an I/O Error. Will only be seen if the write cursors return an I/O error
    Io(io::Error),
    /// Trying to serialize to or from a type that is not yet supported
    TypeNotSupported(&'static str),
    /// A boolean was expected, but a value different than 0 or 1 was found
    InvalidBoolEncoding(u8),
    /// A UTF-8 string was expected, but could not decode it.
    InvalidStringEncoding,
    /// The buffer ran out of bytes but we still had more data to deserialize
    NotEnoughBytes,
    /// Custom errors
    Custom(String),
}

impl Display for ErrorKind {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            ErrorKind::Io(ref ioerr) => write!(fmt, "io error: {}", ioerr),
            ErrorKind::InvalidBoolEncoding(b) => {
                write!(fmt, "{}, expected 0 or 1, found {}", self, b)
            }
            ErrorKind::InvalidStringEncoding => {
                write!(fmt, "string not utf-8 encoded")
            }
            ErrorKind::NotEnoughBytes => {
                write!(fmt, "not enought bytes")
            }
            ErrorKind::TypeNotSupported(s) => {
                write!(fmt, "not supported: {}", s)
            }
            ErrorKind::Custom(ref s) => s.fmt(fmt),
        }
    }
}

impl From<io::Error> for Error {
    #[cold]
    fn from(err: io::Error) -> Error {
        ErrorKind::Io(err).into()
    }
}

impl From<EndiannessError> for Error {
    fn from(_err: EndiannessError) -> Error {
        ErrorKind::NotEnoughBytes.into()
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(_err: std::str::Utf8Error) -> Error {
        ErrorKind::InvalidStringEncoding.into()
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(_err: std::string::FromUtf8Error) -> Error {
        ErrorKind::InvalidStringEncoding.into()
    }
}

impl From<Error> for io::Error {
    fn from(err: Error) -> io::Error {
        io::Error::new(io::ErrorKind::Other, err)
    }
}

impl serde::de::StdError for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl de::Error for Error {
    #[cold]
    fn custom<T: Display>(desc: T) -> Error {
        ErrorKind::Custom(desc.to_string()).into()
    }
}

impl ser::Error for Error {
    #[cold]
    fn custom<T: Display>(msg: T) -> Error {
        ErrorKind::Custom(msg.to_string()).into()
    }
}
