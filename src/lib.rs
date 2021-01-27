// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021
// Datadog, Inc.
//! # kafka_serde - serializers and deserializers for the kafka protocol
//!
//! Details: Options are allowed during serialization, but not deserialization
//!
//! variable sizes like varint, compact bytes, etc, are not supported yet.
//! nullable_string and nullable_bytes are supported during deserialization (they will
//! deserialize into standard string, str and byte-slices) but not yet during serialization.
//!
#![warn(missing_docs, missing_debug_implementations, rust_2018_idioms)]
use std::any::type_name;

pub(crate) fn type_of<T>(_: T) -> &'static str {
    type_name::<T>()
}

macro_rules! type_not_supported {
    ($x:ident) => {
        Err(Box::new(crate::ErrorKind::TypeNotSupported(
            crate::type_of($x),
        )))
    };
    ($x:expr) => {
        Err(Box::new(crate::ErrorKind::TypeNotSupported($x)))
    };
}

mod de;
mod error;
mod ser;

pub use self::error::{Error, ErrorKind, Result};
pub use self::de::from_bytes;
pub use self::ser::to_writer;
