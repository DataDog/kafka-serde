# kafka-serde

Rust's serde implementation for the Kafka protocol.

This allows you to serialize and deserialize kafka payloads. It can
be used as a building block for a native-rust kafka client.

## Usage

Serializing the kafka request header:

```rust
use serde::Serialize;
use std::io::{Write, Cursor};

#[derive(Serialize, Debug)]
struct RequestHeader {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: &'static str,
}

let req = RequestHeader {
    api_key: 0,
    api_version: 0,
    correlation_id: 1,
    client_id: ""
};

let mut x = Cursor::new(Vec::<u8>::new());
kafka_serde::to_writer(x, &req).unwrap();
```

Deserializing the kafka response header:

```rust
use serde::Serialize;

#[derive(Deserialize, Default, Debug, Clone)]
struct ResponseHeader {
    pub correlation: i32,
}

let data : Vec<u8> = [0x0, 0x0, 0x0, 0x1];
let resp: ResponseHeader = kafka_serde::from_bytes(&data).unwrap();
```

## Support

All Kafka protocol types are listed [here](https://kafka.apache.org/protocol#protocol_types)

* The fixed-size types are supported and map to their native rust types (`int8 -> i8`, etc).
* Strings can be deserialized to both `String` and `&str`, and similarly
  for bytes
* UUID is not supported (yet)
* variable-size types like `VARLONG` and `COMPACT_STRING` are not
  supported (yet)
