use endianness::*;
use serde::de::Visitor;
use serde::Deserialize;
use serde::Deserializer;

use crate::error::{Error, ErrorKind, Result};

struct KafkaDeserializer<'de> {
    buf: &'de [u8],
    pos: usize,
}

impl<'de> KafkaDeserializer<'de> {
    fn check_room(&self, room: usize) -> Result<()> {
        if self.pos + room > self.buf.len() {
            Err(Box::new(ErrorKind::NotEnoughBytes))
        } else {
            Ok(())
        }
    }

    fn check_room_for<T: Sized>(&self) -> Result<()> {
        self.check_room(std::mem::size_of::<T>())
    }

    fn slice(&mut self, len: usize) -> Result<&'de [u8]> {
        self.check_room(len)?;
        let begin = self.pos;
        self.pos += len;
        Ok(&self.buf[begin..self.pos])
    }

    fn copy_slice(&mut self, len: usize) -> Result<Vec<u8>> {
        self.check_room(len)?;
        let begin = self.pos;
        self.pos += len;
        let mut bytes: Vec<u8> = Vec::with_capacity(len);
        bytes.extend_from_slice(&self.buf[begin..self.pos]);
        Ok(bytes)
    }

    fn read_i8(&mut self) -> Result<i8> {
        self.check_room_for::<i8>()?;
        let value = self.buf[self.pos];
        self.pos += std::mem::size_of::<i8>();
        Ok(value as i8)
    }

    fn read_u8(&mut self) -> Result<u8> {
        self.check_room_for::<u8>()?;
        let value = self.buf[self.pos];
        self.pos += std::mem::size_of::<u8>();
        Ok(value)
    }

    fn read_i16(&mut self) -> Result<i16> {
        let value = read_i16(&self.buf[self.pos..], ByteOrder::BigEndian)?;
        self.pos += std::mem::size_of::<i16>();
        Ok(value)
    }

    fn read_u16(&mut self) -> Result<u16> {
        let value = read_u16(&self.buf[self.pos..], ByteOrder::BigEndian)?;
        self.pos += std::mem::size_of::<u16>();
        Ok(value)
    }

    fn read_i32(&mut self) -> Result<i32> {
        let value = read_i32(&self.buf[self.pos..], ByteOrder::BigEndian)?;
        self.pos += std::mem::size_of::<i32>();
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32> {
        let value = read_u32(&self.buf[self.pos..], ByteOrder::BigEndian)?;
        self.pos += std::mem::size_of::<u32>();
        Ok(value)
    }

    fn read_i64(&mut self) -> Result<i64> {
        let value = read_i64(&self.buf[self.pos..], ByteOrder::BigEndian)?;
        self.pos += std::mem::size_of::<i64>();
        Ok(value)
    }

    fn read_u64(&mut self) -> Result<u64> {
        let value = read_u64(&self.buf[self.pos..], ByteOrder::BigEndian)?;
        self.pos += std::mem::size_of::<u64>();
        Ok(value)
    }
}

/// Deserialize a kafka payload contained in a byte slice
///
/// # Examples
/// ```
/// use serde::Deserialize;
///
/// #[derive(Deserialize, Debug, Default)]
/// struct ResponseHeader {
///     correlation_id: i32,
/// }
///
/// fn get_header(data: &[u8]) -> kafka_serde::Result<ResponseHeader> {
///     let resp: ResponseHeader = kafka_serde::from_bytes(data)?;
///     Ok(resp)
/// }
/// ```
#[inline]
pub fn from_bytes<'de, T>(buf: &'de [u8]) -> Result<T>
where
    T: Deserialize<'de>,
{
    let mut k_der = KafkaDeserializer { buf, pos: 0 };
    T::deserialize(&mut k_der)
}

impl<'de, 'a> Deserializer<'de> for &'a mut KafkaDeserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        type_not_supported!("de-deserialize-any")
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.read_u8()?;
        match value {
            0 => visitor.visit_bool(false),
            1 => visitor.visit_bool(true),
            _ => Err(ErrorKind::InvalidBoolEncoding(value).into()),
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i8(self.read_i8()?)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_u8(self.read_u8()?)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u16(self.read_u16()?)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i16(self.read_i16()?)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u32(self.read_u32()?)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i32(self.read_i32()?)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u64(self.read_u64()?)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i64(self.read_i64()?)
    }

    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        type_not_supported!("de-f32")
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value = read_f64(&self.buf[self.pos..], ByteOrder::BigEndian)?;
        self.pos += std::mem::size_of::<f64>();
        visitor.visit_f64(value)
    }

    fn deserialize_unit<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        type_not_supported!("de-unit")
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        type_not_supported!("de-char")
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let len = self.read_i16()?;
        if len == 0 || len == -1 {
            return visitor.visit_borrowed_str("");
        }
        let len = len as usize;
        let out_str = std::str::from_utf8(self.slice(len)?)?;
        visitor.visit_borrowed_str(out_str)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let len = self.read_i16()?;
        if len == 0 || len == -1 {
            return visitor.visit_string("".into());
        }
        let len = len as usize;
        let bytes = self.copy_slice(len)?;
        let out_string = String::from_utf8(bytes)?;
        visitor.visit_string(out_string)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let len = self.read_i32()?;
        if len == 0 || len == -1 {
            return visitor.visit_borrowed_bytes(&[]);
        }
        let len = len as usize;
        visitor.visit_borrowed_bytes(self.slice(len)?)
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        type_not_supported!("de-byte-buf")
    }

    fn deserialize_option<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        type_not_supported!("de-option")
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        type_not_supported!("de-unit-struct")
    }

    fn deserialize_newtype_struct<V>(self, _name: &'static str, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        type_not_supported!("de-newtype")
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let mut len = self.read_i32()?;
        if len == -1 {
            len = 0;
        }
        self.deserialize_tuple(len as usize, visitor)
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        struct Access<'a, 'de> {
            deserializer: &'a mut KafkaDeserializer<'de>,
            len: usize,
        }

        impl<'de, 'a> serde::de::SeqAccess<'de> for Access<'a, 'de> {
            type Error = Error;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
            where
                T: serde::de::DeserializeSeed<'de>,
            {
                if self.len > 0 {
                    self.len -= 1;
                    let value =
                        (serde::de::DeserializeSeed::deserialize(seed, &mut *self.deserializer))?;
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }

            fn size_hint(&self) -> Option<usize> {
                Some(self.len)
            }
        }

        visitor.visit_seq(Access {
            deserializer: self,
            len,
        })
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        type_not_supported!("de-tuple-struct")
    }

    fn deserialize_map<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        type_not_supported!("de-map")
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(fields.len(), visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        type_not_supported!("de-enum")
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        type_not_supported!("de-identifier")
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        type_not_supported!("de-ignored-any")
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize, Debug, Default)]
    struct Dummy1 {
        value: u8,
        off: bool,
    }

    #[test]
    fn test_de_u8() {
        let data = [0x05, 0x01];
        let dummy: Dummy1 = from_bytes(&data).unwrap();
        assert_eq!(dummy.value, 5);
        assert_eq!(dummy.off, true);
    }

    #[derive(Deserialize, Debug, Default)]
    struct Dummy2 {
        value: i32,
    }

    #[test]
    fn test_serde_decode_i32() {
        let data = [0x00, 0x00, 0x00, 0x10];

        let dummy: Dummy2 = from_bytes(&data).unwrap();
        assert_eq!(dummy.value, 16);
    }

    #[derive(Deserialize, Debug, Default)]
    struct Dummy3 {
        value: u32,
    }

    #[test]
    fn test_serde_decode_u32() {
        let data = [0x00, 0x00, 0x00, 0x10];
        let dummy: Dummy3 = from_bytes(&data).unwrap();
        assert_eq!(dummy.value, 16);
    }

    #[derive(Deserialize, Debug, Default)]
    struct DummyString {
        value: String,
    }

    #[derive(Deserialize, Debug, Default)]
    struct DummyStringReference<'a> {
        value: &'a str,
    }

    #[derive(Deserialize, Debug, Default)]
    struct DummyByteReference<'a> {
        bytes: &'a [u8],
    }

    #[derive(Deserialize, Debug, Default)]
    struct Foo<'a> {
        string: &'a str,
        bytes: &'a [u8],
    }

    #[derive(Deserialize, Debug, Default)]
    struct ByteReferenceSeq<'a> {
        #[serde(borrow)]
        bytes: Vec<Foo<'a>>,
    }

    #[test]
    fn test_serde_decode_string() {
        let data = [
            0x00, 0x0a, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x2d, 0x31,
        ];
        let dummy: DummyString = from_bytes(&data).unwrap();
        assert_eq!(dummy.value, "consumer-1");
    }

    #[test]
    fn test_serde_decode_string_reference() {
        let data = [
            0x00, 0x0a, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x2d, 0x31,
        ];
        let dummy: DummyStringReference<'_> = from_bytes(&data).unwrap();
        assert_eq!(dummy.value, "consumer-1");
    }

    #[test]
    fn test_serde_decode_empty_string_reference() {
        let data = [0x00, 0x00];
        let dummy: DummyStringReference<'_> = from_bytes(&data).unwrap();
        assert_eq!(dummy.value, "");
    }

    #[test]
    fn test_serde_decode_nullable_string() {
        let data = [255, 255];
        let dummy: DummyString = from_bytes(&data).unwrap();
        assert_eq!(dummy.value, "");
    }

    #[test]
    fn test_serde_decode_byte_reference() {
        let data = [
            0x00, 0x00, 0x00, 0x0a, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x2d, 0x31,
        ];

        let dummy: DummyByteReference<'_> = from_bytes(&data).unwrap();
        assert_eq!(dummy.bytes.len(), 10);
        assert_eq!(dummy.bytes, &data[4..]);
    }

    #[test]
    fn test_serde_decode_byte_reference_sequence() {
        let data = [
            0x00, 0x00, 0x00, 0x01, 0x00, 0x0a, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72,
            0x2d, 0x31, 0x00, 0x00, 0x00, 0x01, 0x0a,
        ];

        let dummy: ByteReferenceSeq<'_> = from_bytes(&data).unwrap();
        assert_eq!(dummy.bytes.len(), 1);
    }

    #[test]
    fn test_serde_decode_empty_byte_reference() {
        let data = [0x00, 0x00, 0x00, 0x00];
        let dummy: DummyByteReference<'_> = from_bytes(&data).unwrap();
        assert_eq!(dummy.bytes.len(), 0);
    }

    #[derive(Deserialize, Debug, Default)]
    struct DummySequence {
        value: Vec<u16>,
    }

    #[test]
    fn test_serde_decode_seq_u32() {
        let data = [0x00, 0x00, 0x00, 0x3, 0x00, 0x01, 0x00, 0x02, 0x00, 0x03];
        let dummy: DummySequence = from_bytes(&data).unwrap();
        assert_eq!(dummy.value.len(), 3);
        assert_eq!(dummy.value[0], 1);
        assert_eq!(dummy.value[1], 2);
        assert_eq!(dummy.value[2], 3);
    }

    #[test]
    fn test_nullable_bytes() {
        // an array of size -1 is to be interpreted as containing 0 elements
        let data = [255, 255, 255, 255];
        let dummy: DummySequence = from_bytes(&data).unwrap();
        assert_eq!(dummy.value.len(), 0);
    }
}
