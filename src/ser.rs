use crate::error::{Error, Result};
use serde::{ser, Serialize};
use std::io;

struct KafkaSerializer<W> {
    writer: W,
}

/// Serializes a kafka payload into a I/O stream
///
/// Often times the kafka protocol will require the message to be manipulated after serializing
/// (for instance to add the size to the beginning), so the initial stream is returned.
///
/// # Examples
/// ```
/// use serde::Serialize;
/// use std::io::{Write, Cursor};
///
/// #[derive(Serialize, Debug)]
/// struct RequestHeader {
///     api_key: i16,
///     api_version: i16,
///     correlation_id: i32,
///     client_id: &'static str,
/// }
///
/// let req = RequestHeader {
///     api_key: 0,
///     api_version: 0,
///     correlation_id: 1,
///     client_id: ""
/// };
///
/// let mut x = Cursor::new(Vec::<u8>::with_capacity(std::mem::size_of::<RequestHeader>()));
/// x.set_position(4u64); // leave space for the size;
/// let mut x = kafka_serde::to_writer(x, &req).unwrap();
/// let sz = (x.position() as usize - std::mem::size_of::<i32>()) as i32;
/// x.set_position(0u64);
/// x.write(&sz.to_be_bytes()).unwrap(); // writes the size to the beginning of the payload
/// ```

#[inline]
pub fn to_writer<W, T>(writer: W, value: &T) -> Result<W>
where
    T: Serialize,
    W: io::Write,
{
    let mut serializer = KafkaSerializer { writer };
    value.serialize(&mut serializer)?;
    Ok(serializer.writer)
}

impl<'a, W> ser::Serializer for &'a mut KafkaSerializer<W>
where
    W: io::Write,
{
    type Ok = ();

    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<()> {
        if v {
            self.serialize_u8(1)
        } else {
            self.serialize_u8(0)
        }
    }

    // JSON does not distinguish between different sizes of integers, so all
    // signed integers will be serialized the same and all unsigned integers
    // will be serialized the same. Other formats, especially compact binary
    // formats, may need independent logic for the different sizes.
    fn serialize_i8(self, v: i8) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        type_not_supported!(v)
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<()> {
        type_not_supported!(v)
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        self.writer.write_all(&((v.len() as i16).to_be_bytes()))?;
        self.writer.write_all(v.as_bytes())?;
        Ok(())
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
        type_not_supported!("bytes")
    }

    fn serialize_none(self) -> Result<()> {
        Ok(())
    }

    fn serialize_some<T>(self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<()> {
        type_not_supported!("ser-unit")
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        type_not_supported!("ser-unit-struct")
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        type_not_supported!("ser-unit-variant")
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        type_not_supported!("ser-newtype-variant")
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        let len = len.unwrap() as i32;
        self.writer.write_all(&len.to_be_bytes())?;
        Ok(self)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.writer.write_all(&((len as i32).to_be_bytes()))?;
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        type_not_supported!("ser-tuple-struct")
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        type_not_supported!("ser-tuple-variant")
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        type_not_supported!("ser-map")
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        type_not_supported!("ser-struct-variant")
    }
}

impl<'a, W> ser::SerializeSeq for &'a mut KafkaSerializer<W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, W> ser::SerializeTuple for &'a mut KafkaSerializer<W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, W> ser::SerializeTupleStruct for &'a mut KafkaSerializer<W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        type_not_supported!("ser-tuple-struct-trait")
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, W> ser::SerializeTupleVariant for &'a mut KafkaSerializer<W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        type_not_supported!("ser-tuple-variant-trait")
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, W> ser::SerializeMap for &'a mut KafkaSerializer<W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = Error;

    fn serialize_key<T>(&mut self, _key: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        type_not_supported!("ser-map-trait")
    }

    // It doesn't make a difference whether the colon is printed at the end of
    // `serialize_key` or at the beginning of `serialize_value`. In this case
    // the code is a bit simpler having it here.
    fn serialize_value<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        type_not_supported!("ser-map-trait")
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, W> ser::SerializeStruct for &'a mut KafkaSerializer<W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, W> ser::SerializeStructVariant for &'a mut KafkaSerializer<W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        type_not_supported!("ser-struct-variant-trait")
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_repr::*;

    macro_rules! test_integer {
        ($t:ty) => {{
            let x = io::Cursor::new(vec![]);
            let i: $t = 1;
            let mut x = to_writer(x, &i).unwrap();
            x.set_position(0);
            let c = x.into_inner();
            assert_eq!(c.len(), std::mem::size_of::<$t>());
            assert_eq!(c.last().unwrap(), &1u8);
            for i in &c[0..c.len() - 1] {
                assert_eq!(i, &0);
            }
        }};
    }

    #[test]
    fn test_integers() {
        test_integer!(u8);
        test_integer!(u16);
        test_integer!(u32);
        test_integer!(u64);
        test_integer!(i8);
        test_integer!(i16);
        test_integer!(i32);
        test_integer!(i64);
    }

    #[test]
    fn test_strings() {
        let x = io::Cursor::new(vec![]);
        let s = String::from("abc");
        let mut x = to_writer(x, &s).unwrap();
        x.set_position(0);
        let c = x.into_inner();
        assert_eq!(c.len(), 5);
        assert_eq!(c[0], 0);
        assert_eq!(c[1], 3);
        assert_eq!(String::from_utf8(c[2..5].to_vec()).unwrap(), "abc");

        let x = io::Cursor::new(vec![]);
        let mut x = to_writer(x, &"abc").unwrap();
        x.set_position(0);
        let c = x.into_inner();
        assert_eq!(c.len(), 5);
        assert_eq!(c[0], 0);
        assert_eq!(c[1], 3);
        assert_eq!(String::from_utf8(c[2..5].to_vec()).unwrap(), "abc");

        let x = io::Cursor::new(vec![]);
        let mut x = to_writer(x, &"").unwrap();
        x.set_position(0);
        let c = x.into_inner();
        assert_eq!(c.len(), 2);
        assert_eq!(c[0], 0);
        assert_eq!(c[1], 0);
    }

    #[test]
    fn test_list() {
        let x = io::Cursor::new(vec![]);
        let bytes = [1u32; 4];
        let x = to_writer(x, &bytes).unwrap();
        let c = x.into_inner();
        assert_eq!(c.len(), 20);
        assert_eq!(c[0], 0);
        assert_eq!(c[1], 0);
        assert_eq!(c[2], 0);
        assert_eq!(c[3], 4);
    }

    #[test]
    fn test_struct() {
        #[derive(Serialize)]
        struct Test {
            a: i32,
            b: i8,
        }

        let x = io::Cursor::new(vec![]);
        let t = Test { a: 1, b: 2 };
        let x = to_writer(x, &t).unwrap();
        let c = x.into_inner();
        assert_eq!(c.len(), 5);
        assert_eq!(c[0], 0);
        assert_eq!(c[1], 0);
        assert_eq!(c[2], 0);
        assert_eq!(c[3], 1);
        assert_eq!(c[4], 2);
    }

    #[test]
    fn test_enum_repr() {
        #[derive(Serialize_repr)]
        #[repr(u8)]
        #[allow(dead_code)]
        enum Test {
            A = 0,
            B = 1,
        }

        let x = io::Cursor::new(vec![]);
        let t = Test::B;
        let x = to_writer(x, &t).unwrap();
        let c = x.into_inner();
        assert_eq!(c.len(), 1);
        assert_eq!(c[0], 1);
    }

    #[test]
    fn test_none() {
        let x = io::Cursor::new(vec![]);
        let t: Option<u32> = None;
        let x = to_writer(x, &t).unwrap();
        let c = x.into_inner();
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn test_some() {
        let x = io::Cursor::new(vec![]);
        let t: Option<u32> = Some(2);
        let x = to_writer(x, &t).unwrap();
        let c = x.into_inner();
        assert_eq!(c.len(), 4);
        assert_eq!(c[0], 0);
        assert_eq!(c[1], 0);
        assert_eq!(c[2], 0);
        assert_eq!(c[3], 2);
    }
}
