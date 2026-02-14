//! A [`RowWriter`] implementation that collects decoded TDS values into
//! `Vec<SqlValue>`, bridging the zero-copy row writer trait with the
//! high-level `SqlValue` type system.

use std::borrow::Cow;
use tabby::protocol::wire::SqlValue;
use tabby::row_writer::RowWriter;

/// A `RowWriter` that collects values into a `Vec<SqlValue>`, preserving
/// the existing API for callers that need `SqlValue`s.
///
/// This is the bridge between tabby's zero-copy `RowWriter` trait and
/// the rich `SqlValue` type system used by `Row` and `Client`.
pub struct SqlValueWriter {
    /// The collected column values for the current row.
    pub values: Vec<SqlValue<'static>>,
}

impl SqlValueWriter {
    /// Create a new writer pre-allocated for `capacity` columns.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
        }
    }
}

impl RowWriter for SqlValueWriter {
    fn write_null(&mut self, _col: usize) {
        self.values.push(SqlValue::I32(None));
    }
    fn write_bool(&mut self, _col: usize, val: bool) {
        self.values.push(SqlValue::Bit(Some(val)));
    }
    fn write_u8(&mut self, _col: usize, val: u8) {
        self.values.push(SqlValue::U8(Some(val)));
    }
    fn write_i16(&mut self, _col: usize, val: i16) {
        self.values.push(SqlValue::I16(Some(val)));
    }
    fn write_i32(&mut self, _col: usize, val: i32) {
        self.values.push(SqlValue::I32(Some(val)));
    }
    fn write_i64(&mut self, _col: usize, val: i64) {
        self.values.push(SqlValue::I64(Some(val)));
    }
    fn write_f32(&mut self, _col: usize, val: f32) {
        self.values.push(SqlValue::F32(Some(val)));
    }
    fn write_f64(&mut self, _col: usize, val: f64) {
        self.values.push(SqlValue::F64(Some(val)));
    }
    fn write_str(&mut self, _col: usize, val: &str) {
        self.values
            .push(SqlValue::String(Some(Cow::Owned(val.to_owned()))));
    }
    fn write_bytes(&mut self, _col: usize, val: &[u8]) {
        self.values
            .push(SqlValue::Binary(Some(Cow::Owned(val.to_owned()))));
    }
    fn write_date(&mut self, _col: usize, days: i32) {
        self.values
            .push(SqlValue::Date(Some(tabby::temporal::Date::new(
                days as u32,
            ))));
    }
    fn write_time(&mut self, _col: usize, nanos: i64) {
        let increments = nanos as u64 / 100;
        self.values
            .push(SqlValue::Time(Some(tabby::temporal::Time::new(
                increments, 7,
            ))));
    }
    fn write_datetime(&mut self, _col: usize, _micros: i64) {
        self.values.push(SqlValue::DateTime2(None));
    }
    fn write_datetimeoffset(&mut self, _col: usize, _micros: i64, _offset_minutes: i16) {
        self.values.push(SqlValue::DateTimeOffset(None));
    }
    fn write_decimal(&mut self, _col: usize, value: i128, _precision: u8, scale: u8) {
        let num = tabby::Numeric::new_with_scale(value, scale);
        self.values.push(SqlValue::Numeric(Some(num)));
    }
    fn write_guid(&mut self, _col: usize, bytes: &[u8; 16]) {
        self.values
            .push(SqlValue::Guid(Some(uuid::Uuid::from_bytes(*bytes))));
    }
}
