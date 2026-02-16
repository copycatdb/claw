//! Arrow integration for claw — zero-copy query results as `RecordBatch`
//! and bulk write from `RecordBatch` into SQL Server via TDS Bulk Load.

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use tabby::Column;
use tabby::row_writer::RowWriter;

/// Maps a claw/tabby [`Column`] to an Arrow [`DataType`](arrow::datatypes::DataType).
fn column_to_arrow_type(col: &Column) -> DataType {
    use tabby::ColumnType::*;
    match col.column_type() {
        Bit | Bitn => DataType::Boolean,
        Int1 => DataType::UInt8,
        Int2 => DataType::Int16,
        Int4 => DataType::Int32,
        Int8 => DataType::Int64,
        Intn => {
            // Inspect type_info for actual width, default to Int64
            if let Some(tabby::DataType::VarLenSized(ctx)) = col.type_info() {
                match ctx.len() {
                    1 => DataType::UInt8,
                    2 => DataType::Int16,
                    4 => DataType::Int32,
                    _ => DataType::Int64,
                }
            } else {
                DataType::Int64
            }
        }
        Float4 => DataType::Float32,
        Float8 => DataType::Float64,
        Floatn => {
            if let Some(tabby::DataType::VarLenSized(ctx)) = col.type_info() {
                if ctx.len() <= 4 {
                    DataType::Float32
                } else {
                    DataType::Float64
                }
            } else {
                DataType::Float64
            }
        }
        Decimaln | Numericn => {
            if let Some(tabby::DataType::VarLenSizedPrecision {
                precision, scale, ..
            }) = col.type_info()
            {
                DataType::Decimal128(*precision, *scale as i8)
            } else {
                DataType::Decimal128(38, 6)
            }
        }
        Money | Money4 => DataType::Decimal128(19, 4),
        BigVarChar | BigChar | NVarchar | NChar | Text | NText => DataType::Utf8,
        BigVarBin | BigBinary | Image => DataType::Binary,
        Daten => DataType::Date32,
        Timen => DataType::Time64(TimeUnit::Nanosecond),
        Datetime | Datetime4 | Datetimen | Datetime2 => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        DatetimeOffsetn => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        Guid => DataType::Utf8,
        Xml => DataType::Utf8,
        _ => DataType::Utf8, // fallback
    }
}

/// Create an appropriate [`ArrayBuilder`] for a given Arrow data type.
fn make_builder(dt: &DataType) -> Box<dyn ArrayBuilder> {
    match dt {
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::UInt8 => Box::new(UInt8Builder::new()),
        DataType::Int16 => Box::new(Int16Builder::new()),
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::Float32 => Box::new(Float32Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Decimal128(p, s) => {
            Box::new(Decimal128Builder::new().with_data_type(DataType::Decimal128(*p, *s)))
        }
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::Binary => Box::new(BinaryBuilder::new()),
        DataType::Date32 => Box::new(Date32Builder::new()),
        DataType::Time64(TimeUnit::Nanosecond) => Box::new(Time64NanosecondBuilder::new()),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => Box::new(
            TimestampMicrosecondBuilder::new()
                .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, tz.clone())),
        ),
        _ => Box::new(StringBuilder::new()), // fallback
    }
}

/// A [`RowWriter`] that collects TDS row data directly into Arrow columnar arrays.
///
/// Usage:
/// 1. Create with `ArrowRowWriter::new()`
/// 2. Pass to `client.batch_into(sql, &mut writer)`
/// 3. Call `writer.finish()` to get a `RecordBatch`
pub struct ArrowRowWriter {
    schema: Option<Arc<Schema>>,
    builders: Vec<Box<dyn ArrayBuilder>>,
    row_count: usize,
    current_col: usize,
}

impl ArrowRowWriter {
    /// Create a new empty Arrow row writer.
    pub fn new() -> Self {
        Self {
            schema: None,
            builders: Vec::new(),
            row_count: 0,
            current_col: 0,
        }
    }

    /// Consume the writer and produce a `RecordBatch`.
    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let schema = self
            .schema
            .clone()
            .ok_or_else(|| ArrowError::InvalidArgumentError("no metadata received".into()))?;

        let arrays: Vec<ArrayRef> = self.builders.iter_mut().map(|b| b.finish()).collect();

        RecordBatch::try_new(schema, arrays)
    }
}

impl Default for ArrowRowWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl RowWriter for ArrowRowWriter {
    fn on_metadata(&mut self, columns: &[Column]) {
        let fields: Vec<Field> = columns
            .iter()
            .map(|c| {
                let dt = column_to_arrow_type(c);
                let nullable = c.nullable().unwrap_or(true);
                Field::new(c.name(), dt, nullable)
            })
            .collect();

        let schema = Arc::new(Schema::new(fields.clone()));
        self.builders = fields.iter().map(|f| make_builder(f.data_type())).collect();
        self.schema = Some(schema);
        self.row_count = 0;
        self.current_col = 0;
    }

    fn write_null(&mut self, _col: usize) {
        let idx = self.current_col;
        self.current_col += 1;
        if idx < self.builders.len() {
            // Append null to the appropriate builder type
            let builder = &mut self.builders[idx];
            let dt = self.schema.as_ref().unwrap().field(idx).data_type().clone();
            append_null(builder.as_mut(), &dt);
        }
    }

    fn write_bool(&mut self, _col: usize, val: bool) {
        let idx = self.current_col;
        self.current_col += 1;
        if idx < self.builders.len() {
            self.builders[idx]
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap()
                .append_value(val);
        }
    }

    fn write_u8(&mut self, _col: usize, val: u8) {
        let idx = self.current_col;
        self.current_col += 1;
        if idx < self.builders.len() {
            let b = &mut self.builders[idx];
            // Could be UInt8 or Int16/Int32/Int64 depending on target
            if let Some(b) = b.as_any_mut().downcast_mut::<UInt8Builder>() {
                b.append_value(val);
            } else if let Some(b) = b.as_any_mut().downcast_mut::<Int16Builder>() {
                b.append_value(val as i16);
            } else if let Some(b) = b.as_any_mut().downcast_mut::<Int32Builder>() {
                b.append_value(val as i32);
            } else if let Some(b) = b.as_any_mut().downcast_mut::<Int64Builder>() {
                b.append_value(val as i64);
            }
        }
    }

    fn write_i16(&mut self, _col: usize, val: i16) {
        let idx = self.current_col;
        self.current_col += 1;
        if idx < self.builders.len() {
            let b = &mut self.builders[idx];
            if let Some(b) = b.as_any_mut().downcast_mut::<Int16Builder>() {
                b.append_value(val);
            } else if let Some(b) = b.as_any_mut().downcast_mut::<Int32Builder>() {
                b.append_value(val as i32);
            } else if let Some(b) = b.as_any_mut().downcast_mut::<Int64Builder>() {
                b.append_value(val as i64);
            }
        }
    }

    fn write_i32(&mut self, _col: usize, val: i32) {
        let idx = self.current_col;
        self.current_col += 1;
        if idx < self.builders.len() {
            let b = &mut self.builders[idx];
            if let Some(b) = b.as_any_mut().downcast_mut::<Int32Builder>() {
                b.append_value(val);
            } else if let Some(b) = b.as_any_mut().downcast_mut::<Int64Builder>() {
                b.append_value(val as i64);
            }
        }
    }

    fn write_i64(&mut self, _col: usize, val: i64) {
        let idx = self.current_col;
        self.current_col += 1;
        if idx < self.builders.len() {
            self.builders[idx]
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .unwrap()
                .append_value(val);
        }
    }

    fn write_f32(&mut self, _col: usize, val: f32) {
        let idx = self.current_col;
        self.current_col += 1;
        if idx < self.builders.len() {
            let b = &mut self.builders[idx];
            if let Some(b) = b.as_any_mut().downcast_mut::<Float32Builder>() {
                b.append_value(val);
            } else if let Some(b) = b.as_any_mut().downcast_mut::<Float64Builder>() {
                b.append_value(val as f64);
            }
        }
    }

    fn write_f64(&mut self, _col: usize, val: f64) {
        let idx = self.current_col;
        self.current_col += 1;
        if idx < self.builders.len() {
            let b = &mut self.builders[idx];
            if let Some(b) = b.as_any_mut().downcast_mut::<Float64Builder>() {
                b.append_value(val);
            } else if let Some(b) = b.as_any_mut().downcast_mut::<Float32Builder>() {
                b.append_value(val as f32);
            }
        }
    }

    fn write_str(&mut self, _col: usize, val: &str) {
        let idx = self.current_col;
        self.current_col += 1;
        if idx < self.builders.len() {
            self.builders[idx]
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap()
                .append_value(val);
        }
    }

    fn write_bytes(&mut self, _col: usize, val: &[u8]) {
        let idx = self.current_col;
        self.current_col += 1;
        if idx < self.builders.len() {
            self.builders[idx]
                .as_any_mut()
                .downcast_mut::<BinaryBuilder>()
                .unwrap()
                .append_value(val);
        }
    }

    fn write_date(&mut self, _col: usize, days: i32) {
        let idx = self.current_col;
        self.current_col += 1;
        if idx < self.builders.len() {
            self.builders[idx]
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .unwrap()
                .append_value(days);
        }
    }

    fn write_time(&mut self, _col: usize, nanos: i64) {
        let idx = self.current_col;
        self.current_col += 1;
        if idx < self.builders.len() {
            self.builders[idx]
                .as_any_mut()
                .downcast_mut::<Time64NanosecondBuilder>()
                .unwrap()
                .append_value(nanos);
        }
    }

    fn write_datetime(&mut self, _col: usize, micros: i64) {
        let idx = self.current_col;
        self.current_col += 1;
        if idx < self.builders.len() {
            self.builders[idx]
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap()
                .append_value(micros);
        }
    }

    fn write_datetimeoffset(&mut self, _col: usize, micros: i64, _offset_minutes: i16) {
        let idx = self.current_col;
        self.current_col += 1;
        if idx < self.builders.len() {
            self.builders[idx]
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap()
                .append_value(micros);
        }
    }

    fn write_decimal(&mut self, _col: usize, value: i128, _precision: u8, _scale: u8) {
        let idx = self.current_col;
        self.current_col += 1;
        if idx < self.builders.len() {
            self.builders[idx]
                .as_any_mut()
                .downcast_mut::<Decimal128Builder>()
                .unwrap()
                .append_value(value);
        }
    }

    fn write_guid(&mut self, _col: usize, bytes: &[u8; 16]) {
        let idx = self.current_col;
        self.current_col += 1;
        if idx < self.builders.len() {
            let uuid = uuid::Uuid::from_bytes(*bytes);
            self.builders[idx]
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap()
                .append_value(uuid.to_string());
        }
    }

    fn on_row_done(&mut self) {
        self.row_count += 1;
        self.current_col = 0;
    }
}

fn append_null(builder: &mut dyn ArrayBuilder, dt: &DataType) {
    match dt {
        DataType::Boolean => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .unwrap()
            .append_null(),
        DataType::UInt8 => builder
            .as_any_mut()
            .downcast_mut::<UInt8Builder>()
            .unwrap()
            .append_null(),
        DataType::Int16 => builder
            .as_any_mut()
            .downcast_mut::<Int16Builder>()
            .unwrap()
            .append_null(),
        DataType::Int32 => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .unwrap()
            .append_null(),
        DataType::Int64 => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .unwrap()
            .append_null(),
        DataType::Float32 => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .unwrap()
            .append_null(),
        DataType::Float64 => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .unwrap()
            .append_null(),
        DataType::Decimal128(_, _) => builder
            .as_any_mut()
            .downcast_mut::<Decimal128Builder>()
            .unwrap()
            .append_null(),
        DataType::Utf8 => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .append_null(),
        DataType::Binary => builder
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .unwrap()
            .append_null(),
        DataType::Date32 => builder
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .unwrap()
            .append_null(),
        DataType::Time64(TimeUnit::Nanosecond) => builder
            .as_any_mut()
            .downcast_mut::<Time64NanosecondBuilder>()
            .unwrap()
            .append_null(),
        DataType::Timestamp(TimeUnit::Microsecond, _) => builder
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .unwrap()
            .append_null(),
        _ => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .append_null(),
    }
}

/// Execute a SQL query and return results as an Arrow `RecordBatch`.
pub async fn query_arrow(client: &mut crate::TcpClient, sql: &str) -> crate::Result<RecordBatch> {
    let mut writer = ArrowRowWriter::new();
    client.batch_into(sql, &mut writer).await?;
    writer
        .finish()
        .map_err(|e| tabby::error::Error::Conversion(e.to_string().into()))
}

/// Bulk-write an Arrow `RecordBatch` into a SQL Server table via TDS Bulk Load.
///
/// The target table must already exist with compatible columns.
/// Returns the number of rows written.
pub async fn bulk_write(
    client: &mut crate::TcpClient,
    table: &str,
    batch: &RecordBatch,
) -> crate::Result<u64> {
    use tabby::protocol::wire::SqlValue;

    let mut bulk = client.bulk_insert(table).await?;

    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();

    for row_idx in 0..num_rows {
        let mut row = tabby::RowMessage::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let col = batch.column(col_idx);
            if col.is_null(row_idx) {
                row.push(SqlValue::I32(None));
                continue;
            }
            let val = array_value_to_sql(col.as_ref(), row_idx);
            row.push(val);
        }
        bulk.send(row).await?;
    }

    let result = bulk.finalize().await?;
    Ok(result.total())
}

fn array_value_to_sql(array: &dyn Array, idx: usize) -> tabby::protocol::wire::SqlValue<'static> {
    use std::borrow::Cow;
    use tabby::protocol::wire::SqlValue;

    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            SqlValue::Bit(Some(a.value(idx)))
        }
        DataType::UInt8 => {
            let a = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            SqlValue::U8(Some(a.value(idx)))
        }
        DataType::Int16 => {
            let a = array.as_any().downcast_ref::<Int16Array>().unwrap();
            SqlValue::I16(Some(a.value(idx)))
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            SqlValue::I32(Some(a.value(idx)))
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            SqlValue::I64(Some(a.value(idx)))
        }
        DataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>().unwrap();
            SqlValue::F32(Some(a.value(idx)))
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            SqlValue::F64(Some(a.value(idx)))
        }
        DataType::Decimal128(_, scale) => {
            let a = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let val = a.value(idx);
            SqlValue::Numeric(Some(tabby::Numeric::new_with_scale(val, *scale as u8)))
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            SqlValue::String(Some(Cow::Owned(a.value(idx).to_string())))
        }
        DataType::Binary => {
            let a = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            SqlValue::Binary(Some(Cow::Owned(a.value(idx).to_vec())))
        }
        DataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>().unwrap();
            SqlValue::Date(Some(tabby::temporal::Date::new(a.value(idx) as u32)))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let a = array
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .unwrap();
            let nanos = a.value(idx);
            let increments = nanos as u64 / 100;
            SqlValue::Time(Some(tabby::temporal::Time::new(increments, 7)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let micros = a.value(idx);
            if tz.is_some() {
                // Convert micros to Date + Time components
                let total_secs = micros / 1_000_000;
                let frac_micros = (micros % 1_000_000).unsigned_abs();
                let days_from_epoch = (total_secs / 86400) as i32;
                // TDS date epoch is 0001-01-01, Unix epoch is 1970-01-01 = day 719162
                let tds_days = (days_from_epoch + 719_162) as u32;
                let time_of_day_secs = (total_secs % 86400).unsigned_abs();
                let increments = (time_of_day_secs * 10_000_000) + (frac_micros * 10);
                let date = tabby::temporal::Date::new(tds_days);
                let time = tabby::temporal::Time::new(increments, 7);
                let dt2 = tabby::temporal::DateTime2::new(date, time);
                SqlValue::DateTimeOffset(Some(tabby::temporal::DateTimeOffset::new(dt2, 0)))
            } else {
                let total_secs = micros / 1_000_000;
                let frac_micros = (micros % 1_000_000).unsigned_abs();
                let days_from_epoch = (total_secs / 86400) as i32;
                let tds_days = (days_from_epoch + 719_162) as u32;
                let time_of_day_secs = (total_secs % 86400).unsigned_abs();
                let increments = (time_of_day_secs * 10_000_000) + (frac_micros * 10);
                let date = tabby::temporal::Date::new(tds_days);
                let time = tabby::temporal::Time::new(increments, 7);
                SqlValue::DateTime2(Some(tabby::temporal::DateTime2::new(date, time)))
            }
        }
        _ => SqlValue::I32(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arrow_row_writer_basic() {
        let mut writer = ArrowRowWriter::new();

        // Simulate metadata
        let columns = vec![
            Column::new("id".to_string(), tabby::ColumnType::Int4),
            Column::new("name".to_string(), tabby::ColumnType::NVarchar),
            Column::new("active".to_string(), tabby::ColumnType::Bit),
        ];
        writer.on_metadata(&columns);

        // Row 1
        writer.write_i32(0, 1);
        writer.write_str(1, "Alice");
        writer.write_bool(2, true);
        writer.on_row_done();

        // Row 2
        writer.write_i32(0, 2);
        writer.write_str(1, "Bob");
        writer.write_bool(2, false);
        writer.on_row_done();

        let batch = writer.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);

        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Bob");
    }

    #[test]
    fn test_arrow_row_writer_nulls() {
        let mut writer = ArrowRowWriter::new();

        let columns = vec![
            Column::new("id".to_string(), tabby::ColumnType::Int4),
            Column::new("val".to_string(), tabby::ColumnType::NVarchar),
        ];
        writer.on_metadata(&columns);

        writer.write_i32(0, 1);
        writer.write_null(1);
        writer.on_row_done();

        writer.write_null(0);
        writer.write_str(1, "hello");
        writer.on_row_done();

        let batch = writer.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(!ids.is_null(0));
        assert!(ids.is_null(1));

        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(vals.is_null(0));
        assert_eq!(vals.value(1), "hello");
    }
}
