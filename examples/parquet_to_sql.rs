//! Parquet file → SQL Server via Arrow
//!
//! Reads a Parquet file into Arrow, then bulk-loads into SQL Server.
//! Generates a sample Parquet file first if it doesn't exist.
//!
//! Usage: cargo run --example parquet_to_sql --features arrow

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use claw::{AuthMethod, Config, connect};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

fn generate_sample_parquet(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("temperature", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "NYC", "London", "Tokyo", "Sydney", "Berlin",
            ])),
            Arc::new(Float64Array::from(vec![22.5, 15.0, 28.3, 19.7, 12.1])),
        ],
    )?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    println!("Generated sample Parquet: {path}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let parquet_path = "examples/data/sample.parquet";
    if !std::path::Path::new(parquet_path).exists() {
        generate_sample_parquet(parquet_path)?;
    }

    // Read Parquet
    let file = File::open(parquet_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>()?;
    let batch = &batches[0];
    println!("Read Parquet → {} rows", batch.num_rows());
    arrow::util::pretty::print_batches(&[batch.clone()])?;

    // Load into SQL Server
    let mut config = Config::new();
    config.host("localhost");
    config.port(1433);
    config.authentication(AuthMethod::sql_server("sa", "TestPass123!"));
    config.trust_cert();

    let mut client = connect(config).await?;

    client
        .run(
            "IF OBJECT_ID('weather') IS NOT NULL DROP TABLE weather",
            &[],
        )
        .await?;
    client
        .run(
            "CREATE TABLE weather (id INT NOT NULL, city NVARCHAR(100), temperature FLOAT)",
            &[],
        )
        .await?;

    let start = Instant::now();
    let rows = claw::bulk_write(&mut client, "weather", batch).await?;
    let elapsed = start.elapsed();

    println!("\nBulk-loaded {rows} rows in {elapsed:?}");

    let verify = claw::query_arrow(&mut client, "SELECT * FROM weather").await?;
    println!("\nSQL Server table:");
    arrow::util::pretty::print_batches(&[verify])?;

    Ok(())
}
