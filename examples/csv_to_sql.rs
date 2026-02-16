//! CSV file → SQL Server via Arrow
//!
//! Reads a CSV file into an Arrow RecordBatch, then bulk-loads it into SQL Server.
//!
//! Usage: cargo run --example csv_to_sql --features arrow

use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use claw::{AuthMethod, Config, connect};
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("active", DataType::Boolean, false),
    ]);

    let file = File::open("examples/data/sample.csv")?;
    let reader = ReaderBuilder::new(Arc::new(schema))
        .with_header(true)
        .build(file)?;

    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>()?;
    let batch = &batches[0];

    println!(
        "Read CSV → {} rows, {} columns",
        batch.num_rows(),
        batch.num_columns()
    );
    arrow::util::pretty::print_batches(&[batch.clone()])?;

    // Connect and load
    let mut config = Config::new();
    config.host("localhost");
    config.port(1433);
    config.authentication(AuthMethod::sql_server("sa", "TestPass123!"));
    config.trust_cert();

    let mut client = connect(config).await?;

    client
        .run(
            "IF OBJECT_ID('csv_products') IS NOT NULL DROP TABLE csv_products",
            &[],
        )
        .await?;
    client
        .run(
            "CREATE TABLE csv_products (
            id INT NOT NULL,
            name NVARCHAR(100),
            price FLOAT,
            quantity INT,
            active BIT
        )",
            &[],
        )
        .await?;

    let start = Instant::now();
    let rows = claw::bulk_write(&mut client, "csv_products", batch).await?;
    let elapsed = start.elapsed();

    println!("\nBulk-loaded {rows} rows in {elapsed:?}");

    // Verify
    let verify = claw::query_arrow(&mut client, "SELECT * FROM csv_products").await?;
    println!("\nSQL Server table:");
    arrow::util::pretty::print_batches(&[verify])?;

    Ok(())
}
