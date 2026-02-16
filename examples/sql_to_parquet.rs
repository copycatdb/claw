//! SQL Server → Parquet export via Arrow
//!
//! Queries SQL Server into Arrow, then writes to a Parquet file.
//!
//! Usage: cargo run --example sql_to_parquet --features arrow

use claw::{AuthMethod, Config, connect};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new();
    config.host("localhost");
    config.port(1433);
    config.authentication(AuthMethod::sql_server("sa", "TestPass123!"));
    config.trust_cert();

    let mut client = connect(config).await?;

    // Setup source data
    client
        .run(
            "IF OBJECT_ID('parquet_source') IS NOT NULL DROP TABLE parquet_source",
            &[],
        )
        .await?;
    client
        .run(
            "CREATE TABLE parquet_source (
            id INT NOT NULL,
            name NVARCHAR(100),
            value FLOAT
        )",
            &[],
        )
        .await?;
    client
        .run(
            "INSERT INTO parquet_source VALUES (1,'hello',3.14),(2,'world',2.72),(3,'test',1.41)",
            &[],
        )
        .await?;

    let start = Instant::now();

    let batch = claw::query_arrow(&mut client, "SELECT * FROM parquet_source").await?;
    println!("Query returned {} rows", batch.num_rows());
    arrow::util::pretty::print_batches(&[batch.clone()])?;

    // Write to Parquet
    let file = File::create("output.parquet")?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;

    let elapsed = start.elapsed();
    let size = std::fs::metadata("output.parquet")?.len();

    println!("\nExported to output.parquet ({size} bytes) in {elapsed:?}");

    Ok(())
}
