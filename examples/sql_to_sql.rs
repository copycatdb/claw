//! SQL Server → SQL Server transfer via Arrow
//!
//! Reads from one table, writes to another using Arrow as the in-memory format.
//! Zero serialization overhead between read and write.
//!
//! Usage: cargo run --example sql_to_sql --features arrow

use claw::{AuthMethod, Config, connect};
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new();
    config.host("localhost");
    config.port(1433);
    config.authentication(AuthMethod::sql_server("sa", "TestPass123!"));
    config.trust_cert();

    let mut client = connect(config).await?;

    // Setup: create source table with data
    client
        .run(
            "IF OBJECT_ID('arrow_source') IS NOT NULL DROP TABLE arrow_source",
            &[],
        )
        .await?;
    client
        .run(
            "CREATE TABLE arrow_source (
                id INT NOT NULL,
                name NVARCHAR(100),
                price FLOAT,
                qty INT
            )",
            &[],
        )
        .await?;
    client
        .run(
            "INSERT INTO arrow_source VALUES
                (1, 'Alpha', 19.99, 100),
                (2, 'Beta', 29.50, 250),
                (3, 'Gamma', 9.99, 50)",
            &[],
        )
        .await?;

    // Create destination table
    client
        .run(
            "IF OBJECT_ID('arrow_dest') IS NOT NULL DROP TABLE arrow_dest",
            &[],
        )
        .await?;
    client
        .run(
            "CREATE TABLE arrow_dest (
                id INT NOT NULL,
                name NVARCHAR(100),
                price FLOAT,
                qty INT
            )",
            &[],
        )
        .await?;

    // ETL: query_arrow → bulk_write
    let start = Instant::now();

    let batch = claw::query_arrow(&mut client, "SELECT * FROM arrow_source").await?;
    println!(
        "Read {} rows, {} columns as Arrow RecordBatch",
        batch.num_rows(),
        batch.num_columns()
    );

    // Pretty-print the batch
    arrow::util::pretty::print_batches(&[batch.clone()])?;

    let rows = claw::bulk_write(&mut client, "arrow_dest", &batch).await?;
    let elapsed = start.elapsed();

    println!("\nTransferred {rows} rows in {elapsed:?}");

    // Verify
    let verify = claw::query_arrow(&mut client, "SELECT * FROM arrow_dest").await?;
    println!("\nDestination table:");
    arrow::util::pretty::print_batches(&[verify])?;

    Ok(())
}
