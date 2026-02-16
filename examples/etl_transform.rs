//! Full ETL pipeline with Arrow compute transformations
//!
//! Demonstrates: query_arrow → transform with arrow::compute → bulk_write.
//! All transformations happen at Arrow columnar speed, zero serialization.
//!
//! Usage: cargo run --example etl_transform --features arrow

use arrow::array::*;
use arrow::compute;
use arrow::record_batch::RecordBatch;
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

    // Setup source
    client
        .run(
            "IF OBJECT_ID('etl_source') IS NOT NULL DROP TABLE etl_source",
            &[],
        )
        .await?;
    client
        .run(
            "CREATE TABLE etl_source (
            id INT NOT NULL,
            product NVARCHAR(100),
            price FLOAT,
            qty INT
        )",
            &[],
        )
        .await?;
    client
        .run(
            "INSERT INTO etl_source VALUES
            (1, 'Widget', 10.0, 5),
            (2, 'Gadget', 25.0, 3),
            (3, 'Doohickey', 5.0, 20),
            (4, 'Thingamajig', 50.0, 1),
            (5, 'Whatsit', 15.0, 8)",
            &[],
        )
        .await?;

    // Destination for transformed data
    client
        .run(
            "IF OBJECT_ID('etl_dest') IS NOT NULL DROP TABLE etl_dest",
            &[],
        )
        .await?;
    client
        .run(
            "CREATE TABLE etl_dest (
            id INT NOT NULL,
            product NVARCHAR(100),
            price FLOAT,
            qty INT
        )",
            &[],
        )
        .await?;

    let start = Instant::now();

    // 1. Extract
    let batch = claw::query_arrow(&mut client, "SELECT * FROM etl_source").await?;
    println!("Source ({} rows):", batch.num_rows());
    arrow::util::pretty::print_batches(&[batch.clone()])?;

    // 2. Transform: filter rows where qty >= 5
    let qty_col = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let threshold = Int32Array::new_scalar(5);
    let mask = compute::kernels::cmp::gt_eq(qty_col, &threshold)?;

    let filtered = RecordBatch::try_new(
        batch.schema(),
        batch
            .columns()
            .iter()
            .map(|col| compute::filter(col.as_ref(), &mask).unwrap())
            .collect(),
    )?;

    println!("\nFiltered to {} rows (qty >= 5):", filtered.num_rows());
    arrow::util::pretty::print_batches(&[filtered.clone()])?;

    // 3. Load
    let rows = claw::bulk_write(&mut client, "etl_dest", &filtered).await?;
    let elapsed = start.elapsed();

    println!("\nETL complete: {rows} rows in {elapsed:?}");

    let verify = claw::query_arrow(&mut client, "SELECT * FROM etl_dest").await?;
    println!("\nDestination:");
    arrow::util::pretty::print_batches(&[verify])?;

    Ok(())
}
