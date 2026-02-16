# claw 🐾

Idiomatic Rust API for SQL Server. Like [tokio-postgres](https://github.com/sfackler/rust-postgres), but sharper.

Part of [CopyCat](https://github.com/copycatdb) 🐱

## What is this?

A thin, ergonomic Rust API on top of [tabby](https://github.com/copycatdb/tabby). If tabby is the protocol engine, claw is the steering wheel.

```rust
use claw::{connect, AuthMethod, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new();
    config.host("localhost");
    config.port(1433);
    config.authentication(AuthMethod::sql_server("sa", "password"));
    config.trust_cert();

    let mut client = connect(config).await?;

    let stream = client.execute("SELECT @P1 AS greeting", &[&"hello"]).await?;
    let row = stream.into_row().await?.unwrap();
    let greeting: &str = row.get("greeting").unwrap();
    println!("{greeting}");

    Ok(())
}
```

## Arrow Integration

Enable the `arrow` feature for zero-copy columnar data exchange with SQL Server:

```toml
[dependencies]
claw = { git = "https://github.com/copycatdb/claw.git", features = ["arrow"] }
```

### Query → Arrow RecordBatch

```rust
let batch = claw::query_arrow(&mut client, "SELECT * FROM products").await?;
arrow::util::pretty::print_batches(&[batch])?;
// +----+--------+-------+
// | id | name   | price |
// +----+--------+-------+
// | 1  | Widget | 19.99 |
// | 2  | Gadget | 29.50 |
// +----+--------+-------+
```

### Arrow RecordBatch → Bulk Load

```rust
let rows = claw::bulk_write(&mut client, "target_table", &batch).await?;
println!("Loaded {rows} rows");
```

### ETL Pipelines

With Arrow as the universal in-memory format, claw becomes an ETL engine:

- **SQL → SQL**: `query_arrow` + `bulk_write` for server-to-server transfer
- **CSV → SQL**: `arrow::csv::ReaderBuilder` + `bulk_write`
- **Parquet → SQL**: `parquet::arrow::arrow_reader` + `bulk_write`
- **SQL → Parquet**: `query_arrow` + `parquet::arrow::ArrowWriter`
- **Transform**: Use `arrow::compute` (filter, cast, sort) between read and write

All transformations happen at columnar Arrow speed with zero serialization overhead.

See [`examples/`](examples/) for complete working samples.

## Why?

Because tiberius made you do all the work yourself — build config, create TCP stream, compat_write, handle TDS tokens manually. claw is what happens when a cat simplifies things.

## License

MIT
