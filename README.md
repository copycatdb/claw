# claw 🐾

Idiomatic Rust API for SQL Server. Like [tokio-postgres](https://github.com/sfackler/rust-postgres), but sharper.

Part of [CopyCat](https://github.com/copycatdb) 🐱

## What is this?

A thin, ergonomic Rust API on top of [tabby](https://github.com/copycatdb/tabby). If tabby is the protocol engine, claw is the steering wheel.

```rust
use claw::Client;

#[tokio::main]
async fn main() -> Result<(), claw::Error> {
    let client = Client::connect(
        "Server=localhost,1433;UID=sa;PWD=pass;TrustServerCertificate=yes"
    ).await?;

    let rows = client.query("SELECT id, name FROM users WHERE id = @p1", &[&42i32]).await?;
    for row in rows {
        let name: &str = row.get("name");
        println!("{name}");
    }

    Ok(())
}
```

## Why?

Because tiberius made you do all the work yourself — build config, create TCP stream, compat_write, handle TDS tokens manually. claw is what happens when a cat simplifies things.

## Status

🚧 Coming soon. tabby is ready, claw is next.

## Attribution

Inspired by [tokio-postgres](https://github.com/sfackler/rust-postgres). Clean API, good docs, great vibes.

## License

MIT
