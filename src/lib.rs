//! # claw
//!
//! A Rust client library for Microsoft SQL Server, built on top of the
//! [`tabby`] TDS 7.4+ wire protocol library.
//!
//! `claw` provides the high-level, user-facing API for connecting to SQL
//! Server, executing queries, and reading results. It re-exports the most
//! commonly needed types from `tabby` so that most users only need
//! `use claw::*`.
//!
//! # Quick Start
//!
//! ```no_run
//! use claw::{connect, AuthMethod, Config};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut config = Config::new();
//!     config.host("localhost");
//!     config.port(1433);
//!     config.authentication(AuthMethod::sql_server("sa", "your_password"));
//!     config.trust_cert();
//!
//!     let mut client = connect(config).await?;
//!
//!     let stream = client.execute("SELECT @P1 AS greeting", &[&"hello"]).await?;
//!     let row = stream.into_row().await?.unwrap();
//!     let greeting: &str = row.get("greeting").unwrap();
//!     println!("{greeting}");
//!
//!     Ok(())
//! }
//! ```
//!
//! # Architecture
//!
//! - **tabby** — pure wire protocol library (TDS packet encode/decode,
//!   authentication, `RowWriter` trait, column metadata, temporal types)
//! - **claw** — high-level client library (Client, Row, SqlValue, Query,
//!   ResultStream + re-exports of tabby wire types)

mod sql_value_writer;
pub use sql_value_writer::SqlValueWriter;

use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

// ── High-level types (owned by claw, defined in tabby internals) ─────

// Client & Query
pub use tabby::connection::Client;
pub use tabby::query::Query;

// Result streaming
pub use tabby::protocol::pipeline::{ResultItem, ResultSchema, ResultStream, ServerMessage};

// Row type
pub use tabby::row::Row;

// SqlValue
pub use tabby::protocol::wire::SqlValue;

// ── Re-exports from tabby (wire-protocol types) ─────────────────────

// Connection & configuration
pub use tabby::{AuthMethod, Config};

// Execution result
pub use tabby::ExecuteResult;

// Row & column types
pub use tabby::{Column, ColumnType};

// Type conversion traits
pub use tabby::{FromServer, FromServerOwned, IntoSql, IntoSqlOwned};

// Wire-level types users may need
pub use tabby::{
    BulkImport, ColumnAttribute, DataType, EncryptionLevel, FixedLenType, IntoRowMessage, Numeric,
    RowMessage, VarLenType,
};

// RowWriter trait (for advanced users building custom decoders)
pub use tabby::RowWriter;
pub use tabby::row_writer;

// Temporal types
pub use tabby::temporal;

// XML support
pub use tabby::xml;

// Error types
pub use tabby::error;

/// An alias for a result that holds tabby's error type.
pub type Result<T> = tabby::Result<T>;

/// The standard claw client type over a TCP connection.
pub type TcpClient = Client<tokio_util::compat::Compat<TcpStream>>;

/// Connect to SQL Server using the given configuration.
///
/// Handles TCP connection, nodelay, and Azure SQL Database routing
/// redirects automatically. Returns a ready-to-use client.
///
/// # Example
///
/// ```no_run
/// use claw::{connect, AuthMethod, Config};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut config = Config::new();
/// config.host("localhost");
/// config.port(1433);
/// config.authentication(AuthMethod::sql_server("sa", "password"));
/// config.trust_cert();
///
/// let mut client = connect(config).await?;
///
/// let rows = client.run("SELECT 1", &[]).await?;
/// println!("Rows affected: {}", rows.total());
/// # Ok(())
/// # }
/// ```
///
/// # Azure SQL Database
///
/// Azure uses gateway redirects. `connect` handles this transparently —
/// it follows the redirect and connects to the actual worker node.
///
/// ```no_run
/// # use claw::{connect, AuthMethod, Config};
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut config = Config::new();
/// config.host("myserver.database.windows.net");
/// config.authentication(AuthMethod::sql_server("user", "password"));
/// config.trust_cert();
///
/// let mut client = connect(config).await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect(config: Config) -> crate::Result<TcpClient> {
    Client::connect_with_redirect(config, |host, port| async move {
        let addr = format!("{}:{}", host, port);
        let tcp = TcpStream::connect(&addr)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        tcp.set_nodelay(true)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        Ok(tcp.compat_write())
    })
    .await
}
