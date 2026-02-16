//! Integration tests for Arrow support (requires running SQL Server)

#![cfg(feature = "arrow")]

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use claw::{AuthMethod, Config, connect};
use std::sync::Arc;

async fn test_client() -> claw::TcpClient {
    let mut config = Config::new();
    config.host("localhost");
    config.port(1433);
    config.authentication(AuthMethod::sql_server("sa", "TestPass123!"));
    config.trust_cert();
    connect(config).await.unwrap()
}

#[tokio::test]
async fn test_query_arrow_basic() {
    let mut client = test_client().await;

    client
        .run(
            "IF OBJECT_ID('test_arrow_basic') IS NOT NULL DROP TABLE test_arrow_basic",
            &[],
        )
        .await
        .unwrap();
    client
        .run(
            "CREATE TABLE test_arrow_basic (id INT, name NVARCHAR(50), val FLOAT)",
            &[],
        )
        .await
        .unwrap();
    client
        .run(
            "INSERT INTO test_arrow_basic VALUES (1, 'hello', 3.14), (2, 'world', 2.72)",
            &[],
        )
        .await
        .unwrap();

    let batch = claw::query_arrow(&mut client, "SELECT * FROM test_arrow_basic ORDER BY id")
        .await
        .unwrap();

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
    assert_eq!(names.value(0), "hello");
    assert_eq!(names.value(1), "world");
}

#[tokio::test]
async fn test_query_arrow_nulls() {
    let mut client = test_client().await;

    client
        .run(
            "IF OBJECT_ID('test_arrow_nulls') IS NOT NULL DROP TABLE test_arrow_nulls",
            &[],
        )
        .await
        .unwrap();
    client
        .run(
            "CREATE TABLE test_arrow_nulls (id INT, val NVARCHAR(50))",
            &[],
        )
        .await
        .unwrap();
    client
        .run(
            "INSERT INTO test_arrow_nulls VALUES (1, NULL), (NULL, 'hi')",
            &[],
        )
        .await
        .unwrap();

    let batch = claw::query_arrow(&mut client, "SELECT * FROM test_arrow_nulls ORDER BY id")
        .await
        .unwrap();

    assert_eq!(batch.num_rows(), 2);

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(ids.is_null(0) || ids.value(0) == 1); // ORDER BY with NULL

    let vals = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // One should be null, one should be "hi"
    assert!(vals.is_null(0) || vals.is_null(1));
}

#[tokio::test]
async fn test_bulk_write_basic() {
    let mut client = test_client().await;

    client
        .run(
            "IF OBJECT_ID('test_bulk_basic') IS NOT NULL DROP TABLE test_bulk_basic",
            &[],
        )
        .await
        .unwrap();
    client
        .run(
            "CREATE TABLE test_bulk_basic (id INT NOT NULL, name NVARCHAR(100), score FLOAT)",
            &[],
        )
        .await
        .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3])),
        ],
    )
    .unwrap();

    let rows = claw::bulk_write(&mut client, "test_bulk_basic", &batch)
        .await
        .unwrap();
    assert_eq!(rows, 3);

    let result = claw::query_arrow(&mut client, "SELECT * FROM test_bulk_basic ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 3);

    let ids = result
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 10);
    assert_eq!(ids.value(2), 30);
}

#[tokio::test]
async fn test_roundtrip() {
    let mut client = test_client().await;

    // Create and populate source
    client
        .run(
            "IF OBJECT_ID('test_rt_src') IS NOT NULL DROP TABLE test_rt_src",
            &[],
        )
        .await
        .unwrap();
    client
        .run(
            "CREATE TABLE test_rt_src (id INT NOT NULL, txt NVARCHAR(100), num FLOAT)",
            &[],
        )
        .await
        .unwrap();
    client
        .run(
            "INSERT INTO test_rt_src VALUES (1,'foo',1.5),(2,'bar',2.5),(3,'baz',3.5)",
            &[],
        )
        .await
        .unwrap();

    // Read
    let batch = claw::query_arrow(&mut client, "SELECT * FROM test_rt_src ORDER BY id")
        .await
        .unwrap();
    assert_eq!(batch.num_rows(), 3);

    // Write to dest
    client
        .run(
            "IF OBJECT_ID('test_rt_dst') IS NOT NULL DROP TABLE test_rt_dst",
            &[],
        )
        .await
        .unwrap();
    client
        .run(
            "CREATE TABLE test_rt_dst (id INT NOT NULL, txt NVARCHAR(100), num FLOAT)",
            &[],
        )
        .await
        .unwrap();
    claw::bulk_write(&mut client, "test_rt_dst", &batch)
        .await
        .unwrap();

    // Read back and compare
    let batch2 = claw::query_arrow(&mut client, "SELECT * FROM test_rt_dst ORDER BY id")
        .await
        .unwrap();
    assert_eq!(batch2.num_rows(), 3);

    let ids1 = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let ids2 = batch2
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids1.values(), ids2.values());

    let txt1 = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let txt2 = batch2
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..3 {
        assert_eq!(txt1.value(i), txt2.value(i));
    }
}
