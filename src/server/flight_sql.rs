use std::sync::Arc;

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use deltalake::{datafusion::prelude::SessionContext, open_table};
use tonic::Status;

use crate::server::DeltaFlightServer;

impl DeltaFlightServer {
    
    /// Execute a SQL query against the registered Delta tables
    pub async fn execute_sql_query(
        &self,
        query: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), Status> {
        // Create DataFusion session context
        let ctx = SessionContext::new();

        // Register all Delta tables with DataFusion
        let tables = self.tables.lock().await;
        for (table_name, table_path) in tables.iter() {
            // open the Delta table
            let delta_table = open_table(table_path.to_string_lossy())
                .await
                .map_err(|e| Status::internal(format!("Failed to open table {}: {}", table_name, e)))?;

            // Register the table with DataFusion
            ctx.register_table(table_name, Arc::new(delta_table))
                .map_err(|e| Status::internal(format!("Failed to register table {}: {}", table_name, e)))?;
        }
        drop(tables);

        // Execute the SQL query
        let df = ctx.sql(query)
            .await
            .map_err(|e| Status::internal(format!("SQL execution error: {}", e)))?;

        // Get the schema - convert DFSchema to Arrow Schema
        let df_schema = df.schema();
        let arrow_schema = df_schema.inner().clone();

        // Collect the results
        let batches = df.collect()
            .await
            .map_err(|e| Status::internal(format!("Error collecting query resultss: {}", e)))?;

        Ok((arrow_schema, batches))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, sync::Arc};
    use arrow::{
        array::{ArrayRef, Int32Array, StringArray, Decimal128Builder},
        datatypes::{DataType, Field, Schema},
    };
    use deltalake::DeltaOps;
    use tempfile::tempdir;
    use tokio::sync::Mutex;
    use crate::deltaflight::{ColumnSchema, TableCreateRequest};
    use crate::deltaflight::delta_flight_service_server::DeltaFlightService;
    use tonic::Request;

    #[tokio::test]
    async fn test_execute_sql_query() {
        // Setup
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let server = DeltaFlightServer {
            tables: Arc::new(Mutex::new(HashMap::new())),
            data_dir: temp_dir.path().to_path_buf(),
        };

        // 1. Create two test tables: sales and products
        // Create sales table
        let sales_columns = vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                nullable: false,
            },
            ColumnSchema {
                name: "product_id".to_string(),
                data_type: "integer".to_string(),
                nullable: false,
            },
            ColumnSchema {
                name: "quantity".to_string(),
                data_type: "integer".to_string(),
                nullable: false,
            },
            ColumnSchema {
                name: "amount".to_string(),
                data_type: "decimal(10,2)".to_string(),
                nullable: false,
            },
        ];

        let sales_table_name = "sales";
        let request = Request::new(TableCreateRequest {
            table_path: sales_table_name.to_string(),
            columns: sales_columns,
        });

        let _ = server
            .create_table(request)
            .await
            .expect("Sales table creation failed");

        // Create products table
        let products_columns = vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                data_type: "string".to_string(),
                nullable: false,
            },
            ColumnSchema {
                name: "price".to_string(),
                data_type: "decimal(10,2)".to_string(),
                nullable: false,
            },
        ];

        let products_table_name = "products";
        let request = Request::new(TableCreateRequest {
            table_path: products_table_name.to_string(),
            columns: products_columns,
        });

        let _ = server
            .create_table(request)
            .await
            .expect("Products table creation failed");

        // 2. Insert data into the tables
        // Insert data into sales table
        let sales_path = {
            let tables = server.tables.lock().await;
            tables.get(sales_table_name).unwrap().clone()
        };

        let sales_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("product_id", DataType::Int32, false),
            Field::new("quantity", DataType::Int32, false),
            Field::new("amount", DataType::Decimal128(10, 2), false),
        ]));

        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let product_id_array: ArrayRef = Arc::new(Int32Array::from(vec![101, 102, 101, 103, 102]));
        let quantity_array: ArrayRef = Arc::new(Int32Array::from(vec![2, 1, 3, 2, 5]));

        let mut amount_builder = Decimal128Builder::with_capacity(5)
            .with_precision_and_scale(10, 2)
            .unwrap();
        for value in [1999, 3450, 2999, 1675, 8625] {
            amount_builder.append_value(value);
        }
        let amount_array: ArrayRef = Arc::new(amount_builder.finish());

        let sales_batch = RecordBatch::try_new(
            sales_schema.clone(),
            vec![id_array, product_id_array, quantity_array, amount_array],
        ).expect("Failed to create sales record batch");

        let ops = DeltaOps::try_from_uri(sales_path.to_string_lossy())
            .await
            .expect("Failed to open Delta table");
        ops.write(vec![sales_batch.clone()])
            .await
            .expect("Failed to write to sales table");

        // Insert data into products table
        let products_path = {
            let tables = server.tables.lock().await;
            tables.get(products_table_name).unwrap().clone()
        };

        let products_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Decimal128(10, 2), false),
        ]));

        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![101, 102, 103]));
        let name_array: ArrayRef = Arc::new(StringArray::from(vec!["Widget", "Gadget", "Tool"]));

        let mut price_builder = Decimal128Builder::with_capacity(3)
            .with_precision_and_scale(10, 2)
            .unwrap();
        for value in [999, 3450, 1675] {
            price_builder.append_value(value);
        }
        let price_array: ArrayRef = Arc::new(price_builder.finish());

        let products_batch = RecordBatch::try_new(
            products_schema.clone(),
            vec![id_array, name_array, price_array],
        ).expect("Failed to create products record batch");

        let ops = DeltaOps::try_from_uri(products_path.to_string_lossy())
            .await
            .expect("Failed to open Delta table");
        ops.write(vec![products_batch.clone()])
            .await
            .expect("Failed to write to products table");

        // 3. Execute several SQL queries and check results
        
        // Simple SELECT query
        let sql = "SELECT * FROM sales WHERE product_id = 101";
        let (schema, batches) = server.execute_sql_query(sql).await.expect("Query failed");
        
        // Verify we got 2 rows (the sales for product_id 101)
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "Expected 2 rows for product_id 101");
        
        // Verify schema has 4 columns
        assert_eq!(schema.fields().len(), 4);
        
        // JOIN query
        let join_sql = "SELECT s.id as sale_id, p.name as product_name, s.quantity, s.amount 
                        FROM sales s
                        JOIN products p ON s.product_id = p.id
                        ORDER BY s.amount DESC";
        
        let (join_schema, join_batches) = server.execute_sql_query(join_sql).await.expect("Join query failed");
        
        // Verify we got all 5 rows
        let total_join_rows: usize = join_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_join_rows, 5, "Expected 5 rows from join query");
        
        // Verify join schema has the correct columns
        assert_eq!(join_schema.fields().len(), 4);
        assert_eq!(join_schema.field(0).name(), "sale_id");
        assert_eq!(join_schema.field(1).name(), "product_name");
        
        // Verify the first row is the one with the highest amount (should be 5 Gadgets with amount 8625)
        if !join_batches.is_empty() && join_batches[0].num_rows() > 0 {
            let product_name_array = join_batches[0]
                .column_by_name("product_name")
                .expect("Missing product_name column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray");
                
            let quantity_array = join_batches[0]
                .column_by_name("quantity")
                .expect("Missing quantity column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Expected Int32Array");
                
            assert_eq!(product_name_array.value(0), "Gadget");
            assert_eq!(quantity_array.value(0), 5);
        }
        
        // Test aggregate query
        let agg_sql = "SELECT p.name, SUM(s.quantity) as total_quantity, SUM(s.amount) as total_amount
                       FROM sales s
                       JOIN products p ON s.product_id = p.id
                       GROUP BY p.name
                       ORDER BY total_amount DESC";
                       
        let (agg_schema, agg_batches) = server.execute_sql_query(agg_sql).await.expect("Aggregate query failed");
        
        // Verify we got 3 rows (one for each product)
        let total_agg_rows: usize = agg_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_agg_rows, 3, "Expected 3 rows from aggregate query");
        
        // Verify the schema has the right columns
        assert_eq!(agg_schema.fields().len(), 3);
        assert_eq!(agg_schema.field(0).name(), "name");
        assert_eq!(agg_schema.field(1).name(), "total_quantity");
        assert_eq!(agg_schema.field(2).name(), "total_amount");
    }
    
    #[tokio::test]
    async fn test_execute_sql_query_errors() {
        // Setup
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let server = DeltaFlightServer {
            tables: Arc::new(Mutex::new(HashMap::new())),
            data_dir: temp_dir.path().to_path_buf(),
        };
        
        // Test with non-existent table
        let sql = "SELECT * FROM nonexistent_table";
        let result = server.execute_sql_query(sql).await;
        
        assert!(result.is_err(), "Query should fail with non-existent table");
        
        // Create a table for further tests
        let columns = vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                data_type: "string".to_string(),
                nullable: false,
            },
        ];
        
        let table_name = "test_table";
        let request = Request::new(TableCreateRequest {
            table_path: table_name.to_string(),
            columns,
        });
        
        let _ = server
            .create_table(request)
            .await
            .expect("Table creation failed");
        
        // Test with syntax error in SQL
        let bad_sql = "SELEC * FROM test_table"; // Missing column list
        let result = server.execute_sql_query(bad_sql).await;
        
        assert!(result.is_err(), "Query with syntax error should fail");
        
        // Test with invalid column name
        let bad_column_sql = "SELECT nonexistent_column FROM test_table";
        let result = server.execute_sql_query(bad_column_sql).await;
        
        assert!(result.is_err(), "Query with invalid column should fail");
    }
}