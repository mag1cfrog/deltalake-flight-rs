use deltalake::{kernel::StructField, DeltaOps};
use tonic::{Request, Response, Status};

use crate::{
    deltaflight::{TableCreateRequest, TableCreateResponse},
    server::DeltaFlightServer,
    utils::data_type_parser::parse_data_type,
};

pub async fn create_table(
    server: &DeltaFlightServer,
    request: Request<TableCreateRequest>,
) -> Result<Response<TableCreateResponse>, Status> {
    let req = request.into_inner();

    // Convert protobuf schema to Delta Lake schema
    let fields = req
        .columns
        .iter()
        .map(|c| -> Result<StructField, Status> {
            Ok(StructField::new(
                c.name.clone(),
                parse_data_type(&c.data_type).map_err(|e| Status::internal(e.to_string()))?,
                c.nullable,
            ))
        })
        .collect::<Result<Vec<_>, Status>>()?;

    let table_path = server.data_dir.join(&req.table_path);
    let _ = DeltaOps::try_from_uri(table_path.to_string_lossy().to_string())
        .await
        .map_err(|e| Status::internal(e.to_string()))? // Convert DeltaTableError to Status
        .create()
        .with_columns(fields)
        .await
        .map_err(|e| Status::internal(e.to_string()))?; // Convert DeltaTableError to Status;

    server
        .tables
        .lock()
        .await
        .insert(req.table_path, table_path.clone());

    Ok(Response::new(TableCreateResponse {
        success: true,
        message: format!(
            "Table {} created successfully",
            table_path.to_string_lossy()
        ),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deltaflight::delta_flight_service_server::DeltaFlightService;
    use crate::deltaflight::ColumnSchema;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_create_table() {
        // Setup
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let server = DeltaFlightServer {
            tables: Arc::new(Mutex::new(HashMap::new())),
            data_dir: temp_dir.path().to_path_buf(),
        };

        // Create a simple table schema
        let columns = vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                data_type: "string".to_string(),
                nullable: true,
            },
            ColumnSchema {
                name: "amount".to_string(),
                data_type: "decimal(10,2)".to_string(),
                nullable: true,
            },
        ];

        // Create the request
        let request = Request::new(TableCreateRequest {
            table_path: "test_table".to_string(),
            columns,
        });

        // Call the function
        let response = server
            .create_table(request)
            .await
            .expect("Table creation failed");
        let response_inner = response.into_inner();

        // Validate the response
        assert!(response_inner.success);
        assert!(response_inner.message.contains("created successfully"));

        // Verify the table exists in the in-memory map
        let tables = server.tables.lock().await;
        assert!(tables.contains_key("test_table"));

        // Check that the Delta table files were actually created
        let table_path = tables.get("test_table").unwrap();
        assert!(table_path.exists());

        // Verify that the _delta_log directory was created (a sign that it's a valid Delta table)
        let delta_log_path = table_path.join("_delta_log");
        assert!(delta_log_path.exists());
    }

    #[tokio::test]
    async fn test_create_table_with_invalid_type() {
        // Setup
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let server = DeltaFlightServer {
            tables: Arc::new(Mutex::new(HashMap::new())),
            data_dir: temp_dir.path().to_path_buf(),
        };

        // Create an invalid schema (uuid type is not supported)
        let columns = vec![ColumnSchema {
            name: "id".to_string(),
            data_type: "uuid".to_string(), // Invalid type
            nullable: false,
        }];

        let request = Request::new(TableCreateRequest {
            table_path: "invalid_table".to_string(),
            columns,
        });

        // This should return an error
        let result = server.create_table(request).await;
        assert!(result.is_err());

        // Check that the error message mentions the unsupported type
        if let Err(status) = result {
            assert!(status.message().contains("Unsupported data type"));
        }
    }
}
