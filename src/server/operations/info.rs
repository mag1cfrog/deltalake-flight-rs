use deltalake::DeltaOps;
use tonic::{Request, Response, Status};

use crate::deltaflight::{TableInfoRequest, TableInfoResponse, TableSchema, SchemaField};
use crate::server::DeltaFlightServer;

pub async fn get_table_info(
    server: &DeltaFlightServer,
    request: Request<TableInfoRequest>,
) -> Result<Response<TableInfoResponse>, Status> {
    let req = request.into_inner();
    
    // Get table URI from the registry
    let tables = server.tables.lock().await;
    let table_uri = tables
        .get(&req.table_path)
        .ok_or_else(|| Status::not_found(format!("Table {} not found", req.table_path)))?
        .clone();
    drop(tables); // Release the lock
    
    // Load the Delta table
    let ops = DeltaOps::try_from_uri(table_uri.to_string_lossy())
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
    
    // Get the table and metadata
    let (table, _) = ops.load().await.map_err(|e| Status::internal(e.to_string()))?;
    
    // Get metadata details
    let metadata = table
        .metadata()
        .map_err(|_| Status::internal("Failed to get table metadata"))?;
    
    // Format timestamp as string (using simple conversion instead of chrono)
    let created_at = metadata.created_time
        .map(|ts| format!("{}", ts))
        .unwrap_or_else(|| "Unknown".to_string());
    
    // Build properties map
    let mut properties = std::collections::HashMap::new();
    
    // Always add these basic properties
    properties.insert("table.version".to_string(), table.version().to_string());
    properties.insert("table.location".to_string(), table_uri.to_string_lossy().to_string());
    
    // Add configuration properties
    let config = &metadata.configuration;
    for (key, value_option) in config {
        if let Some(value) = value_option {
            properties.insert(format!("config.{}", key), value.to_string());
        }
    }
    
    // Add partition columns
    if !metadata.partition_columns.is_empty() {
        properties.insert(
            "partition.columns".to_string(), 
            metadata.partition_columns.join(",")
        );
    }

    // Extract schema information
    let schema_fields = if let Ok(schema) = metadata.schema() {
        let fields = schema.fields()
            .map(|field| {
                SchemaField {
                    name: field.name().to_string(),
                    data_type: format!("{:?}", field.data_type()),
                    nullable: field.is_nullable(),
                }
            })
            .collect();
        
        Some(TableSchema {
            field_count: schema.fields().count() as i32,
            fields,
        })
    } else {
        None
    };
    
    // Create the response
    let response = TableInfoResponse {
        table_path: req.table_path,
        version: table.version(),
        created_at,
        table_name: metadata.name.clone(),
        description: metadata.description.clone(),
        properties,
        schema: schema_fields,
    };
    
    Ok(Response::new(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, sync::Arc};
    use tempfile::tempdir;
    use tokio::sync::Mutex;
    use crate::deltaflight::{ColumnSchema, TableCreateRequest};
    use crate::deltaflight::delta_flight_service_server::DeltaFlightService;
    use tonic::Request;

    #[tokio::test]
    async fn test_get_table_info_not_found() {
        // Setup
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let server = DeltaFlightServer {
            tables: Arc::new(Mutex::new(HashMap::new())),
            data_dir: temp_dir.path().to_path_buf(),
        };

        // Create a request for a non-existent table
        let request = Request::new(TableInfoRequest {
            table_path: "nonexistent_table".to_string(),
        });

        // Call get_table_info, which should fail
        let result = get_table_info(&server, request).await;
        assert!(result.is_err());

        // Check that the error is "not found"
        if let Err(status) = result {
            assert_eq!(status.code(), tonic::Code::NotFound);
        }
    }

    #[tokio::test]
    async fn test_get_table_info_success() {
        // Setup
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let server = DeltaFlightServer {
            tables: Arc::new(Mutex::new(HashMap::new())),
            data_dir: temp_dir.path().to_path_buf(),
        };

        // First create a table
        let table_name = "test_info_table";
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
        ];

        // Create the table
        let create_request = Request::new(TableCreateRequest {
            table_path: table_name.to_string(),
            columns,
        });

        server
            .create_table(create_request)
            .await
            .expect("Failed to create table");

        // Now get table info
        let info_request = Request::new(TableInfoRequest {
            table_path: table_name.to_string(),
        });

        let response = get_table_info(&server, info_request)
            .await
            .expect("Failed to get table info");
        
        let info = response.into_inner();

        println!("{:?}", info);

        // Verify the response
        assert_eq!(info.table_path, table_name);
        assert_eq!(info.version, 0); // New table should be at version 0
        
        
        // We expect at least some properties
        assert!(!info.properties.is_empty());
        
    }
}