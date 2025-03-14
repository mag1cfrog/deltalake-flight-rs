use crate::deltaflight::{ListTablesRequest, ListTablesResponse};
use crate::server::DeltaFlightServer;
use regex::Regex;
use tonic::{Request, Response, Status};

pub async fn list_tables(
    server: &DeltaFlightServer,
    request: Request<ListTablesRequest>,
) -> Result<Response<ListTablesResponse>, Status> {
    let req = request.into_inner();

    // Get all table paths from the server's table registry
    let tables = server.tables.lock().await;

    // Get table paths as strings
    let mut table_paths: Vec<String> = tables.keys().cloned().collect();

    // Apply pattern filtering if specified
    if let Some(pattern) = req.pattern {
        // Create a regex for the pattern
        let regex = Regex::new(&pattern)
            .map_err(|e| Status::invalid_argument(format!("Invalid regex pattern: {}", e)))?;

        // Filter table paths that match the pattern
        table_paths.retain(|path| regex.is_match(path));
    }

    // Sort table paths alphabetically for consistent responses
    table_paths.sort();

    // Create the response
    let response = ListTablesResponse { table_paths };

    Ok(Response::new(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, sync::Arc};
    use tempfile::tempdir;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_list_tables_no_pattern() {
        // Setup
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut tables = HashMap::new();

        // Add some test tables
        tables.insert("table1".to_string(), temp_dir.path().join("table1"));
        tables.insert("table2".to_string(), temp_dir.path().join("table2"));
        tables.insert(
            "other_table".to_string(),
            temp_dir.path().join("other_table"),
        );

        let server = DeltaFlightServer {
            tables: Arc::new(Mutex::new(tables)),
            data_dir: temp_dir.path().to_path_buf(),
        };

        // Create a request with no pattern
        let request = Request::new(ListTablesRequest { pattern: None });

        // Call list_tables
        let response = list_tables(&server, request)
            .await
            .expect("List tables failed");
        let response_inner = response.into_inner();

        // Verify all tables are returned and sorted
        assert_eq!(response_inner.table_paths.len(), 3);
        assert_eq!(response_inner.table_paths[0], "other_table");
        assert_eq!(response_inner.table_paths[1], "table1");
        assert_eq!(response_inner.table_paths[2], "table2");
    }

    #[tokio::test]
    async fn test_list_tables_with_pattern() {
        // Setup
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut tables = HashMap::new();

        // Add some test tables
        tables.insert("table1".to_string(), temp_dir.path().join("table1"));
        tables.insert("table2".to_string(), temp_dir.path().join("table2"));
        tables.insert(
            "other_table".to_string(),
            temp_dir.path().join("other_table"),
        );

        let server = DeltaFlightServer {
            tables: Arc::new(Mutex::new(tables)),
            data_dir: temp_dir.path().to_path_buf(),
        };

        // Create a request with a pattern that matches only "table*"
        let request = Request::new(ListTablesRequest {
            pattern: Some("^table.*$".to_string()),
        });

        // Call list_tables
        let response = list_tables(&server, request)
            .await
            .expect("List tables failed");
        let response_inner = response.into_inner();

        // Verify only matching tables are returned
        assert_eq!(response_inner.table_paths.len(), 2);
        assert_eq!(response_inner.table_paths[0], "table1");
        assert_eq!(response_inner.table_paths[1], "table2");
    }

    #[tokio::test]
    async fn test_list_tables_invalid_pattern() {
        // Setup
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let server = DeltaFlightServer {
            tables: Arc::new(Mutex::new(HashMap::new())),
            data_dir: temp_dir.path().to_path_buf(),
        };

        // Create a request with an invalid regex pattern
        let request = Request::new(ListTablesRequest {
            pattern: Some("*[invalid".to_string()), // Invalid regex
        });

        // Call list_tables - should fail
        let result = list_tables(&server, request).await;
        assert!(result.is_err());

        // Check that the error is about the invalid pattern
        if let Err(status) = result {
            assert_eq!(status.code(), tonic::Code::InvalidArgument);
            assert!(status.message().contains("Invalid regex pattern"));
        }
    }
}
