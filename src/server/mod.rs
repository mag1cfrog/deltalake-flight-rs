mod utils;

use std::{collections::HashMap, path::PathBuf, pin::Pin, str::from_utf8};

use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::error::FlightError;
use arrow_flight::PutResult;
use arrow_flight::{FlightData, SchemaAsIpc, Ticket};
use deltalake::{kernel::StructField, DeltaOps};
use futures::{Stream, TryStreamExt};
use futures::StreamExt;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::{deltaflight::{delta_flight_service_server::DeltaFlightService, TableCreateRequest, TableCreateResponse}, utils::data_type_parser::parse_data_type};

#[derive(Debug)]
struct DeltaFlightServer {
    tables: Mutex<HashMap<String, PathBuf>>,
    data_dir: PathBuf,
}

type FlightStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

#[tonic::async_trait]
impl DeltaFlightService for DeltaFlightServer { 
    type DoGetStream = FlightStream;

    async fn create_table(&self, request: Request<TableCreateRequest>) -> Result<Response<TableCreateResponse>, Status> {
        let req = request.into_inner();

        // Convert protobuf schema to Delta Lake schema
        let fields = req.columns.iter()
            .map(|c| -> Result<StructField, Status> {
                Ok(
                    StructField::new(
                        c.name.clone(),
                        parse_data_type(&c.data_type).map_err(|e| Status::internal(e.to_string()))?,
                        c.nullable
                    )
                )
                }
            )
            .collect::<Result<Vec<_>, Status>>()?;

        let table_path = self.data_dir.join(&req.table_path);
        let _ = DeltaOps::try_from_uri(table_path.to_string_lossy().to_string())
            .await
            .map_err(|e| Status::internal(e.to_string()))?  // Convert DeltaTableError to Status
            .create()
            .with_columns(fields)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;  // Convert DeltaTableError to Status;
        
        self.tables.lock().await.insert(req.table_path, table_path.clone());

        Ok(Response::new(TableCreateResponse {
            success: true,
            message: format!("Table {} created successfully", table_path.to_string_lossy()),
        }))
    }

    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let table_path = from_utf8(&ticket.ticket)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let tables = self.tables.lock().await;
        let table_uri = tables.get(table_path)
            .ok_or_else(|| Status::not_found(format!("Table {} not found", table_path)))?;

        // Create stream using DeltaOps
        let ops = DeltaOps::try_from_uri(table_uri.to_string_lossy())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        let (_, mut stream) = ops.load()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        // Convert RecordBatch Stream to FlightData Stream
        let flight_stream = async_stream::try_stream! {
            let mut schema_sent = false;
            // let schema: Option<arrow::datatypes::SchemaRef> = None;

            while let Some(batch) = stream.next().await {
                let batch = batch.map_err(|e| Status::internal(e.to_string()))?;

                // Send schema only once
                if !schema_sent {
                    let schema = batch.schema();
                    let options = IpcWriteOptions::default();
                    let schema_data = SchemaAsIpc::new(&schema, &options);
                    yield schema_data.into();
                    schema_sent = true;
                }

                // Send batch data without schema
                let flight_data_result = utils::batches_to_flight_data_without_schema(vec![batch.clone()]);

                // Handle the Result properly
                match flight_data_result {
                    Ok(flight_data) => {
                        // Yield each FlightData from the vector
                        for data in flight_data {
                            yield data;
                        }
                    },
                    Err(e) => Err(Status::internal(e.to_string()))?,
                }

           }
        };

        Ok(Response::new(Box::pin(flight_stream)))
    }

    async fn do_put(&self, request: Request<tonic::Streaming<FlightData>>) -> Result<Response<arrow_flight::PutResult>, Status> {
        let stream = request.into_inner();

        // Convert the stream to use FlightError instead of Status
        let converted_stream = stream.map_err(|status| {
            FlightError::from_external_error(Box::new(status))
        });

        // Use FlightDataDecoder to process the stream
        let mut decoder = FlightDataDecoder::new(converted_stream);

        // The first message should contain the descriptor with table information
        let first_data = decoder.next().await
            .ok_or_else(|| Status::invalid_argument("Empty flight data stream"))
            .map_err(|e| Status::internal(e.to_string()))??;

        // Extract table path and schema
        let _schema = match &first_data.payload {
            DecodedPayload::Schema(schema) => schema.clone(),
            _ => return Err(Status::invalid_argument("First message must contain schema"))
        };

        let descriptor = first_data.inner.flight_descriptor
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing flight descriptor"))?;

        let table_path = &descriptor.path[0];

        // Get table URI from stored tables
        let tables = self.tables.lock().await;
        let table_uri = tables.get(table_path)
            .ok_or_else(|| Status::not_found(format!("Table {} not found", table_path)))?
            .clone();
        drop(tables); // Release the lock

        // Now collect the record batches
        let mut batches = Vec::new();

        while let Some(data_result) = decoder.next().await {
            match data_result {
                Ok(data) => {
                    if let DecodedPayload::RecordBatch(batch) = data.payload {
                        batches.push(batch);
                    }
                },
                Err(e) => return Err(Status::internal(format!("Failed to decode data: {}", e))),
            }
        }

        // If we have batches, write them to the table
        if !batches.is_empty() {
            // Open the Delta table
            let ops = DeltaOps::try_from_uri(table_uri.to_string_lossy())
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            // Write the batches
            ops.write(batches)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        // Return sucess
        Ok(Response::new(PutResult {
            app_metadata: format!("Successfully wrote data to table {}", table_path).into(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::{array::{ArrayRef, Decimal128Builder, Int32Array, RecordBatch, StringArray}, datatypes::{DataType, Field, Schema}};
    use arrow_flight::decode::FlightRecordBatchStream;
    use tempfile::tempdir;
    use crate::deltaflight::ColumnSchema;

    // Helper function to create a test server with a temporary directory
    async fn setup_test_server() -> (DeltaFlightServer, tempfile::TempDir) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let server = DeltaFlightServer {
            tables: Mutex::new(HashMap::new()),
            data_dir: temp_dir.path().to_path_buf(),
        };
        (server, temp_dir)
    }

    #[tokio::test]
    async fn test_create_table() {
        // Setup
        let (server, _temp_dir) = setup_test_server().await;
        
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
        let response = server.create_table(request).await.expect("Table creation failed");
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
        let (server, _temp_dir) = setup_test_server().await;
        
        // Create an invalid schema (uuid type is not supported)
        let columns = vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: "uuid".to_string(), // Invalid type
                nullable: false,
            },
        ];
        
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

    #[tokio::test]
    async fn test_do_get_table_not_found() {
        // Setup
        let (server, _temp_dir) = setup_test_server().await;

        // Create a ticket for a non-existent table
        let ticket = Ticket {
            ticket: "non_existent_table".as_bytes().to_vec().into(),
        };
        
        // Call do_get, which should fail
        let result = server.do_get(Request::new(ticket)).await;
        assert!(result.is_err());
        
        // Check that the error is a "not found" error
        if let Err(status) = result {
            assert_eq!(status.code(), tonic::Code::NotFound);
            assert!(status.message().contains("not found"));
        }
    }

    #[tokio::test]
    async fn test_do_get_with_data() {
        // Setup
        let (server, _temp_dir) = setup_test_server().await;
        
        // 1. First create a test table
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
        
        let table_name = "test_get_table";
        let request = Request::new(TableCreateRequest {
            table_path: table_name.to_string(),
            columns,
        });
        
        let _ = server.create_table(request).await.expect("Table creation failed");
        
        // 2. Write some data to the table
        // Get the table path from server's tables
        let table_path = {
            let tables = server.tables.lock().await;
            tables.get(table_name).unwrap().clone()
        };
        
        // Create record batch with test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
        ]));
        
        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let name_array: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave", "Eve"]));
        
        // Create a decimal array for the amount
        let decimal_values = vec![10042, 20000, 15099, 0, 50025]; // Values like 100.42, 200.00, etc.
        let mut decimal_builder = Decimal128Builder::with_capacity(5)
            .with_precision_and_scale(10, 2)
            .unwrap();

        for value in decimal_values {
            decimal_builder.append_value(value);
        }
        let amount_array: ArrayRef = Arc::new(decimal_builder.finish());
        
        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, name_array, amount_array])
            .expect("Failed to create record batch");
        
        // Write the batch to the delta table
        let ops = DeltaOps::try_from_uri(table_path.to_string_lossy())
            .await
            .expect("Failed to open Delta table");
        ops.write(vec![batch.clone()])
            .await
            .expect("Failed to write to Delta table");
        
        // 3. Call do_get to retrieve the data
        let ticket = Ticket {
            ticket: table_name.as_bytes().to_vec().into(),
        };
        
        let response = server.do_get(Request::new(ticket)).await.expect("do_get failed");
        let stream = response.into_inner();
        
        // First convert the Status errors to FlightError
        let converted_stream = stream.map_err(FlightError::Tonic);

        // Create the decoder from the stream
        let decoder = FlightDataDecoder::new(converted_stream);

        // Now create the record batch stream from the decoder
        let mut batch_stream = FlightRecordBatchStream::new(decoder);
        
        // Process all batches, starting with the first one
        let mut received_rows = 0;
        let mut batch_count = 0;
        
        while let Some(batch_result) = batch_stream.next().await {
            let batch = batch_result.expect("Failed to get record batch");
            received_rows += batch.num_rows();
            batch_count += 1;
            
            // For the first batch, check the schema too
            if batch_count == 1 {
                let schema = batch.schema();
                assert_eq!(schema.fields.len(), 3);
            }
            
            // Verify the structure and content of each batch
            assert_eq!(batch.num_columns(), 3);
            
            // Check the id column
            let id_array = batch.column(0).as_any().downcast_ref::<Int32Array>()
                .expect("Expected Int32Array");
            
            // Check the name column
            let name_array = batch.column(1).as_any().downcast_ref::<StringArray>()
                .expect("Expected StringArray");
            
            // Verify the first element of each batch (they should all be the same
            // since we're getting the same batch multiple times due to how Flight works)
            assert_eq!(id_array.value(0), 1);
            assert_eq!(name_array.value(0), "Alice");
        }
        
        // Verify we got all rows
        assert_eq!(received_rows, 5);
        
        // Make sure we got at least one batch
        assert!(batch_count > 0, "No batches received");
    }
}