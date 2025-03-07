use std::{collections::HashMap, path::PathBuf, pin::Pin, str::from_utf8};

use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::error::FlightError;
use arrow_flight::PutResult;
use arrow_flight::{FlightData, SchemaAsIpc, Ticket, utils::batches_to_flight_data};
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

                if !schema_sent {
                    let schema = batch.schema();
                    let options = IpcWriteOptions::default();
                    let schema_data = SchemaAsIpc::new(&schema, &options);
                    // Convert to FlightData before yielding
                    yield schema_data.into(); // Use the From<SchemaAsIpc> for FlightData implementation
                    schema_sent = true;
                }

                // Create a vector containing a clone of the batch
                let flight_data_result = batches_to_flight_data(&batch.schema(), vec![batch.clone()]);

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
