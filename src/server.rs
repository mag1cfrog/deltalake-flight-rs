use std::{collections::HashMap, path::PathBuf, pin::Pin, str::from_utf8};
use std::error::Error;

use arrow::ipc::writer::IpcWriteOptions;
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch as ArrowRecordBatch;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::SchemaAsIpc;
use deltalake::{kernel::StructField, open_table, DeltaOps};
use futures::Stream;
use futures::StreamExt;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::{arrow::flight::protocol::{FlightData, Ticket}, deltaflight::{delta_flight_service_server::DeltaFlightService, TableCreateRequest, TableCreateResponse}, utils::data_type_parser::parse_data_type};

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
            .map(|c| {
                Ok(
                    StructField::new(
                        c.name.clone(),
                        parse_data_type(&c.data_type)?,
                        c.nullable
                    )
                )
                }
            )
            .collect::<Result<Vec<_>, _>>()?;

        let table_path = self.data_dir.join(&req.table_path);
        let _ = DeltaOps::try_from_uri(table_path.to_string_lossy())
            .await
            .map_err(|e| Status::internal(e.to_string()))?  // Convert DeltaTableError to Status
            .create()
            .with_columns(fields)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;  // Convert DeltaTableError to Status;
        
        self.tables.lock().await.insert(req.table_path, table_path);

        Ok(Response::new(TableCreateResponse {
            success: true,
            message: format!("Table {} created successfully", req.table_path).into(),
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
        
        let (_, stream) = ops.load()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        // Convert RecordBatch Stream to FlightData Stream
        let flight_stream = async_stream::try_stream! {
            let mut schema_sent = false;
            let mut schema = None;

            while let Some(batch) = stream.next().await {
                let batch = batch.map_err(|e| Status::internal(e.to_string()))?;

                if !schema_sent {
                    schema = Some(batch.schema().clone());
                    let schema_data = SchemaAsIpc::new(&batch.schema(), &IpcWriteOptions::default())
                        .into();
                    yield schema_data;
                    schema_sent = true;
                }

                let flight_data = batches_to_flight_data(&batch.schema(), &batch);
                yield flight_data;


           }
        };

        Ok(Response::new(Box::pin(flight_stream)))
    }
}

fn to_arrow_batch(batch: deltalake::arrow::record_batch::RecordBatch) -> ArrowRecordBatch {
    ArrowRecordBatch::try_new(
        batch.schema().clone(), 
        batch.columns().to_vec()
    ).map_err(|e| Status::internal(e.to_string()))?
}


fn to_arrow_schema(schema: &deltalake::arrow::datatypes::Schema) -> ArrowSchema {
    s
}