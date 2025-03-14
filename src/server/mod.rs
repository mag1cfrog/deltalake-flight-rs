mod operations;
mod utils;

use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf, pin::Pin};

use arrow_flight::{FlightData, Ticket};

use futures::Stream;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::deltaflight::{
    delta_flight_service_server::DeltaFlightService, TableCreateRequest, TableCreateResponse,
};
use crate::deltaflight::{ListTablesRequest, ListTablesResponse, TableInfoRequest, TableInfoResponse};

#[derive(Debug)]
struct DeltaFlightServer {
    tables: Arc<Mutex<HashMap<String, PathBuf>>>,
    data_dir: PathBuf,
}

pub type FlightStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

#[tonic::async_trait]
impl DeltaFlightService for DeltaFlightServer {
    type DoGetStream = FlightStream;

    async fn create_table(
        &self,
        request: Request<TableCreateRequest>,
    ) -> Result<Response<TableCreateResponse>, Status> {
        operations::create_table(self, request).await
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        operations::do_get(self, request).await
    }

    async fn do_put(
        &self,
        request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<arrow_flight::PutResult>, Status> {
        operations::do_put(self, request).await
    }

    async fn list_tables(
        &self,
        request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        operations::list_tables(self, request).await
    }
    
    async fn get_table_info(
        &self,
        request: Request<TableInfoRequest>,
    ) -> Result<Response<TableInfoResponse>, Status> {
        operations::get_table_info(self, request).await
    }
}
