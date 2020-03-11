pub use crate::errors::ClientError;
pub use crate::errors::DgraphError;
use api::dgraph_client::DgraphClient;
use api::{
    Assigned, Check, LoginRequest, Mutation, Operation, Payload, Request, Response, TxnContext,
    Version,
};
pub use async_client::{Client as AsyncClient, IDgraphClient as IAsyncClient};
use failure::Error;
use std::convert::TryInto;
pub use sync_client::{Client as SyncClient, IDgraphClient as ISyncCLient};
pub use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

mod async_client;
mod errors;
mod sync_client;

mod api {
    tonic::include_proto!("api");
}

pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type ClientResult<T, E = StdError> = ::std::result::Result<T, E>;

impl TxnContext {
    pub(crate) fn merge_context(&mut self, src: &TxnContext) -> Result<(), DgraphError> {
        if self.start_ts == 0 {
            self.start_ts = src.start_ts;
        } else if self.start_ts != src.start_ts {
            return Err(DgraphError::StartTsMismatch);
        };
        let dedup = |data: &mut Vec<String>| {
            data.sort_unstable();
            data.dedup();
        };
        self.keys.append(&mut src.keys.clone());
        dedup(&mut self.keys);
        self.preds.append(&mut src.preds.clone());
        dedup(&mut self.preds);

        Ok(())
    }
}

pub struct Builder;

impl Builder {
    pub async fn async_client<S: TryInto<Endpoint>>(
        endpoints: impl Iterator<Item = S>,
    ) -> Result<AsyncClient, Error> {
        async_client::Client::new(endpoints).await
    }

    pub fn sync_client<S: TryInto<Endpoint>>(
        endpoints: impl Iterator<Item = S>,
    ) -> Result<SyncClient, Error> {
        sync_client::Client::new(endpoints)
    }
}
