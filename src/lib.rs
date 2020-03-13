use std::convert::TryInto;
use std::path::Path;

use failure::Error;
pub use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use api::dgraph_client::DgraphClient;
pub use api::{
    Assigned, Check, LoginRequest, Mutation, Operation, Payload, Request, Response, TxnContext,
    Version,
};
pub use async_client::{Client as AsyncClient, IDgraphClient as IAsyncClient};
pub use sync_client::{Client as SyncClient, IDgraphClient as ISyncCLient};

pub use crate::errors::ClientError;
pub use crate::errors::DgraphError;

pub mod async_client;
mod errors;
mod response;
pub mod sync_client;

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

    pub async fn async_client_with_tls_auth<S: TryInto<Endpoint>>(
        domain_name: impl Into<String>,
        endpoints: impl Iterator<Item = S>,
        server_root_ca_cert: impl AsRef<Path>,
        client_cert: impl AsRef<Path>,
        client_key: impl AsRef<Path>,
    ) -> Result<AsyncClient, Error> {
        async_client::Client::new_with_tls_client_auth(
            domain_name,
            endpoints,
            server_root_ca_cert,
            client_cert,
            client_key,
        )
        .await
    }

    pub fn sync_client<S: TryInto<Endpoint>>(
        endpoints: impl Iterator<Item = S>,
    ) -> Result<SyncClient, Error> {
        sync_client::Client::new(endpoints)
    }

    pub fn sync_client_with_tls_auth<S: TryInto<Endpoint>>(
        domain_name: impl Into<String>,
        endpoints: impl Iterator<Item = S>,
        server_root_ca_cert: impl AsRef<Path>,
        client_cert: impl AsRef<Path>,
        client_key: impl AsRef<Path>,
    ) -> Result<SyncClient, Error> {
        sync_client::Client::new_with_tls_client_auth(
            domain_name,
            endpoints,
            server_root_ca_cert,
            client_cert,
            client_key,
        )
    }
}
