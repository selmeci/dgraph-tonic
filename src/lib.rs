pub use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::Status;

use api::dgraph_client::DgraphClient;
pub(crate) use api::Mutation as Mu;
pub use api::{
    Assigned, Check, LoginRequest, Operation, Payload, Request, Response, TxnContext, Version,
};
use async_trait::async_trait;

pub use crate::client::Client;
pub use crate::errors::{ClientError, DgraphError};
use crate::mutation::Mutation;
pub use crate::txn::{BestEffortTxn, MutatedTxn, ReadOnlyTxn, Txn};

mod client;
mod errors;
mod mutation;
mod response;
mod stub;
mod txn;

mod api {
    tonic::include_proto!("api");
}

pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T, E = StdError> = ::std::result::Result<T, E>;

#[async_trait]
pub(crate) trait IDgraphClient: Clone + Sized {
    async fn login(&mut self, user_id: String, password: String) -> Result<Response, Status>;

    async fn query(&mut self, query: Request) -> Result<Response, Status>;

    async fn mutate(&mut self, mu: Mutation) -> Result<Assigned, Status>;

    async fn alter(&mut self, op: Operation) -> Result<Payload, Status>;

    async fn commit_or_abort(&mut self, txn: TxnContext) -> Result<TxnContext, Status>;

    async fn check_version(&mut self) -> Result<Version, Status>;
}

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
