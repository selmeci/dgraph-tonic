pub use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
pub use tonic::Status;

use crate::api::dgraph_client::DgraphClient;
#[cfg(feature = "dgraph-1-0")]
pub use crate::api::Assigned;
use crate::api::IDgraphClient;
pub use crate::api::{
    Check, LoginRequest, Mutation, Operation, Payload, Request, Response, TxnContext, Version,
};
pub use crate::client::Client;
pub use crate::errors::{ClientError, DgraphError};
pub use crate::txn::{BestEffortTxn, MutatedTxn, ReadOnlyTxn, Txn, TxnState, TxnVariant};

mod api;
mod client;
mod errors;
mod stub;
mod txn;

pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T, E = StdError> = ::std::result::Result<T, E>;
