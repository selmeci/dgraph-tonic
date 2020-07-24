pub use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
pub use tonic::Status;

#[cfg(feature = "dgraph-1-0")]
pub use crate::api::Assigned;
use crate::api::IDgraphClient;
pub use crate::api::{
    Check, LoginRequest, Mutation, Operation, Payload, Request, Response, TxnContext, Version,
};
pub use crate::client::Endpoints;
#[cfg(feature = "acl")]
pub use crate::client::{
    AclClient, AclClientType, LazyChannel, TxnAcl, TxnAclBestEffort, TxnAclMutated, TxnAclReadOnly,
};
#[cfg(all(feature = "acl", feature = "tls"))]
pub use crate::client::{
    AclTlsClient, TxnAclTls, TxnAclTlsBestEffort, TxnAclTlsMutated, TxnAclTlsReadOnly,
};
pub use crate::client::{
    Client, ClientVariant, IClient, Txn, TxnBestEffort, TxnMutated, TxnReadOnly,
};
#[cfg(feature = "tls")]
pub use crate::client::{TlsClient, TxnTls, TxnTlsBestEffort, TxnTlsMutated, TxnTlsReadOnly};
pub use crate::errors::{ClientError, DgraphError};
pub use crate::txn::{
    Mutate, MutationResponse, Query, TxnBestEffortType, TxnMutatedType, TxnReadOnlyType, TxnState,
    TxnType, TxnVariant,
};

mod api;
mod client;
mod errors;
#[cfg(feature = "experimental")]
mod stream;
mod stub;
#[cfg(feature = "sync")]
pub mod sync;
mod txn;

pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;
