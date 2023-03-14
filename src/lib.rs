pub use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
pub use tonic::Status;

#[cfg(feature = "dgraph-1-0")]
pub use crate::api::Assigned;
use crate::api::IDgraphClient;
#[cfg(any(feature = "dgraph-1-1", feature = "dgraph-21-03"))]
pub use crate::api::Metrics;
pub use crate::api::{
    Check, Latency, LoginRequest, Mutation, Operation, Payload, Request, Response, TxnContext,
    Version,
};
#[cfg(feature = "acl")]
pub use crate::client::{
    AclClient, AclClientType, LazyChannel, TxnAcl, TxnAclBestEffort, TxnAclMutated, TxnAclReadOnly,
};
#[cfg(all(feature = "acl", feature = "tls"))]
pub use crate::client::{
    AclTlsClient, TxnAclTls, TxnAclTlsBestEffort, TxnAclTlsMutated, TxnAclTlsReadOnly,
};
pub use crate::client::{
    Client, ClientVariant, EndpointConfig, Endpoints, Http, IClient, Txn, TxnBestEffort,
    TxnMutated, TxnReadOnly,
};
#[cfg(feature = "slash-ql")]
pub use crate::client::{
    SlashQl, SlashQlClient, TxnSlashQl, TxnSlashQlBestEffort, TxnSlashQlMutated, TxnSlashQlReadOnly,
};
#[cfg(feature = "tls")]
pub use crate::client::{Tls, TlsClient, TxnTls, TxnTlsBestEffort, TxnTlsMutated, TxnTlsReadOnly};
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
