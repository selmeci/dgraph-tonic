#[cfg(feature = "acl")]
pub use crate::sync::client::{AclClient, AclClientType, TxnAcl};
#[cfg(all(feature = "acl", feature = "tls"))]
pub use crate::sync::client::{AclTlsClient, TxnAclTls};
pub use crate::sync::client::{Client, Txn};
#[cfg(feature = "tls")]
pub use crate::sync::client::{TlsClient, TxnTls};
pub use crate::sync::txn::{
    BestEffortTxn, Mutate, MutatedTxn, MutationResponse, Query, ReadOnlyTxn, TxnState, TxnType,
    TxnVariant,
};

pub(crate) mod client;
pub(crate) mod txn;
