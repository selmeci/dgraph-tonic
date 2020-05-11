#[cfg(all(feature = "acl", feature = "tls"))]
pub use crate::sync::client::TlsAclTxn;
#[cfg(feature = "acl")]
pub use crate::sync::client::{AclClient, DefaultAclTxn};
pub use crate::sync::client::{Client, DefaultTxn};
#[cfg(feature = "tls")]
pub use crate::sync::client::{TlsClient, TlsTxn};
pub use crate::sync::txn::{
    BestEffortTxn, Mutate, MutatedTxn, MutationResponse, Query, ReadOnlyTxn, Txn, TxnState,
    TxnVariant,
};

pub(crate) mod client;
pub(crate) mod txn;
