use anyhow::Error as Failure;
use thiserror::Error as Fail;

///
/// Possible Dgraph errors
///
#[derive(Debug, Fail)]
pub enum Error {
    #[error("Dgraph: Txn start mismatch")]
    StartTsMismatch,
    #[error("Dgraph: gRPC communication Error")]
    GrpcError(Failure),
    #[error("Dgraph: Txn is empty")]
    EmptyTxn,
    #[error("Dgraph: Missing Txn context")]
    MissingTxnContext,
    #[error("Dgraph: Txn is already committed")]
    TxnCommitted,
}
