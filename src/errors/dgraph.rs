use failure::{Error as Failure, Fail};

///
/// Possible Dgraph errors
///
#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Dgraph: Txn start mismatch")]
    StartTsMismatch,
    #[fail(display = "Dgraph: gRPC communication Error")]
    GrpcError(Failure),
    #[fail(display = "Dgraph: Txn is empty")]
    EmptyTxn,
    #[fail(display = "Dgraph: Missing Txn context")]
    MissingTxnContext,
    #[fail(display = "Dgraph: Txn is already committed")]
    TxnCommitted,
}
