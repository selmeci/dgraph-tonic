///
/// Possible Dgraph errors
///
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    StartTsMismatch,
    GrpcError(super::ClientError),
    EmptyTxn,
    MissingTxnContext,
    TxnCommitted,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::StartTsMismatch => write!(f, "Dgraph: Txn start mismatch"),
            Error::GrpcError(err) => write!(f, "Dgraph: gRPC communication Error - {}", err),
            Error::EmptyTxn => write!(f, "Dgraph: Txn is empty"),
            Error::MissingTxnContext => write!(f, "Dgraph: Missing Txn context)"),
            Error::TxnCommitted => write!(f, "Dgraph: Txn is already committed"),
        }
    }
}
