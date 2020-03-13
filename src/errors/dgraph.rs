use failure::Fail;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Dgraph: Txn start mismatch")]
    StartTsMismatch,
    #[fail(display = "Dgraph: gRPC communication Error")]
    GrpcError(String),
    #[fail(display = "Dgraph: Txn is empty")]
    EmptyTxn,
    #[fail(display = "Dgraph: Missing Txn context")]
    MissingTxnContext,
}
