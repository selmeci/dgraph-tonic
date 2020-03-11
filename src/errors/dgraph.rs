use failure::Fail;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Dgraph: Txn start mismatch")]
    StartTsMismatch,
    #[fail(display = "Dgraph: gRPC communication Error")]
    GrpcError,
    #[fail(display = "Dgraph: Txn is empty")]
    EmptyTxn,
    #[fail(display = "Dgraph: Missing Txn context")]
    MissingTxnContext,
}
