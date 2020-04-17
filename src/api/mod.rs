use tonic::Status;

use async_trait::async_trait;

pub use crate::api::v1_0_x::*;

mod mutation;
mod response;
mod txn_context;
mod v1_0_x;

#[async_trait]
pub(crate) trait IDgraphClient: Clone + Sized {
    async fn login(&mut self, user_id: String, password: String) -> Result<Response, Status>;

    async fn query(&mut self, query: Request) -> Result<Response, Status>;

    async fn mutate(&mut self, mu: Mutation) -> Result<Assigned, Status>;

    async fn alter(&mut self, op: Operation) -> Result<Payload, Status>;

    async fn commit_or_abort(&mut self, txn: TxnContext) -> Result<TxnContext, Status>;

    async fn check_version(&mut self) -> Result<Version, Status>;
}
