use anyhow::Result;
use async_trait::async_trait;

#[cfg(feature = "dgraph-1-0")]
pub use crate::api::v1_0_x::*;
#[cfg(feature = "dgraph-1-1")]
pub use crate::api::v1_1_x::*;

mod mutation;
mod response;
mod txn_context;
mod v1_0_x;
mod v1_1_x;

#[async_trait]
#[doc(hidden)]
pub(crate) trait IDgraphClient: Clone + Sized {
    async fn login(&mut self, login: LoginRequest) -> Result<Response>;

    async fn query(&mut self, query: Request) -> Result<Response>;

    #[cfg(feature = "dgraph-1-0")]
    async fn mutate(&mut self, mu: Mutation) -> Result<Assigned>;

    #[cfg(feature = "dgraph-1-1")]
    async fn do_request(&mut self, req: Request) -> Result<Response>;

    async fn alter(&mut self, op: Operation) -> Result<Payload>;

    async fn commit_or_abort(&mut self, txn: TxnContext) -> Result<TxnContext>;

    async fn check_version(&mut self) -> Result<Version>;
}
