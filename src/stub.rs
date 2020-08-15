use anyhow::Result;
use async_trait::async_trait;
use tonic::Request;
use tracing::trace;
use tracing_attributes::instrument;

use crate::client::ILazyClient;
#[cfg(feature = "dgraph-1-0")]
use crate::{Assigned, Mutation};
use crate::{
    Check, ClientError, IDgraphClient, LoginRequest, Operation, Payload, Request as DgraphRequest,
    Response as DgraphResponse, TxnContext, Version,
};

///
/// Hold channel connection do Dgraph and implement calls for Dgraph API operations.
///
#[derive(Clone, Debug)]
pub struct Stub<C: ILazyClient> {
    client: C,
}

impl<C: ILazyClient> Stub<C> {
    pub fn new(client: C) -> Self {
        Self { client }
    }
}

#[async_trait]
impl<C: ILazyClient> IDgraphClient for Stub<C> {
    #[instrument(skip(self))]
    async fn login(&mut self, login: LoginRequest) -> Result<DgraphResponse> {
        trace!("login");
        let request = Request::new(login);
        let client = self.client.client().await?;
        match client.login(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(ClientError::CannotLogin(status).into()),
        }
    }

    #[instrument(skip(self))]
    async fn query(&mut self, query: DgraphRequest) -> Result<DgraphResponse> {
        trace!("query");
        let request = Request::new(query);
        let client = self.client.client().await?;
        match client.query(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(ClientError::CannotQuery(status).into()),
        }
    }

    #[instrument(skip(self))]
    #[cfg(feature = "dgraph-1-0")]
    async fn mutate(&mut self, mu: Mutation) -> Result<Assigned> {
        trace!("mutate");
        let request = Request::new(mu);
        let client = self.client.client().await?;
        match client.mutate(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(ClientError::CannotMutate(status).into()),
        }
    }

    #[instrument(skip(self))]
    #[cfg(feature = "dgraph-1-1")]
    async fn do_request(&mut self, req: DgraphRequest) -> Result<DgraphResponse> {
        trace!("do_request");
        let request = Request::new(req);
        let client = self.client.client().await?;
        match client.query(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(ClientError::CannotDoRequest(status).into()),
        }
    }

    #[instrument(skip(self))]
    async fn alter(&mut self, op: Operation) -> Result<Payload> {
        trace!("alter");
        let request = Request::new(op);
        let client = self.client.client().await?;
        match client.alter(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(ClientError::CannotAlter(status).into()),
        }
    }

    #[instrument(skip(self))]
    async fn commit_or_abort(&mut self, txn: TxnContext) -> Result<TxnContext> {
        trace!("commit_or_abort");
        let request = Request::new(txn);
        let client = self.client.client().await?;
        match client.commit_or_abort(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(ClientError::CannotCommitOrAbort(status).into()),
        }
    }

    #[instrument(skip(self))]
    async fn check_version(&mut self) -> Result<Version> {
        trace!("check_version");
        let request = Request::new(Check {});
        let client = self.client.client().await?;
        match client.check_version(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(ClientError::CannotCheckVersion(status).into()),
        }
    }
}
