use anyhow::Result;
use async_trait::async_trait;
use tonic::Request;

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
    async fn login(&mut self, login: LoginRequest) -> Result<DgraphResponse> {
        let request = Request::new(login);
        let client = self.client.client().await?;
        match client.login(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(ClientError::CannotLogin(status).into()),
        }
    }

    async fn query(&mut self, query: DgraphRequest) -> Result<DgraphResponse> {
        let request = Request::new(query);
        let client = self.client.client().await?;
        match client.query(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(ClientError::CannotQuery(status).into()),
        }
    }

    #[cfg(feature = "dgraph-1-0")]
    async fn mutate(&mut self, mu: Mutation) -> Result<Assigned> {
        let request = Request::new(mu);
        let client = self.client.client().await?;
        match client.mutate(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(ClientError::CannotMutate(status).into()),
        }
    }

    #[cfg(feature = "dgraph-1-1")]
    async fn do_request(&mut self, req: DgraphRequest) -> Result<DgraphResponse> {
        let request = Request::new(req);
        let client = self.client.client().await?;
        match client.query(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(ClientError::CannotDoRequest(status).into()),
        }
    }

    async fn alter(&mut self, op: Operation) -> Result<Payload> {
        let request = Request::new(op);
        let client = self.client.client().await?;
        match client.alter(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(ClientError::CannotAlter(status).into()),
        }
    }

    async fn commit_or_abort(&mut self, txn: TxnContext) -> Result<TxnContext> {
        let request = Request::new(txn);
        let client = self.client.client().await?;
        match client.commit_or_abort(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(ClientError::CannotCommitOrAbort(status).into()),
        }
    }

    async fn check_version(&mut self) -> Result<Version> {
        let request = Request::new(Check {});
        let client = self.client.client().await?;
        match client.check_version(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(ClientError::CannotCheckVersion(status).into()),
        }
    }
}
