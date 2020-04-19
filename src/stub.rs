use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use async_trait::async_trait;

#[cfg(feature = "dgraph-1-0")]
use crate::{Assigned, Mutation};
use crate::{
    Check, DgraphClient, IDgraphClient, LoginRequest, Operation, Payload, Request as DgraphRequest,
    Response as DgraphResponse, Result, TxnContext, Version,
};

#[derive(Clone, Debug)]
pub struct Stub {
    client: DgraphClient<Channel>,
}

impl Stub {
    pub fn new(client: DgraphClient<Channel>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl IDgraphClient for Stub {
    async fn login(&mut self, user_id: String, password: String) -> Result<DgraphResponse, Status> {
        let login = LoginRequest {
            userid: user_id,
            password,
            ..Default::default()
        };
        let request = Request::new(login);
        let response: Response<DgraphResponse> = self.client.login(request).await?;
        Ok(response.into_inner())
    }

    async fn query(&mut self, query: DgraphRequest) -> Result<DgraphResponse, Status> {
        let request = Request::new(query);
        let response: Response<DgraphResponse> = self.client.query(request).await?;
        Ok(response.into_inner())
    }

    #[cfg(feature = "dgraph-1-0")]
    async fn mutate(&mut self, mu: Mutation) -> Result<Assigned, Status> {
        let request = Request::new(mu);
        let response: Response<Assigned> = self.client.mutate(request).await?;
        Ok(response.into_inner())
    }

    #[cfg(feature = "dgraph-1-1")]
    async fn do_request(&mut self, req: DgraphRequest) -> Result<DgraphResponse, Status> {
        let request = Request::new(req);
        let response: Response<DgraphResponse> = self.client.query(request).await?;
        Ok(response.into_inner())
    }

    async fn alter(&mut self, op: Operation) -> Result<Payload, Status> {
        let request = Request::new(op);
        let response: Response<Payload> = self.client.alter(request).await?;
        Ok(response.into_inner())
    }

    async fn commit_or_abort(&mut self, txn: TxnContext) -> Result<TxnContext, Status> {
        let request = Request::new(txn);
        let response: Response<TxnContext> = self.client.commit_or_abort(request).await?;
        Ok(response.into_inner())
    }

    async fn check_version(&mut self) -> Result<Version, Status> {
        let request = Request::new(Check {});
        let response: Response<Version> = self.client.check_version(request).await?;
        Ok(response.into_inner())
    }
}
