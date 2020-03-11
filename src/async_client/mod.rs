pub use crate::async_client::txn::Txn;
use crate::errors::ClientError;
use crate::{
    Assigned, Check, ClientResult, DgraphClient, LoginRequest, Mutation, Operation, Payload,
    Request as DgraphRequest, Response as DgraphResponse, TxnContext, Version,
};
use async_trait::async_trait;
use failure::Error as Failure;
use std::convert::TryInto;
use std::fmt::{self, Debug, Formatter};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

mod txn;

#[async_trait]
pub trait IDgraphClient: Clone {
    async fn login(
        &mut self,
        user_id: String,
        password: String,
    ) -> ClientResult<DgraphResponse, Status>;

    async fn query(&mut self, query: DgraphRequest) -> ClientResult<DgraphResponse, Status>;

    async fn mutate(&mut self, mu: Mutation) -> ClientResult<Assigned, Status>;

    async fn alter(&mut self, op: Operation) -> ClientResult<Payload, Status>;

    async fn commit_or_abort(&mut self, txn: TxnContext) -> ClientResult<TxnContext, Status>;

    async fn check_version(&mut self) -> ClientResult<Version, Status>;
}

#[derive(Clone)]
pub struct Client {
    pub(crate) balance_list: Vec<Endpoint>,
    client: DgraphClient<Channel>,
}

impl Debug for Client {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "AsyncDgraphClient")
    }
}

impl Client {
    pub async fn new<S: TryInto<Endpoint>>(
        endpoints: impl Iterator<Item = S>,
    ) -> Result<Self, Failure> {
        let mut balance_list: Vec<Endpoint> = Vec::new();
        for maybe_endpoint in endpoints {
            let endpoint = match maybe_endpoint.try_into() {
                Ok(endpoint) => endpoint,
                Err(_err) => {
                    return Err(ClientError::InvalidEndpoint.into());
                }
            };
            balance_list.push(endpoint);
        }
        if balance_list.is_empty() {
            return Err(ClientError::NoEndpointsDefined.into());
        }
        let channel = Channel::balance_list(balance_list.clone().into_iter());
        let client = DgraphClient::new(channel);
        Ok(Self {
            client,
            balance_list,
        })
    }

    pub fn new_txn(&self) -> Txn {
        Txn::new(self.clone())
    }
}

#[async_trait]
impl IDgraphClient for Client {
    async fn login(
        &mut self,
        user_id: String,
        password: String,
    ) -> ClientResult<DgraphResponse, Status> {
        let login = LoginRequest {
            userid: user_id,
            password,
            ..Default::default()
        };
        let request = Request::new(login);
        let response: Response<DgraphResponse> = self.client.login(request).await?;
        Ok(response.into_inner())
    }

    async fn query(&mut self, query: DgraphRequest) -> ClientResult<DgraphResponse, Status> {
        let request = Request::new(query);
        let response: Response<DgraphResponse> = self.client.query(request).await?;
        Ok(response.into_inner())
    }

    async fn mutate(&mut self, mu: Mutation) -> ClientResult<Assigned, Status> {
        let request = Request::new(mu);
        let response: Response<Assigned> = self.client.mutate(request).await?;
        Ok(response.into_inner())
    }

    async fn alter(&mut self, op: Operation) -> ClientResult<Payload, Status> {
        let request = Request::new(op);
        let response: Response<Payload> = self.client.alter(request).await?;
        Ok(response.into_inner())
    }

    async fn commit_or_abort(&mut self, txn: TxnContext) -> ClientResult<TxnContext, Status> {
        let request = Request::new(txn);
        let response: Response<TxnContext> = self.client.commit_or_abort(request).await?;
        Ok(response.into_inner())
    }

    async fn check_version(&mut self) -> ClientResult<Version, Status> {
        let request = Request::new(Check {});
        let response: Response<Version> = self.client.check_version(request).await?;
        Ok(response.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    fn alter() {
        let mut rt = Runtime::new().unwrap();
        let mut client = rt
            .block_on(Client::new(vec!["http://127.0.0.1:19080"].into_iter()))
            .unwrap();
        let op = Operation {
            schema: "name: string @index(exact) .".into(),
            ..Default::default()
        };
        let response = rt.block_on(client.alter(op));
        assert!(response.is_ok());
    }
}
