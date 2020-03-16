use std::convert::TryInto;
use std::fmt::{self, Debug, Formatter};
use std::path::Path;

use failure::Error;
use rand::Rng;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Request, Response, Status};

use async_trait::async_trait;

pub use crate::async_client::txn::{BestEffortTxn, MutatedTxn, ReadOnlyTxn, Txn};
use crate::errors::ClientError;
use crate::{
    Assigned, Check, ClientResult, DgraphClient, LoginRequest, Mutation, Operation, Payload,
    Request as DgraphRequest, Response as DgraphResponse, TxnContext, Version,
};

mod txn;

#[async_trait]
pub trait IDgraphClient: Clone + Sized {
    async fn login(
        &self,
        user_id: String,
        password: String,
    ) -> ClientResult<DgraphResponse, Status>;

    async fn query(&self, query: DgraphRequest) -> ClientResult<DgraphResponse, Status>;

    async fn mutate(&self, mu: Mutation) -> ClientResult<Assigned, Status>;

    async fn alter(&self, op: Operation) -> ClientResult<Payload, Status>;

    async fn commit_or_abort(&self, txn: TxnContext) -> ClientResult<TxnContext, Status>;

    async fn check_version(&self) -> ClientResult<Version, Status>;
}

#[derive(Clone)]
pub struct Client {
    stubs: Vec<DgraphClient<Channel>>,
}

impl Debug for Client {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "AsyncDgraphClient")
    }
}

impl Client {
    fn balance_list<S: TryInto<Endpoint>>(
        endpoints: impl Iterator<Item = S>,
    ) -> Result<Vec<Endpoint>, Error> {
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
        };
        Ok(balance_list)
    }

    fn any_client(&self) -> DgraphClient<Channel> {
        let mut rng = rand::thread_rng();
        let i = rng.gen_range(0, self.stubs.len());
        if let Some(client) = self.stubs.get(i) {
            client.clone()
        } else {
            unreachable!()
        }
    }

    pub async fn new<S: TryInto<Endpoint>>(
        endpoints: impl Iterator<Item = S>,
    ) -> Result<Self, Error> {
        let balance_list = Self::balance_list(endpoints)?;
        let mut stubs = Vec::with_capacity(balance_list.len());
        for endpoint in balance_list {
            let channel = endpoint.connect().await?;
            stubs.push(DgraphClient::new(channel));
        }
        Ok(Self { stubs })
    }

    pub async fn new_with_tls_client_auth<S: TryInto<Endpoint>>(
        domain_name: impl Into<String>,
        endpoints: impl Iterator<Item = S>,
        server_root_ca_cert: impl AsRef<Path>,
        client_cert: impl AsRef<Path>,
        client_key: impl AsRef<Path>,
    ) -> Result<Self, Error> {
        let server_root_ca_cert_future = tokio::fs::read(server_root_ca_cert);
        let client_cert_future = tokio::fs::read(client_cert);
        let client_key_future = tokio::fs::read(client_key);
        let server_root_ca_cert = Certificate::from_pem(server_root_ca_cert_future.await?);
        let client_identity =
            Identity::from_pem(client_cert_future.await?, client_key_future.await?);
        let tls = ClientTlsConfig::new()
            .domain_name(domain_name)
            .ca_certificate(server_root_ca_cert)
            .identity(client_identity);
        let balance_list = Self::balance_list(endpoints)?;
        let mut stubs = Vec::with_capacity(balance_list.len());
        for endpoint in balance_list {
            let channel = endpoint.tls_config(tls.clone()).connect().await?;
            stubs.push(DgraphClient::new(channel));
        }
        Ok(Self { stubs })
    }

    pub fn new_txn(&self) -> Txn {
        Txn::new(&self)
    }
}

#[async_trait]
impl IDgraphClient for Client {
    async fn login(
        &self,
        user_id: String,
        password: String,
    ) -> ClientResult<DgraphResponse, Status> {
        let login = LoginRequest {
            userid: user_id,
            password,
            ..Default::default()
        };
        let mut client = self.any_client();
        let request = Request::new(login);
        let response: Response<DgraphResponse> = client.login(request).await?;
        Ok(response.into_inner())
    }

    async fn query(&self, query: DgraphRequest) -> ClientResult<DgraphResponse, Status> {
        let mut client = self.any_client();
        let request = Request::new(query);
        let response: Response<DgraphResponse> = client.query(request).await?;
        Ok(response.into_inner())
    }

    async fn mutate(&self, mu: Mutation) -> ClientResult<Assigned, Status> {
        let mut client = self.any_client();
        let request = Request::new(mu);
        let response: Response<Assigned> = client.mutate(request).await?;
        Ok(response.into_inner())
    }

    async fn alter(&self, op: Operation) -> ClientResult<Payload, Status> {
        let mut client = self.any_client();
        let request = Request::new(op);
        let response: Response<Payload> = client.alter(request).await?;
        Ok(response.into_inner())
    }

    async fn commit_or_abort(&self, txn: TxnContext) -> ClientResult<TxnContext, Status> {
        let mut client = self.any_client();
        let request = Request::new(txn);
        let response: Response<TxnContext> = client.commit_or_abort(request).await?;
        Ok(response.into_inner())
    }

    async fn check_version(&self) -> ClientResult<Version, Status> {
        let mut client = self.any_client();
        let request = Request::new(Check {});
        let response: Response<Version> = client.check_version(request).await?;
        Ok(response.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use tokio::runtime::Runtime;

    use super::*;

    #[test]
    fn alter() {
        let mut rt = Runtime::new().unwrap();
        let client = rt
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
