use std::convert::TryInto;
use std::fmt::{self, Debug, Formatter};
use std::path::Path;

use failure::Error;
use rand::Rng;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
use tonic::Status;

use crate::errors::ClientError;
use crate::stub::Stub;
use crate::{
    BestEffortTxn, DgraphClient, IDgraphClient, MutatedTxn, Operation, Payload, ReadOnlyTxn,
    Result, Txn,
};

#[derive(Clone)]
pub struct Client {
    stubs: Vec<Stub>,
}

impl Debug for Client {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "AsyncDgraphClient")
    }
}

impl Client {
    fn balance_list<S: TryInto<Endpoint>>(endpoints: Vec<S>) -> Result<Vec<Endpoint>, Error> {
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

    fn any_client(&self) -> Stub {
        let mut rng = rand::thread_rng();
        let i = rng.gen_range(0, self.stubs.len());
        if let Some(client) = self.stubs.get(i) {
            client.clone()
        } else {
            unreachable!()
        }
    }

    pub async fn new<S: TryInto<Endpoint>>(endpoints: Vec<S>) -> Result<Self, Error> {
        let balance_list = Self::balance_list(endpoints)?;
        let mut stubs = Vec::with_capacity(balance_list.len());
        for endpoint in balance_list {
            let channel = endpoint.connect().await?;
            stubs.push(Stub::new(DgraphClient::new(channel)));
        }
        Ok(Self { stubs })
    }

    pub async fn new_with_tls_client_auth<S: TryInto<Endpoint>>(
        domain_name: impl Into<String>,
        endpoints: Vec<S>,
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
            stubs.push(Stub::new(DgraphClient::new(channel)));
        }
        Ok(Self { stubs })
    }

    ///
    /// Return transaction in default state, which can be specialized into ReadOnly or Mutated
    ///
    pub fn new_txn(&self) -> Txn {
        Txn::new(self.any_client())
    }

    ///
    /// Return transaction which can only queries
    ///
    pub fn new_read_only_txn(&self) -> ReadOnlyTxn {
        self.new_txn().read_only()
    }

    ///
    /// Return transaction which can performs mutate, commit and discard
    ///
    pub fn new_mutated_txn(&self) -> MutatedTxn {
        self.new_txn().mutated()
    }

    pub async fn alter(&self, op: Operation) -> Result<Payload, Status> {
        let mut stub = self.any_client();
        stub.alter(op).await
    }
}

#[cfg(test)]
mod tests {
    use crate::Client;

    use super::*;

    #[tokio::test]
    async fn alter() {
        let client = Client::new(vec!["http://127.0.0.1:19080"]).await.unwrap();
        let op = Operation {
            schema: "name: string @index(exact) .".into(),
            ..Default::default()
        };
        let response = client.alter(op).await;
        assert!(response.is_ok());
    }
}
