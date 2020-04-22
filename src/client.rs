use std::convert::TryInto;
use std::path::Path;

use failure::Error;
use http::Uri;
use rand::Rng;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
use tonic::Status;

use crate::errors::ClientError;
use crate::stub::Stub;
use crate::{
    BestEffortTxn, DgraphClient, IDgraphClient, MutatedTxn, Operation, Payload, ReadOnlyTxn,
    Result, Txn,
};

///
/// Helper struct for endpoints input argument in new client function.
/// Allows to create client with one or more endpoints.
///
pub struct Endpoints<S: TryInto<Uri>> {
    endpoints: Vec<S>,
}

impl<S: TryInto<Uri>> From<Vec<S>> for Endpoints<S> {
    fn from(endpoints: Vec<S>) -> Self {
        Self { endpoints }
    }
}

impl<S: TryInto<Uri>> From<S> for Endpoints<S> {
    fn from(endpoint: S) -> Self {
        Self {
            endpoints: vec![endpoint],
        }
    }
}

///
/// Async client for Dgraph DB.
///
#[derive(Clone, Debug)]
pub struct Client {
    stubs: Vec<Stub>,
}

impl Client {
    fn balance_list<S: TryInto<Uri>, E: Into<Endpoints<S>>>(
        endpoints: E,
    ) -> Result<Vec<Uri>, Error> {
        let endpoints: Endpoints<S> = endpoints.into();
        let mut balance_list: Vec<Uri> = Vec::new();
        for maybe_endpoint in endpoints.endpoints {
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

    fn make(stubs: Vec<Stub>) -> Self {
        Self { stubs }
    }

    ///
    /// Create new Dgraph client for interacting v DB and try to connect to given endpoints.
    ///
    /// The client can be backed by multiple endpoints (to the same server, or multiple servers in a cluster).
    ///
    /// # Arguments
    ///
    /// * `endpoints` - one endpoint or vector of endpoints
    ///
    /// # Errors
    ///
    /// * connection to DB fails
    /// * endpoints vector is empty
    /// * item in vector cannot by converted into Uri
    ///
    /// # Example
    ///
    /// ```
    /// use dgraph_tonic::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     // vector of endpoints
    ///     let client = Client::new(vec!["http://127.0.0.1:19080", "http://127.0.0.1:19080"]).await.expect("Connected to Dgraph");
    ///     // one endpoint
    ///     let client = Client::new("http://127.0.0.1:19080").await.expect("Connected to Dgraph");
    /// }
    /// ```
    ///
    pub async fn new<S: TryInto<Uri>, E: Into<Endpoints<S>>>(endpoints: E) -> Result<Self, Error> {
        let balance_list = Self::balance_list(endpoints)?;
        let mut stubs = Vec::with_capacity(balance_list.len());
        for uri in balance_list {
            let endpoint: Endpoint = uri.into();
            let channel = endpoint.connect().await?;
            stubs.push(Stub::new(DgraphClient::new(channel)));
        }
        Ok(Self::make(stubs))
    }

    ///
    /// Create new Dgraph client authorized with SSL cert for interacting v DB and try to connect to given endpoints.
    ///
    /// The client can be backed by multiple endpoints (to the same server, or multiple servers in a cluster).
    ///
    /// # Arguments
    ///
    /// * `endpoints` - one endpoint or vector of endpoints
    /// * `server_root_ca_cert` - path to server CA certificate
    /// * `client_cert` - path to client certificate
    /// * `client_key` - path to client private key
    ///
    /// # Errors
    ///
    /// * connection to DB fails
    /// * endpoints vector is empty
    /// * item in vector cannot by converted into Uri
    ///
    /// # Example
    ///
    /// ```
    /// use dgraph_tonic::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     // vector of endpoints
    ///     let client = Client::new_with_tls_client_auth(
    ///             vec!["http://127.0.0.1:19080", "http://127.0.0.1:19080"],
    ///             "path/to/ca.crt",
    ///             "path/to/client.crt",
    ///             "path/to/ca.key")
    ///         .await
    ///         .expect("Connected to Dgraph");
    ///     //one endpoint
    ///     let client = Client::new_with_tls_client_auth(
    ///             "http://127.0.0.1:19080",
    ///             "path/to/ca.crt",
    ///             "path/to/client.crt",
    ///             "path/to/ca.key")
    ///         .await
    ///         .expect("Connected to Dgraph");
    /// }
    /// ```
    ///
    pub async fn new_with_tls_client_auth<S: TryInto<Uri>, E: Into<Endpoints<S>>>(
        endpoints: E,
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
            .ca_certificate(server_root_ca_cert)
            .identity(client_identity);
        let balance_list: Vec<Uri> = Self::balance_list(endpoints)?;
        let mut stubs = Vec::with_capacity(balance_list.len());
        for uri in balance_list {
            let tls = tls
                .clone()
                .domain_name(uri.host().expect("host in endpoint"));
            let endpoint: Endpoint = uri.into();
            let channel = endpoint.tls_config(tls.clone()).connect().await?;
            stubs.push(Stub::new(DgraphClient::new(channel)));
        }
        Ok(Self::make(stubs))
    }

    ///
    /// Return transaction in default state, which can be specialized into ReadOnly or Mutated
    ///
    pub fn new_txn(&self) -> Txn {
        Txn::new(self.any_client())
    }

    ///
    /// Create new transaction which can only do queries.
    ///
    /// Read-only transactions are useful to increase read speed because they can circumvent the
    /// usual consensus protocol.
    ///
    pub fn new_read_only_txn(&self) -> ReadOnlyTxn {
        self.new_txn().read_only()
    }

    ///
    /// Create new transaction which can only do queries in best effort mode.
    ///
    /// Read-only queries can optionally be set as best-effort. Using this flag will ask the
    /// Dgraph Alpha to try to get timestamps from memory on a best-effort basis to reduce the number
    /// of outbound requests to Zero. This may yield improved latencies in read-bound workloads where
    /// linearizable reads are not strictly needed.
    ///
    pub fn new_best_effort_txn(&self) -> BestEffortTxn {
        self.new_read_only_txn().best_effort()
    }

    ///
    /// Create new transaction which can do mutate, commit and discard operations
    ///
    pub fn new_mutated_txn(&self) -> MutatedTxn {
        self.new_txn().mutated()
    }

    ///
    /// The /alter endpoint is used to create or change the schema.
    ///
    /// # Arguments
    ///
    /// - `op`: Alter operation
    ///
    /// # Errors
    ///
    /// * gRPC error
    /// * DB reject alter command
    ///
    /// # Example
    ///
    /// Install a schema into dgraph. A `name` predicate is string type and has exact index.
    ///
    /// ```
    /// use dgraph_tonic::{Client, Operation};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = Client::new(vec!["http://127.0.0.1:19080"]).await.expect("Connected to Dgraph");
    ///     let op = Operation {
    ///         schema: "name: string @index(exact) .".into(),
    ///         ..Default::default()
    ///     };
    ///     client.alter(op).await.expect("Schema is not updated");
    ///     Ok(())
    /// }
    /// ```
    ///
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
        let client = Client::new("http://127.0.0.1:19080").await.unwrap();
        let op = Operation {
            schema: "name: string @index(exact) .".into(),
            ..Default::default()
        };
        let response = client.alter(op).await;
        assert!(response.is_ok());
    }
}
