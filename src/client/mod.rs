use std::convert::TryInto;

use failure::Error;
use http::Uri;
use rand::Rng;
use tonic::transport::Channel;
use tonic::Status;

use crate::api::Jwt;
pub use crate::client::endpoints::Endpoints;
use crate::errors::ClientError;
use crate::stub::Stub;
use crate::{
    BestEffortTxn, DgraphClient, IDgraphClient, LoginRequest, MutatedTxn, Operation, Payload,
    ReadOnlyTxn, Result, Txn,
};
use async_trait::async_trait;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::Mutex;

pub use crate::client::default::Client;
pub use crate::client::tls::TlsClient;

mod default;
mod endpoints;
mod tls;

#[async_trait]
pub trait IClient: Debug {
    async fn channel(&self, state: &ClientState) -> Result<Channel, Error>;
}

///
/// Client state.
///
#[derive(Debug)]
#[doc(hidden)]
pub struct ClientState {
    endpoints: Vec<Uri>,
    access_jwt: Mutex<Option<String>>,
    refresh_jwt: Mutex<Option<String>>,
}

impl ClientState {
    ///
    /// Create new client state with given Dgraph endpoints
    ///
    pub fn new(endpoints: Vec<Uri>) -> Self {
        Self {
            endpoints,
            access_jwt: Mutex::new(None),
            refresh_jwt: Mutex::new(None),
        }
    }

    ///
    /// Return one of stored uris
    ///
    pub fn any_endpoint(&self) -> Uri {
        let mut rng = rand::thread_rng();
        let i = rng.gen_range(0, self.endpoints.len());
        if let Some(uri) = self.endpoints.get(i) {
            uri.clone()
        } else {
            unreachable!()
        }
    }
}

///
/// Dgraph client has several variants which offer different behavior.
///
pub struct ClientVariant<S: IClient> {
    state: Box<ClientState>,
    extra: S,
}

impl<S: IClient> Deref for ClientVariant<S> {
    type Target = Box<ClientState>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<S: IClient> DerefMut for ClientVariant<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl<S: IClient> ClientVariant<S> {
    ///
    /// Check if every endpoint is valid uri and also check if at least one endpoint is given
    ///
    fn balance_list<U: TryInto<Uri>, E: Into<Endpoints<U>>>(
        endpoints: E,
    ) -> Result<Vec<Uri>, Error> {
        let endpoints: Endpoints<U> = endpoints.into();
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

    ///
    /// Return new stub with channel implemented according to actual client variant.
    ///
    async fn any_stub(&self) -> Result<Stub, Error> {
        let channel = self.extra.channel(&self.state).await?;
        Ok(Stub::new(DgraphClient::new(channel)))
    }

    ///
    /// Return transaction in default state, which can be specialized into ReadOnly or Mutated
    ///
    pub async fn new_txn(&self) -> Result<Txn, Error> {
        Ok(Txn::new(self.any_stub().await?))
    }

    ///
    /// Create new transaction which can only do queries.
    ///
    /// Read-only transactions are useful to increase read speed because they can circumvent the
    /// usual consensus protocol.
    ///
    pub async fn new_read_only_txn(&self) -> Result<ReadOnlyTxn, Error> {
        Ok(self.new_txn().await?.read_only())
    }

    ///
    /// Create new transaction which can only do queries in best effort mode.
    ///
    /// Read-only queries can optionally be set as best-effort. Using this flag will ask the
    /// Dgraph Alpha to try to get timestamps from memory on a best-effort basis to reduce the number
    /// of outbound requests to Zero. This may yield improved latencies in read-bound workloads where
    /// linearizable reads are not strictly needed.
    ///
    pub async fn new_best_effort_txn(&self) -> Result<BestEffortTxn, Error> {
        Ok(self.new_read_only_txn().await?.best_effort())
    }

    ///
    /// Create new transaction which can do mutate, commit and discard operations
    ///
    pub async fn new_mutated_txn(&self) -> Result<MutatedTxn, Error> {
        Ok(self.new_txn().await?.mutated())
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
    pub async fn alter(&self, op: Operation) -> Result<Payload, Error> {
        let mut stub = self.any_stub().await?;
        match stub.alter(op).await {
            Ok(payload) => Ok(payload),
            Err(status) => Err(ClientError::CannotAlter(status).into()),
        }
    }

    pub async fn login<T: Into<String> + Default>(
        &self,
        user_id: Option<T>,
        password: Option<T>,
        refresh_token: Option<T>,
    ) -> Result<Jwt, Status> {
        let mut stub = self.any_stub();
        let login = LoginRequest {
            userid: user_id.unwrap_or_default().into(),
            password: password.unwrap_or_default().into(),
            refresh_token: refresh_token.unwrap_or_default().into(),
            ..Default::default()
        };
        unimplemented!()
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
