use std::convert::TryInto;

use failure::Error;
use http::Uri;
use rand::Rng;

pub use crate::client::endpoints::Endpoints;
use crate::errors::ClientError;
use crate::stub::Stub;
use crate::{
    BestEffortTxn, IDgraphClient, MutatedTxn, Operation, Payload, ReadOnlyTxn, Result, Txn,
};
use async_trait::async_trait;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

#[cfg(feature = "acl")]
pub use crate::client::acl::AclClient;
pub use crate::client::default::Client;
pub(crate) use crate::client::lazy::ILazyClient;
use crate::client::lazy::LazyChannel;
#[cfg(feature = "tls")]
pub use crate::client::tls::TlsClient;

#[cfg(feature = "acl")]
mod acl;
mod default;
mod endpoints;
mod lazy;
#[cfg(feature = "tls")]
mod tls;

pub(crate) fn rnd_item<T: Clone>(items: &Vec<T>) -> T {
    let mut rng = rand::thread_rng();
    let i = rng.gen_range(0, items.len());
    if let Some(item) = items.get(i) {
        item.to_owned()
    } else {
        unreachable!()
    }
}

///
/// Marker for client variant implementation
///
#[async_trait]
pub trait IClient: Debug + Send + Sync {
    type Client: ILazyClient<Channel = Self::Channel>;
    type Channel: LazyChannel;
    ///
    /// Return lazy Dgraph gRPC client
    ///
    fn client(&self) -> Self::Client;

    ///
    /// consume self and return all lazy clients
    ///
    fn clients(self) -> Vec<Self::Client>;
}

///
/// Client state.
///
#[derive(Debug)]
#[doc(hidden)]
pub struct ClientState;

impl ClientState {
    ///
    /// Create new client state with given Dgraph endpoints
    ///
    pub fn new() -> Self {
        Self {}
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
    /// Return new stub with grpc client implemented according to actual variant.
    ///
    ///
    fn any_stub(&self) -> Stub<S::Client> {
        Stub::new(self.extra.client())
    }

    ///
    /// Return transaction in default state, which can be specialized into ReadOnly or Mutated
    ///
    pub fn new_txn(&self) -> Txn<S::Client> {
        Txn::new(self.any_stub())
    }

    ///
    /// Create new transaction which can only do queries.
    ///
    /// Read-only transactions are useful to increase read speed because they can circumvent the
    /// usual consensus protocol.
    ///
    pub fn new_read_only_txn(&self) -> ReadOnlyTxn<S::Client> {
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
    pub fn new_best_effort_txn(&self) -> BestEffortTxn<S::Client> {
        self.new_read_only_txn().best_effort()
    }

    ///
    /// Create new transaction which can do mutate, commit and discard operations
    ///
    pub fn new_mutated_txn(&self) -> MutatedTxn<S::Client> {
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
    ///     let client = Client::new(vec!["http://127.0.0.1:19080"]).expect("Dgraph client");
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
        let mut stub = self.any_stub();
        stub.alter(op).await
    }
}

#[cfg(test)]
mod tests {
    use crate::Client;

    use super::*;

    #[tokio::test]
    async fn alter() {
        let client = Client::new("http://127.0.0.1:19080").unwrap();
        let op = Operation {
            schema: "name: string @index(exact) .".into(),
            ..Default::default()
        };
        let response = client.alter(op).await;
        assert!(response.is_ok());
    }
}
