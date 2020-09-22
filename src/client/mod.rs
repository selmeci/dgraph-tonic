use std::convert::TryInto;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

use anyhow::Result;
use http::Uri;
use rand::Rng;

use crate::api::Version;
#[cfg(feature = "acl")]
pub use crate::client::acl::{
    AclClient, AclClientType, TxnAcl, TxnAclBestEffort, TxnAclMutated, TxnAclReadOnly,
};
#[cfg(all(feature = "acl", feature = "tls"))]
pub use crate::client::acl::{
    AclTlsClient, TxnAclTls, TxnAclTlsBestEffort, TxnAclTlsMutated, TxnAclTlsReadOnly,
};
pub use crate::client::default::{
    Client, Http, LazyChannel, Txn, TxnBestEffort, TxnMutated, TxnReadOnly,
};
pub use crate::client::endpoints::Endpoints;
use crate::client::lazy::ILazyChannel;
pub(crate) use crate::client::lazy::ILazyClient;
#[cfg(feature = "tls")]
pub use crate::client::tls::{
    Tls, TlsClient, TxnTls, TxnTlsBestEffort, TxnTlsMutated, TxnTlsReadOnly,
};
use crate::errors::ClientError;
use crate::stub::Stub;
use crate::{
    IDgraphClient, Operation, Payload, TxnBestEffortType, TxnMutatedType, TxnReadOnlyType, TxnType,
};

#[cfg(feature = "acl")]
pub(crate) mod acl;
pub(crate) mod default;
pub(crate) mod endpoints;
pub(crate) mod lazy;
#[cfg(feature = "tls")]
pub(crate) mod tls;

///
/// return random cloned item from vector
///
pub(crate) fn rnd_item<T: Clone>(items: &[T]) -> T {
    let mut rng = rand::thread_rng();
    let i = rng.gen_range(0, items.len());
    if let Some(item) = items.get(i) {
        item.to_owned()
    } else {
        unreachable!()
    }
}

///
/// Check if every endpoint is valid uri and also check if at least one endpoint is given
///
pub(crate) fn balance_list<U: TryInto<Uri>, E: Into<Endpoints<U>>>(
    endpoints: E,
) -> Result<Vec<Uri>> {
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
/// Marker for client variant implementation
///
pub trait IClient: Debug + Send + Sync {
    type Client: ILazyClient<Channel = Self::Channel>;
    type Channel: ILazyChannel;
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
#[derive(Debug, Default)]
pub struct ClientState;

impl ClientState {
    ///
    /// Create new client state
    ///
    pub fn new() -> Self {
        Self::default()
    }
}

///
/// Dgraph client has several variants which offer different behavior.
///
#[derive(Debug)]
pub struct ClientVariant<S: IClient> {
    state: Box<ClientState>,
    pub(crate) extra: S,
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

impl<C: IClient> ClientVariant<C> {
    ///
    /// Return new stub with grpc client implemented according to actual variant.
    ///
    fn any_stub(&self) -> Stub<C::Client> {
        Stub::new(self.extra.client())
    }

    ///
    /// Return transaction in default state, which can be specialized into ReadOnly or Mutated
    ///
    pub fn new_txn(&self) -> TxnType<C::Client> {
        TxnType::new(self.any_stub())
    }

    ///
    /// Create new transaction which can only do queries.
    ///
    /// Read-only transactions are useful to increase read speed because they can circumvent the
    /// usual consensus protocol.
    ///
    pub fn new_read_only_txn(&self) -> TxnReadOnlyType<C::Client> {
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
    pub fn new_best_effort_txn(&self) -> TxnBestEffortType<C::Client> {
        self.new_read_only_txn().best_effort()
    }

    ///
    /// Create new transaction which can do mutate, commit and discard operations
    ///
    pub fn new_mutated_txn(&self) -> TxnMutatedType<C::Client> {
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
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::{AclClientType, LazyChannel};
    ///
    /// #[cfg(not(feature = "acl"))]
    /// async fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// async fn client() -> AclClientType<LazyChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").await.expect("Acl client")
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = client().await;
    ///     let op = Operation {
    ///         schema: "name: string @index(exact) .".into(),
    ///         ..Default::default()
    ///     };
    ///     client.alter(op).await.expect("Schema is not updated");
    ///     Ok(())
    /// }
    /// ```
    ///
    pub async fn alter(&self, op: Operation) -> Result<Payload> {
        let mut stub = self.any_stub();
        stub.alter(op).await
    }

    ///
    /// Create or change the schema.
    ///
    /// # Arguments
    ///
    /// - `schema`: Schema modification
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
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::{AclClientType, LazyChannel};
    ///
    /// #[cfg(not(feature = "acl"))]
    /// async fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// async fn client() -> AclClientType<LazyChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").await.expect("Acl client")
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = client().await;
    ///     client.set_schema("name: string @index(exact) .").await.expect("Schema is not updated");
    ///     Ok(())
    /// }
    /// ```
    ///
    pub async fn set_schema<S: Into<String>>(&self, schema: S) -> Result<Payload> {
        let op = Operation {
            schema: schema.into(),
            ..Default::default()
        };
        self.alter(op).await
    }

    ///
    /// Create or change the schema in background.
    ///
    /// # Arguments
    ///
    /// - `schema`: Schema modification
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
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::{AclClientType, LazyChannel};
    ///
    /// #[cfg(not(feature = "acl"))]
    /// async fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// async fn client() -> AclClientType<LazyChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").await.expect("Acl client")
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = client().await;
    ///     client.set_schema_in_background("name: string @index(exact) .").await.expect("Schema is not updated");
    ///     Ok(())
    /// }
    /// ```
    ///
    #[cfg(feature = "dgraph-1-1")]
    pub async fn set_schema_in_background<S: Into<String>>(&self, schema: S) -> Result<Payload> {
        let op = Operation {
            schema: schema.into(),
            run_in_background: true,
            ..Default::default()
        };
        self.alter(op).await
    }

    ///
    /// Drop all data in DB
    ///
    ///
    /// # Errors
    ///
    /// * gRPC error
    /// * DB reject alter command
    ///
    /// # Example
    ///
    ///
    /// ```
    /// use dgraph_tonic::{Client, Operation};
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::{AclClientType, LazyChannel};
    ///
    /// #[cfg(not(feature = "acl"))]
    /// async fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// async fn client() -> AclClientType<LazyChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").await.expect("Acl client")
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = client().await;
    ///     client.drop_all().await.expect("Data not dropped");
    ///     Ok(())
    /// }
    /// ```
    ///
    pub async fn drop_all(&self) -> Result<Payload> {
        let op = Operation {
            drop_all: true,
            ..Default::default()
        };
        self.alter(op).await
    }

    ///
    /// Check DB version
    ///
    /// # Errors
    ///
    /// * gRPC error
    ///
    /// # Example
    ///
    /// ```
    /// use dgraph_tonic::{Client, Operation};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = Client::new(vec!["http://127.0.0.1:19080"]).expect("Dgraph client");
    ///     let version = client.check_version().await.expect("Version");
    ///     println!("{:#?}", version);
    ///     Ok(())
    /// }
    /// ```
    ///
    pub async fn check_version(&self) -> Result<Version> {
        let mut stub = self.any_stub();
        stub.check_version().await
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "acl")]
    use crate::client::{Client, LazyChannel};

    use super::*;

    #[cfg(not(feature = "acl"))]
    async fn client() -> Client {
        Client::new("http://127.0.0.1:19080").unwrap()
    }

    #[cfg(feature = "acl")]
    async fn client() -> AclClientType<LazyChannel> {
        let default = Client::new("http://127.0.0.1:19080").unwrap();
        default.login("groot", "password").await.unwrap()
    }

    #[tokio::test]
    async fn alter() {
        let client = client().await;
        let op = Operation {
            schema: "name: string @index(exact) .".into(),
            ..Default::default()
        };
        let response = client.alter(op).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn drop_all() {
        let client = client().await;
        let response = client.drop_all().await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn check_version() {
        let client = client().await;
        let response = client.check_version().await;
        assert!(response.is_ok());
    }
}
