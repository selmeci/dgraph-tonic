use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use tokio::runtime::Runtime;

use crate::api::IDgraphClient;
use crate::client::lazy::ILazyChannel;
#[cfg(feature = "acl")]
use crate::client::AclClientType as AsyncAclClient;
use crate::client::ILazyClient;
use crate::stub::Stub;
#[cfg(feature = "acl")]
pub use crate::sync::client::acl::{
    AclClient, AclClientType, TxnAcl, TxnAclBestEffort, TxnAclMutated, TxnAlcReadOnly,
};
#[cfg(all(feature = "acl", feature = "tls"))]
pub use crate::sync::client::acl::{
    AclTlsClient, TxnAclTls, TxnAclTlsBestEffort, TxnAclTlsMutated, TxnAclTlsReadOnly,
};
pub use crate::sync::client::default::{Client, Txn, TxnBestEffort, TxnMutated, TxnReadOnly};
#[cfg(feature = "slash-ql")]
pub use crate::sync::client::slash_ql::{
    SlashQl, SlashQlClient, TxnSlashQl, TxnSlashQlBestEffort, TxnSlashQlMutated, TxnSlashQlReadOnly,
};
#[cfg(feature = "tls")]
pub use crate::sync::client::tls::{
    TlsClient, TxnTls, TxnTlsBestEffort, TxnTlsMutated, TxnTlsReadOnly,
};
use crate::sync::txn::{TxnBestEffortType, TxnMutatedType, TxnReadOnlyType, TxnType};
use crate::txn::TxnType as AsyncTxn;
use crate::{Operation, Payload, Version};

#[cfg(feature = "acl")]
mod acl;
mod default;
#[cfg(feature = "slash-ql")]
mod slash_ql;
#[cfg(feature = "tls")]
mod tls;

lazy_static! {
    static ref RT: Arc<Mutex<Runtime>> =
        Arc::new(Mutex::new(Runtime::new().expect("Tokio runtime")));
}

///
/// Client state.
///
#[derive(Debug)]
pub struct ClientState {
    rt: Arc<Mutex<Runtime>>,
}

impl ClientState {
    ///
    /// Create new client state with default async implementation
    ///
    pub fn new() -> Self {
        Self {
            rt: Arc::clone(&*RT),
        }
    }
}

impl Default for ClientState {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
pub trait IClient {
    type AsyncClient;
    type Client: ILazyClient<Channel = Self::Channel>;
    type Channel: ILazyChannel;

    fn client(&self) -> Self::Client;

    fn clients(self) -> Vec<Self::Client>;

    fn async_client_ref(&self) -> &Self::AsyncClient;

    fn async_client(self) -> Self::AsyncClient;

    fn new_txn(&self) -> AsyncTxn<Self::Client>;

    #[cfg(feature = "acl")]
    async fn login<T: Into<String> + Send + Sync>(
        self,
        user_id: T,
        password: T,
    ) -> Result<AsyncAclClient<Self::Channel>>;
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
        let rt = Arc::clone(&self.rt);
        let async_txn = self.extra.new_txn();
        TxnType::new(rt, async_txn)
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
    /// Create new transaction which can do mutate, commit and discard operations
    ///
    pub fn new_mutated_txn(&self) -> TxnMutatedType<C::Client> {
        self.new_txn().mutated()
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
    /// use dgraph_tonic::Operation;
    /// use dgraph_tonic::sync::Client;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::sync::AclClientType;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::LazyChannel;
    ///
    /// #[cfg(not(feature = "acl"))]
    /// fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// fn client() -> AclClientType<LazyChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").expect("Acl client")
    /// }
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = client();
    ///     let op = Operation {
    ///         schema: "name: string @index(exact) .".into(),
    ///         ..Default::default()
    ///     };
    ///     client.alter(op).expect("Schema is not updated");
    ///     Ok(())
    /// }
    /// ```
    ///
    pub fn alter(&self, op: Operation) -> Result<Payload> {
        let rt = self.rt.lock().expect("Tokio runtime");
        let mut stub = self.any_stub();
        rt.block_on(async move { stub.alter(op).await })
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
    /// use dgraph_tonic::Operation;
    /// use dgraph_tonic::sync::Client;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::sync::AclClientType;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::LazyChannel;
    ///
    /// #[cfg(not(feature = "acl"))]
    /// fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// fn client() -> AclClientType<LazyChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").expect("Acl client")
    /// }
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = client();
    ///     client.set_schema("name: string @index(exact) .").expect("Schema is not updated");
    ///     Ok(())
    /// }
    /// ```
    ///
    pub fn set_schema<S: Into<String>>(&self, schema: S) -> Result<Payload> {
        let op = Operation {
            schema: schema.into(),
            ..Default::default()
        };
        self.alter(op)
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
    /// use dgraph_tonic::Operation;
    /// use dgraph_tonic::sync::Client;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::sync::AclClientType;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::LazyChannel;
    ///
    /// #[cfg(not(feature = "acl"))]
    /// fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// fn client() -> AclClientType<LazyChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").expect("Acl client")
    /// }
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = client();
    ///     client.set_schema_in_background("name: string @index(exact) .").expect("Schema is not updated");
    ///     Ok(())
    /// }
    /// ```
    ///
    #[cfg(feature = "dgraph-1-1")]
    pub fn set_schema_in_background<S: Into<String>>(&self, schema: S) -> Result<Payload> {
        let op = Operation {
            schema: schema.into(),
            run_in_background: true,
            ..Default::default()
        };
        self.alter(op)
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
    /// use dgraph_tonic::sync::Client;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::sync::AclClientType;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::LazyChannel;
    ///
    /// #[cfg(not(feature = "acl"))]
    /// fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// fn client() -> AclClientType<LazyChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").expect("Acl client")
    /// }
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = client();
    ///     client.drop_all().expect("Data not dropped");
    ///     Ok(())
    /// }
    /// ```
    ///
    pub fn drop_all(&self) -> Result<Payload> {
        let op = Operation {
            drop_all: true,
            ..Default::default()
        };
        self.alter(op)
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
    /// use dgraph_tonic::Operation;
    /// use dgraph_tonic::sync::Client;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = Client::new(vec!["http://127.0.0.1:19080"]).expect("Dgraph client");
    ///     let version = client.check_version().expect("Version");
    ///     println!("{:#?}", version);
    ///     Ok(())
    /// }
    /// ```
    ///
    pub fn check_version(&self) -> Result<Version> {
        let rt = self.rt.lock().expect("Tokio runtime");
        let mut stub = self.any_stub();
        rt.block_on(async move { stub.check_version().await })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "acl")]
    use crate::client::LazyChannel;

    use super::*;

    #[cfg(not(feature = "acl"))]
    fn client() -> Client {
        Client::new("http://127.0.0.1:19080").unwrap()
    }

    #[cfg(feature = "acl")]
    fn client() -> AclClientType<LazyChannel> {
        let default = Client::new("http://127.0.0.1:19080").unwrap();
        default.login("groot", "password").unwrap()
    }

    #[test]
    fn alter() {
        let client = client();
        let op = Operation {
            schema: "name: string @index(exact) .".into(),
            ..Default::default()
        };
        let response = client.alter(op);
        assert!(response.is_ok());
    }

    #[test]
    fn drop_all() {
        let client = client();
        let response = client.drop_all();
        assert!(response.is_ok());
    }

    #[test]
    fn check_version() {
        let client = client();
        let response = client.check_version();
        assert!(response.is_ok());
    }
}
