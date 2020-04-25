use crate::api::IDgraphClient;
#[cfg(feature = "acl")]
use crate::client::AclClient as AsyncAclClient;
use crate::client::ILazyClient;
use crate::stub::Stub;
use crate::{Operation, Payload, Version};
use async_trait::async_trait;
use failure::Error;
use lazy_static::lazy_static;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

use crate::client::lazy::LazyChannel;
#[cfg(feature = "acl")]
pub use crate::sync::client::acl::AclClient;
pub use crate::sync::client::default::Client;
#[cfg(feature = "tls")]
pub use crate::sync::client::tls::TlsClient;
use crate::sync::txn::{BestEffortTxn, MutatedTxn, ReadOnlyTxn, Txn};
use crate::txn::Txn as AsyncTxn;

#[cfg(feature = "acl")]
mod acl;
mod default;
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
#[doc(hidden)]
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

#[async_trait]
pub trait IClient {
    type AsyncClient;
    type Client: ILazyClient<Channel = Self::Channel>;
    type Channel: LazyChannel;

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
    ) -> Result<AsyncAclClient<Self::Channel>, Error>;
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
    /// Return new stub with grpc client implemented according to actual variant.
    ///
    fn any_stub(&self) -> Stub<S::Client> {
        Stub::new(self.extra.client())
    }

    ///
    /// Return transaction in default state, which can be specialized into ReadOnly or Mutated
    ///
    pub fn new_txn(&self) -> Txn<S::Client> {
        let rt = Arc::clone(&self.rt);
        let async_txn = self.extra.new_txn();
        Txn::new(rt, async_txn)
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
    /// Create new transaction which can do mutate, commit and discard operations
    ///
    pub fn new_mutated_txn(&self) -> MutatedTxn<S::Client> {
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
    pub fn new_best_effort_txn(&self) -> BestEffortTxn<S::Client> {
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
    /// use dgraph_tonic::sync::AclClient;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::LazyDefaultChannel;
    ///
    /// #[cfg(not(feature = "acl"))]
    /// fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// fn client() -> AclClient<LazyDefaultChannel> {
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
    pub fn alter(&self, op: Operation) -> Result<Payload, Error> {
        let mut rt = self.rt.lock().expect("Tokio runtime");
        let mut stub = self.any_stub();
        rt.block_on(async move { stub.alter(op).await })
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
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = Client::new(vec!["http://127.0.0.1:19080"]).expect("Dgraph client");
    ///     let version = client.check_version().expect("Version");
    ///     println!("{:#?}", version);
    ///     Ok(())
    /// }
    /// ```
    ///
    pub async fn check_version(&self) -> Result<Version, Error> {
        let mut rt = self.rt.lock().expect("Tokio runtime");
        let mut stub = self.any_stub();
        rt.block_on(async move { stub.check_version().await })
    }
}
