use std::convert::TryInto;

use anyhow::Result;
use async_trait::async_trait;
use http::Uri;
use std::fmt::Debug;

use crate::client::lazy::LazyClient;
#[cfg(feature = "acl")]
use crate::client::AclClientType;
use crate::client::{Client as AsyncClient, IClient as IAsyncClient, LazyChannel};
use crate::sync::client::{ClientState, ClientVariant, IClient};
use crate::sync::txn::{TxnBestEffortType, TxnMutatedType, TxnReadOnlyType, TxnType as SyncTxn};
use crate::txn::TxnType;
use crate::{EndpointConfig, Endpoints};

///
/// Inner state for default Client
///
#[derive(Debug)]
#[doc(hidden)]
pub struct Default {
    async_client: AsyncClient,
}

#[async_trait]
impl IClient for Default {
    type AsyncClient = AsyncClient;

    type Client = LazyClient<Self::Channel>;
    type Channel = LazyChannel;

    fn client(&self) -> Self::Client {
        self.async_client.extra.client()
    }

    fn clients(self) -> Vec<Self::Client> {
        self.async_client.extra.clients()
    }

    fn async_client_ref(&self) -> &Self::AsyncClient {
        &self.async_client
    }

    fn async_client(self) -> Self::AsyncClient {
        self.async_client
    }

    fn new_txn(&self) -> TxnType<Self::Client> {
        self.async_client_ref().new_txn()
    }

    #[cfg(feature = "acl")]
    async fn login<T: Into<String> + Send + Sync>(
        self,
        user_id: T,
        password: T,
    ) -> Result<AclClientType<Self::Channel>> {
        self.async_client.login(user_id, password).await
    }

    #[cfg(all(feature = "acl", feature = "dgraph-21-03"))]
    async fn login_into_namespace<T: Into<String> + Send + Sync>(
        self,
        user_id: T,
        password: T,
        namespace: u64,
    ) -> Result<AclClientType<Self::Channel>> {
        self.async_client
            .login_into_namespace(user_id, password, namespace)
            .await
    }
}

///
/// Default client.
///
pub type Client = ClientVariant<Default>;

///
/// Txn over http
///
pub type Txn = SyncTxn<LazyClient<LazyChannel>>;

///
/// Readonly txn over http
///
pub type TxnReadOnly = TxnReadOnlyType<LazyClient<LazyChannel>>;

///
/// Best effort txn over http
///
pub type TxnBestEffort = TxnBestEffortType<LazyClient<LazyChannel>>;

///
/// Mutated txn over http
///
pub type TxnMutated = TxnMutatedType<LazyClient<LazyChannel>>;

impl Client {
    ///
    /// Create new Sync Dgraph client for interacting v DB.
    ///
    /// The client can be backed by multiple endpoints (to the same server, or multiple servers in a cluster).
    ///
    /// # Arguments
    ///
    /// * `endpoints` - one endpoint or vector of endpoints
    ///
    /// # Errors
    ///
    /// * endpoints vector is empty
    /// * item in vector cannot by converted into Uri
    ///
    /// # Example
    ///
    /// ```
    /// use dgraph_tonic::sync::Client;
    ///
    /// // vector of endpoints
    /// let client = Client::new(vec!["http://127.0.0.1:19080", "http://127.0.0.1:19080"]).expect("Dgraph client");
    /// // one endpoint
    /// let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
    /// ```
    ///
    pub fn new<S: TryInto<Uri>, E: Into<Endpoints<S>> + Debug>(endpoints: E) -> Result<Self> {
        let extra = Default {
            async_client: AsyncClient::new(endpoints)?,
        };
        let state = Box::new(ClientState::new());
        Ok(Self { state, extra })
    }

    ///
    /// Create new Sync Dgraph client with custom endpoint configuration for interacting with DB.
    ///
    /// The client can be backed by multiple endpoints (to the same server, or multiple servers in a cluster).
    ///
    /// # Arguments
    ///
    /// * `endpoints` - one endpoint or vector of endpoints
    /// * `endpoint_config` - custom endpoint configuration
    ///
    /// # Errors
    ///
    /// * endpoints vector is empty
    /// * item in vector cannot by converted into Uri
    ///
    /// # Example
    ///
    /// ```
    /// use dgraph_tonic::{Endpoint, EndpointConfig};
    /// use dgraph_tonic::sync::Client;
    ///
    /// use std::time::Duration;
    ///
    /// #[derive(Debug, Default)]
    /// struct EndpointWithTimeout {}
    ///
    /// impl EndpointConfig for EndpointWithTimeout {
    ///     fn configure_endpoint(&self, endpoint: Endpoint) -> Endpoint {
    ///         endpoint.timeout(Duration::from_secs(5))
    ///     }
    /// }
    ///
    /// // vector of endpoints
    /// let client = Client::new(vec!["http://127.0.0.1:19080", "http://127.0.0.1:19080"]).expect("Dgraph client");
    /// // custom configuration
    /// let endpoint_config = EndpointWithTimeout::default();
    /// // one endpoint
    /// let client = Client::new_with_endpoint_config("http://127.0.0.1:19080",endpoint_config).expect("Dgraph client");
    /// ```
    ///
    pub fn new_with_endpoint_config<
        S: TryInto<Uri>,
        E: Into<Endpoints<S>> + Debug,
        C: EndpointConfig + 'static,
    >(
        endpoints: E,
        endpoint_config: C,
    ) -> Result<Self> {
        let extra = Default {
            async_client: AsyncClient::new_with_endpoint_config(endpoints, endpoint_config)?,
        };
        let state = Box::new(ClientState::new());
        Ok(Self { state, extra })
    }
}
