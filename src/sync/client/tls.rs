use std::convert::TryInto;

use anyhow::Result;
use async_trait::async_trait;
use http::Uri;
use std::fmt::Debug;

use crate::client::lazy::LazyClient;
use crate::client::tls::LazyTlsChannel;
use crate::client::{AclClientType, IClient as IAsyncClient, TlsClient as AsyncTlsClient};
use crate::sync::client::{ClientState, ClientVariant, IClient};
use crate::sync::txn::{TxnBestEffortType, TxnMutatedType, TxnReadOnlyType, TxnType as SyncTxn};
use crate::txn::TxnType;
use crate::Endpoints;

///
/// Inner state for Tls Client
///
#[derive(Debug)]
#[doc(hidden)]
pub struct Tls {
    async_client: AsyncTlsClient,
}

#[async_trait]
impl IClient for Tls {
    type AsyncClient = AsyncTlsClient;
    type Client = LazyClient<Self::Channel>;
    #[cfg(feature = "acl")]
    type Channel = LazyTlsChannel;

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
}

///
/// Client with TLS authorization
///
pub type TlsClient = ClientVariant<Tls>;

///
/// Txn with tls
///
pub type TxnTls = SyncTxn<LazyClient<LazyTlsChannel>>;

///
/// Readonly txn with tls
///
pub type TxnTlsReadOnly = TxnReadOnlyType<LazyClient<LazyTlsChannel>>;

///
/// Best effort txn with tls
///
pub type TxnTlsBestEffort = TxnBestEffortType<LazyClient<LazyTlsChannel>>;

///
/// Mutated txn with tls
///
pub type TxnTlsMutated = TxnMutatedType<LazyClient<LazyTlsChannel>>;

impl TlsClient {
    ///
    /// Create new Sync Dgraph client authorized with SSL cert for interacting v DB.
    ///
    /// The client can be backed by multiple endpoints (to the same server, or multiple servers in a cluster).
    ///
    /// # Arguments
    ///
    /// * `endpoints` - one endpoint or vector of endpoints
    /// * `server_root_ca_cert` - CA certificate
    /// * `client_cert` - Client certificate
    /// * `client_key` - Client key
    ///
    /// # Errors
    ///
    /// * endpoints vector is empty
    /// * item in vector cannot by converted into Uri
    ///
    /// # Example
    ///
    /// ```no_run
    /// use dgraph_tonic::sync::TlsClient;
    ///
    /// fn main() {
    ///     let server_root_ca_cert = std::fs::read("path/to/ca.crt").expect("CA cert");
    ///     let client_cert = std::fs::read("path/to/client.crt").expect("Client cert");
    ///     let client_key = std::fs::read("path/to/ca.key").expect("Client key");
    ///     // vector of endpoints
    ///     let client = TlsClient::new(
    ///             vec!["http://127.0.0.1:19080", "http://127.0.0.1:19080"],
    ///             server_root_ca_cert,
    ///             client_cert,
    ///             client_key)
    ///         .expect("Dgraph TLS client");
    /// }
    /// ```
    ///
    pub fn new<S: TryInto<Uri>, E: Into<Endpoints<S>>, V: Into<Vec<u8>>>(
        endpoints: E,
        server_root_ca_cert: V,
        client_cert: V,
        client_key: V,
    ) -> Result<Self> {
        let extra = Tls {
            async_client: AsyncTlsClient::new(
                endpoints,
                server_root_ca_cert,
                client_cert,
                client_key,
            )?,
        };
        let state = Box::new(ClientState::new());
        Ok(Self { state, extra })
    }
}
