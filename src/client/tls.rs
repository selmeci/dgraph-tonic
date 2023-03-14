use std::convert::TryInto;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use http::Uri;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::client::lazy::{ILazyChannel, LazyClient};
use crate::client::{balance_list, rnd_item, ClientState, ClientVariant, EndpointConfig, IClient};
use crate::{Endpoints, TxnBestEffortType, TxnMutatedType, TxnReadOnlyType, TxnType};

///
/// Lazy initialization of gRPC channel with TLS
///
#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct LazyTlsChannel {
    uri: Uri,
    endpoint_config: Option<Arc<dyn EndpointConfig>>,
    tls: Arc<ClientTlsConfig>,
    channel: Option<Channel>,
}

impl LazyTlsChannel {
    fn new(uri: Uri, tls: Arc<ClientTlsConfig>) -> Self {
        Self {
            uri,
            tls,
            channel: None,
            endpoint_config: None,
        }
    }

    fn with_endpoint_config(mut self, endpoint_config: Option<Arc<dyn EndpointConfig>>) -> Self {
        self.endpoint_config = endpoint_config;
        self
    }
}

#[async_trait]
impl ILazyChannel for LazyTlsChannel {
    async fn channel(&mut self) -> Result<Channel> {
        if let Some(channel) = &self.channel {
            Ok(channel.to_owned())
        } else {
            let mut endpoint: Endpoint = self.uri.to_owned().into();
            if let Some(endpoint_config) = &self.endpoint_config {
                endpoint = endpoint_config.configure_endpoint(endpoint);
            }
            let channel = endpoint
                .tls_config(self.tls.as_ref().clone())?
                .connect()
                .await?;
            self.channel.replace(channel.to_owned());
            Ok(channel)
        }
    }
}

///
/// Inner state for Tls Client
///
#[derive(Debug)]
#[doc(hidden)]
pub struct Tls {
    clients: Vec<LazyClient<LazyTlsChannel>>,
}

#[async_trait]
impl IClient for Tls {
    type Client = LazyClient<Self::Channel>;
    type Channel = LazyTlsChannel;

    fn client(&self) -> Self::Client {
        rnd_item(&self.clients)
    }

    fn clients(self) -> Vec<Self::Client> {
        self.clients
    }
}

///
/// Client with TLS authorization
///
pub type TlsClient = ClientVariant<Tls>;

///
/// Txn with tls
///
pub type TxnTls = TxnType<LazyClient<LazyTlsChannel>>;

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
    fn init_tls<V: Into<Vec<u8>>>(
        server_root_ca_cert: V,
        client_cert: V,
        client_key: V,
    ) -> Arc<ClientTlsConfig> {
        let server_root_ca_cert = Certificate::from_pem(server_root_ca_cert.into());
        let client_identity = Identity::from_pem(client_cert.into(), client_key.into());
        let tls = ClientTlsConfig::new()
            .ca_certificate(server_root_ca_cert)
            .identity(client_identity);
        Arc::new(tls)
    }

    pub(crate) fn init<S: TryInto<Uri>, E: Into<Endpoints<S>>>(
        endpoints: E,
        tls: Arc<ClientTlsConfig>,
        endpoint_config: Option<Arc<dyn EndpointConfig>>,
    ) -> Result<Self> {
        let extra = Tls {
            clients: balance_list(endpoints)?
                .into_iter()
                .map(|uri| {
                    LazyClient::new(
                        LazyTlsChannel::new(uri, Arc::clone(&tls))
                            .with_endpoint_config(endpoint_config.clone()),
                    )
                })
                .collect(),
        };
        let state = Box::new(ClientState::new());
        Ok(Self { state, extra })
    }

    ///
    /// Create new Dgraph client authorized with SSL cert and custom endpoint configuration for interacting v DB.
    ///
    /// The client can be backed by multiple endpoints (to the same server, or multiple servers in a cluster).
    ///
    /// # Arguments
    ///
    /// * `endpoints` - one endpoint or vector of endpoints
    /// * `server_root_ca_cert` - CA certificate
    /// * `client_cert` - Client certificate
    /// * `client_key` - Client key
    /// * `endpoint_config` - custom endpoint configuration
    ///
    /// # Errors
    ///
    /// * endpoints vector is empty
    /// * item in vector cannot by converted into Uri
    ///
    /// # Example
    ///
    /// ```no_run
    /// use dgraph_tonic::{Endpoint, EndpointConfig, TlsClient};
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
    /// #[tokio::main]
    /// async fn main() {
    ///     let server_root_ca_cert = tokio::fs::read("path/to/ca.crt").await.expect("CA cert");
    ///     let client_cert = tokio::fs::read("path/to/client.crt").await.expect("Client cert");
    ///     let client_key = tokio::fs::read("path/to/ca.key").await.expect("Client key");
    ///     let endpoint_config = EndpointWithTimeout::default();
    ///     // vector of endpoints
    ///     let client = TlsClient::new_with_endpoint_config(
    ///             vec!["http://127.0.0.1:19080", "http://127.0.0.1:19080"],
    ///             server_root_ca_cert,
    ///             client_cert,
    ///             client_key,
    ///             endpoint_config)
    ///         .expect("Dgraph TLS client");
    /// }
    /// ```
    ///
    pub fn new_with_endpoint_config<
        S: TryInto<Uri>,
        E: Into<Endpoints<S>>,
        V: Into<Vec<u8>>,
        C: EndpointConfig + 'static,
    >(
        endpoints: E,
        server_root_ca_cert: V,
        client_cert: V,
        client_key: V,
        endpoint_config: C,
    ) -> Result<Self> {
        let tls = Self::init_tls(server_root_ca_cert, client_cert, client_key);
        Self::init(endpoints, tls, Some(Arc::new(endpoint_config)))
    }

    ///
    /// Create new Dgraph client authorized with SSL cert for interacting v DB.
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
    /// use dgraph_tonic::TlsClient;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let server_root_ca_cert = tokio::fs::read("path/to/ca.crt").await.expect("CA cert");
    ///     let client_cert = tokio::fs::read("path/to/client.crt").await.expect("Client cert");
    ///     let client_key = tokio::fs::read("path/to/ca.key").await.expect("Client key");
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
        let tls = Self::init_tls(server_root_ca_cert, client_cert, client_key);
        Self::init(endpoints, tls, None)
    }
}
