use crate::client::lazy::{LazyChannel, LazyClient};
use crate::client::{balance_list, rnd_item, ClientState, ClientVariant, IClient};
use crate::{Endpoints, Result, TxnType};
use async_trait::async_trait;
use failure::Error;
use http::Uri;
use std::convert::TryInto;
use std::sync::Arc;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

///
/// Lazy initialization of gRPC channel with TLS
///
#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct LazyTlsChannel {
    uri: Uri,
    tls: Arc<ClientTlsConfig>,
    channel: Option<Channel>,
}

impl LazyTlsChannel {
    fn new(uri: Uri, tls: Arc<ClientTlsConfig>) -> Self {
        Self {
            uri,
            tls,
            channel: None,
        }
    }
}

#[async_trait]
impl LazyChannel for LazyTlsChannel {
    async fn channel(&mut self) -> Result<Channel, Error> {
        if let Some(channel) = &self.channel {
            Ok(channel.to_owned())
        } else {
            let endpoint: Endpoint = self.uri.to_owned().into();
            let channel = endpoint
                .tls_config(self.tls.as_ref().clone())
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

impl TlsClient {
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
    ) -> Result<Self, Error> {
        let server_root_ca_cert = Certificate::from_pem(server_root_ca_cert.into());
        let client_identity = Identity::from_pem(client_cert.into(), client_key.into());
        let tls = ClientTlsConfig::new()
            .ca_certificate(server_root_ca_cert)
            .identity(client_identity);
        let tls = Arc::new(tls);
        let extra = Tls {
            clients: balance_list(endpoints)?
                .into_iter()
                .map(|uri| LazyClient::new(LazyTlsChannel::new(uri, Arc::clone(&tls))))
                .collect(),
        };
        let state = Box::new(ClientState::new());
        Ok(Self { state, extra })
    }
}
