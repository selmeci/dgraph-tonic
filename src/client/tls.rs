use crate::client::{ClientState, ClientVariant, IClient};
use crate::{Endpoints, Result};
use async_trait::async_trait;
use failure::Error;
use http::Uri;
use std::convert::TryInto;
use std::path::Path;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

///
/// Inner state for Tls Client
///
#[derive(Debug)]
#[doc(hidden)]
pub struct Tls {
    server_root_ca_cert: Vec<u8>,
    client_cert: Vec<u8>,
    client_key: Vec<u8>,
}

#[async_trait]
impl IClient for Tls {
    async fn channel(&self, state: &ClientState) -> Result<Channel, Error> {
        let server_root_ca_cert = Certificate::from_pem(self.server_root_ca_cert.clone());
        let client_identity = Identity::from_pem(self.client_cert.clone(), self.client_key.clone());
        let tls = ClientTlsConfig::new()
            .ca_certificate(server_root_ca_cert)
            .identity(client_identity);
        let endpoint: Endpoint = state.any_endpoint().into();
        Ok(endpoint.tls_config(tls.clone()).connect().await?)
    }
}

///
/// Client with TLS authorization
///
pub type TlsClient = ClientVariant<Tls>;

impl TlsClient {
    ///
    /// Create new Dgraph client authorized with SSL cert for interacting v DB.
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
    /// ```no_run
    /// use dgraph_tonic::TlsClient;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     // vector of endpoints
    ///     let client = TlsClient::new(
    ///             vec!["http://127.0.0.1:19080", "http://127.0.0.1:19080"],
    ///             "path/to/ca.crt",
    ///             "path/to/client.crt",
    ///             "path/to/ca.key")
    ///         .await
    ///         .expect("Connected to Dgraph");
    ///     //one endpoint
    ///     let client = TlsClient::new(
    ///             "http://127.0.0.1:19080",
    ///             "path/to/ca.crt",
    ///             "path/to/client.crt",
    ///             "path/to/ca.key")
    ///         .await
    ///         .expect("Connected to Dgraph");
    /// }
    /// ```
    ///
    pub async fn new<S: TryInto<Uri>, E: Into<Endpoints<S>>>(
        endpoints: E,
        server_root_ca_cert: impl AsRef<Path>,
        client_cert: impl AsRef<Path>,
        client_key: impl AsRef<Path>,
    ) -> Result<Self, Error> {
        let server_root_ca_cert_future = tokio::fs::read(server_root_ca_cert);
        let client_cert_future = tokio::fs::read(client_cert);
        let client_key_future = tokio::fs::read(client_key);
        Ok(Self {
            state: Box::new(ClientState::new(Self::balance_list(endpoints)?)),
            extra: Tls {
                server_root_ca_cert: server_root_ca_cert_future.await?,
                client_cert: client_cert_future.await?,
                client_key: client_key_future.await?,
            },
        })
    }
}
