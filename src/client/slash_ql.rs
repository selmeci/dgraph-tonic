use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::Request;

use crate::api::dgraph_client::DgraphClient;
use crate::client::lazy::{ILazyChannel, ILazyClient};
use crate::client::tls::LazyTlsChannel;
use crate::client::{rnd_item, ClientVariant, IClient};
use crate::{Endpoints, TlsClient, TxnBestEffortType, TxnMutatedType, TxnReadOnlyType, TxnType};
use http::Uri;
use rustls::{
    Certificate as RustCertificate, RootCertStore, ServerCertVerified, ServerCertVerifier, TLSError,
};
use std::convert::TryInto;
use webpki::DNSNameRef;

const ALPN_H2: &str = "h2";

impl ServerCertVerifier for InsecureVerifier {
    ///
    /// Allow any server certificate
    ///
    fn verify_server_cert(
        &self,
        _roots: &RootCertStore,
        _presented_certs: &[RustCertificate],
        _dns_name: DNSNameRef,
        _ocsp_response: &[u8],
    ) -> Result<ServerCertVerified, TLSError> {
        Ok(ServerCertVerified::assertion())
    }
}

pub struct InsecureVerifier {}

///
/// SlashQL gRPC lazy Dgraph client
///
#[derive(Clone, Debug)]
pub struct LazySlashQlClient {
    channel: LazyTlsChannel,
    api_key: Arc<String>,
    client: Option<DgraphClient<Channel>>,
}

impl LazySlashQlClient {
    pub fn new(channel: LazyTlsChannel, api_key: Arc<String>) -> Self {
        Self {
            channel,
            api_key,
            client: None,
        }
    }

    async fn init(&mut self) -> Result<()> {
        if self.client.is_none() {
            let channel = self.channel.channel().await?;
            let api_key = Arc::clone(&self.api_key);
            let client = DgraphClient::with_interceptor(channel, move |mut req: Request<()>| {
                let api_key = MetadataValue::from_str(&api_key).expect("gRPC metadata");
                req.metadata_mut().insert("authorization", api_key);
                Ok(req)
            });
            self.client.replace(client);
        }
        Ok(())
    }
}

#[async_trait]
impl ILazyClient for LazySlashQlClient {
    type Channel = LazyTlsChannel;

    async fn client(&mut self) -> Result<&mut DgraphClient<Channel>> {
        self.init().await?;
        if let Some(client) = &mut self.client {
            Ok(client)
        } else {
            unreachable!()
        }
    }

    fn channel(self) -> Self::Channel {
        self.channel
    }
}

///
/// Inner state for logged Client
///
#[derive(Debug)]
#[doc(hidden)]
pub struct SlashQl {
    api_key: Arc<String>,
    clients: Vec<LazySlashQlClient>,
}

#[async_trait]
impl IClient for SlashQl {
    type Client = LazySlashQlClient;
    type Channel = LazyTlsChannel;

    fn client(&self) -> Self::Client {
        rnd_item(&self.clients)
    }

    fn clients(self) -> Vec<Self::Client> {
        self.clients
    }
}

///
/// Logged SlashQL client
///
pub type SlashQlClient = ClientVariant<SlashQl>;

///
/// Txn over https for SlashQL
///
pub type TxnSlashQl = TxnType<LazySlashQlClient>;

///
/// Readonly txn over https for SlashQL
///
pub type TxnSlashQlReadOnly = TxnReadOnlyType<LazySlashQlClient>;

///
/// Best effort txn over https for SlashQL
///
pub type TxnSlashQlBestEffort = TxnBestEffortType<LazySlashQlClient>;

///
/// Mutated txn over https for SlashQL
///
pub type TxnSlashQlMutated = TxnMutatedType<LazySlashQlClient>;

impl TlsClient {
    ///
    /// New gRPC [SlashQL](https://dgraph.io/slash-graphql) client.
    ///
    /// If your SlashQL endpoint is `https://app.eu-central-1.aws.cloud.dgraph.io/graphql` than connection endpoint for gRPC client is `http://app.grpc.eu-central-1.aws.cloud.dgraph.io:443`
    ///
    ///
    /// # Arguments
    ///
    /// * `endpoints` - one endpoint or vector of endpoints
    /// * `api_key` -  API Key for SlashQL
    ///
    /// # Errors
    ///
    /// * endpoints vector is empty
    /// * item in vector cannot by converted into Uri
    ///
    /// # Examples
    ///
    /// ```
    /// use dgraph_tonic::TlsClient;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = TlsClient::for_slash_ql(
    ///             "http://app.eu-central-1.aws.cloud.dgraph.io:443",
    ///             "API_KEY",
    ///         ).expect("Dgraph client");
    ///     // now you can use client for all operations over DB
    ///     Ok(())
    /// }
    /// ```
    ///
    pub fn for_slash_ql<S: TryInto<Uri>, E: Into<Endpoints<S>>, T: Into<String>>(
        endpoints: E,
        api_key: T,
    ) -> Result<SlashQlClient> {
        let mut config = rustls::ClientConfig::new();
        config.set_protocols(&[Vec::from(&ALPN_H2[..])]);
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(InsecureVerifier {}));
        let tls = Arc::new(ClientTlsConfig::new().rustls_client_config(config));
        let tls_client = Self::init(endpoints, tls)?;
        let api_key = Arc::new(api_key.into());
        let clients = tls_client
            .extra
            .clients()
            .into_iter()
            .map(|client| {
                let channel = client.channel();
                LazySlashQlClient::new(channel, Arc::clone(&api_key))
            })
            .collect::<Vec<LazySlashQlClient>>();
        Ok(SlashQlClient {
            state: tls_client.state,
            extra: SlashQl { clients, api_key },
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::TlsClient;

    //#[tokio::test]
    #[allow(dead_code)]
    async fn for_slash_ql() {
        let client = TlsClient::for_slash_ql(
            "http://app.grpc.eu-central-1.aws.cloud.dgraph.io:443",
            "API_KEY",
        )
        .unwrap();
        let version = client.check_version().await;
        assert!(version.is_ok());
    }
}
