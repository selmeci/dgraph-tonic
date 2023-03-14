use std::convert::TryInto;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use http::Uri;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::ClientTlsConfig;
use tonic::Request;

use crate::api::dgraph_client::DgraphClient as DClient;
use crate::client::lazy::{ILazyChannel, ILazyClient};
use crate::client::tls::LazyTlsChannel;
use crate::client::{rnd_item, ClientVariant, DgraphClient, DgraphInterceptorClient, IClient};
use crate::{
    EndpointConfig, Endpoints, Status, TlsClient, TxnBestEffortType, TxnMutatedType,
    TxnReadOnlyType, TxnType,
};

#[derive(Clone, Debug)]
pub struct SlashQlInterceptor {
    api_key: Arc<String>,
}

impl Interceptor for SlashQlInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let api_key = MetadataValue::from_str(&self.api_key).expect("gRPC metadata");
        request.metadata_mut().insert("authorization", api_key);
        Ok(request)
    }
}

pub type DgraphSlashQlClient = DgraphInterceptorClient<SlashQlInterceptor>;

///
/// SlashQL gRPC lazy Dgraph client
///
#[derive(Clone, Debug)]
pub struct LazySlashQlClient {
    channel: LazyTlsChannel,
    api_key: Arc<String>,
    client: Option<DgraphClient>,
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
            let interceptor = SlashQlInterceptor { api_key };
            let client = DgraphClient::SlashQl {
                client: DClient::with_interceptor(channel, interceptor),
            };
            self.client.replace(client);
        }
        Ok(())
    }
}

#[async_trait]
impl ILazyClient for LazySlashQlClient {
    type Channel = LazyTlsChannel;

    async fn client(&mut self) -> Result<&mut DgraphClient> {
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
    fn lift_client<T: Into<String>>(api_key: T, tls_client: Self) -> Result<SlashQlClient> {
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
            extra: SlashQl { clients },
        })
    }

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
        let tls = Arc::new(ClientTlsConfig::new());
        let tls_client = Self::init(endpoints, tls, None)?;
        Self::lift_client(api_key, tls_client)
    }

    ///
    /// New gRPC [SlashQL](https://dgraph.io/slash-graphql) client with endpoint config.
    ///
    /// If your SlashQL endpoint is `https://app.eu-central-1.aws.cloud.dgraph.io/graphql` than connection endpoint for gRPC client is `http://app.grpc.eu-central-1.aws.cloud.dgraph.io:443`
    ///
    ///
    /// # Arguments
    ///
    /// * `endpoints` - one endpoint or vector of endpoints
    /// * `api_key` -  API Key for SlashQL
    /// * `endpoint_config` - custom endpoint configuration
    ///
    /// # Errors
    ///
    /// * endpoints vector is empty
    /// * item in vector cannot by converted into Uri
    ///
    /// # Examples
    ///
    /// ```
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
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let endpoint_config = EndpointWithTimeout::default();
    ///     let client = TlsClient::for_slash_ql_with_endpoint_config(
    ///             "http://app.eu-central-1.aws.cloud.dgraph.io:443",
    ///             "API_KEY",
    ///         endpoint_config).expect("Dgraph client");
    ///     // now you can use client for all operations over DB
    ///     Ok(())
    /// }
    /// ```
    ///
    pub fn for_slash_ql_with_endpoint_config<
        S: TryInto<Uri>,
        E: Into<Endpoints<S>>,
        T: Into<String>,
        C: EndpointConfig + 'static,
    >(
        endpoints: E,
        api_key: T,
        endpoint_config: C,
    ) -> Result<SlashQlClient> {
        let tls = Arc::new(ClientTlsConfig::new());
        let tls_client = Self::init(endpoints, tls, Some(Arc::new(endpoint_config)))?;
        Self::lift_client(api_key, tls_client)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Endpoint, EndpointConfig, TlsClient};
    use std::time::Duration;

    #[derive(Debug, Default)]
    struct EndpointWithTimeout {}

    impl EndpointConfig for EndpointWithTimeout {
        fn configure_endpoint(&self, endpoint: Endpoint) -> Endpoint {
            endpoint.timeout(Duration::from_secs(5))
        }
    }

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

    //#[tokio::test]
    #[allow(dead_code)]
    async fn for_slash_ql_with_endpoint_config() {
        let endpoint_config = EndpointWithTimeout::default();
        let client = TlsClient::for_slash_ql_with_endpoint_config(
            "http://app.grpc.eu-central-1.aws.cloud.dgraph.io:443",
            "API_KEY",
            endpoint_config,
        )
        .unwrap();
        let version = client.check_version().await;
        assert!(version.is_ok());
    }
}
