use std::convert::TryInto;

use anyhow::Result;
use async_trait::async_trait;
use http::Uri;
use std::fmt::Debug;

use crate::client::slash_ql::LazySlashQlClient;
use crate::client::tls::LazyTlsChannel;
#[cfg(feature = "acl")]
use crate::client::AclClientType;
use crate::client::{
    IClient as IAsyncClient, SlashQlClient as AsyncSlashQlClient, TlsClient as AsyncTlsClient,
};
use crate::sync::client::{ClientState, ClientVariant, IClient, TlsClient};
use crate::sync::txn::{TxnBestEffortType, TxnMutatedType, TxnReadOnlyType, TxnType as SyncTxn};
use crate::txn::TxnType;
use crate::{EndpointConfig, Endpoints};

///
/// Inner state for Tls Client
///
#[derive(Debug)]
#[doc(hidden)]
pub struct SlashQl {
    async_client: AsyncSlashQlClient,
}

#[async_trait]
impl IClient for SlashQl {
    type AsyncClient = AsyncSlashQlClient;
    type Client = LazySlashQlClient;
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
/// Client with TLS authorization
///
pub type SlashQlClient = ClientVariant<SlashQl>;

///
/// Txn with tls
///
pub type TxnSlashQl = SyncTxn<LazySlashQlClient>;

///
/// Readonly txn with tls
///
pub type TxnSlashQlReadOnly = TxnReadOnlyType<LazySlashQlClient>;

///
/// Best effort txn with tls
///
pub type TxnSlashQlBestEffort = TxnBestEffortType<LazySlashQlClient>;

///
/// Mutated txn with tls
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
    /// use dgraph_tonic::sync::TlsClient;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
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
        let extra = SlashQl {
            async_client: AsyncTlsClient::for_slash_ql(endpoints, api_key)?,
        };
        let state = Box::new(ClientState::new());
        Ok(SlashQlClient { state, extra })
    }

    ///
    /// New gRPC [SlashQL](https://dgraph.io/slash-graphql) client with custom endpoint configuration.
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
    /// ```no_run
    /// use dgraph_tonic::{Endpoint, EndpointConfig};
    ///
    /// use dgraph_tonic::sync::TlsClient;
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
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let endpoint_config = EndpointWithTimeout::default();
    ///     let client = TlsClient::for_slash_ql_with_endpoint_config(
    ///             "http://app.eu-central-1.aws.cloud.dgraph.io:443",
    ///             "API_KEY",
    ///             endpoint_config
    ///         ).expect("Dgraph client");
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
        let extra = SlashQl {
            async_client: AsyncTlsClient::for_slash_ql_with_endpoint_config(
                endpoints,
                api_key,
                endpoint_config,
            )?,
        };
        let state = Box::new(ClientState::new());
        Ok(SlashQlClient { state, extra })
    }
}

#[cfg(test)]
mod tests {
    use crate::sync::TlsClient;

    //#[test]
    #[allow(dead_code)]
    fn for_slash_ql() {
        let client = TlsClient::for_slash_ql(
            "http://app.grpc.eu-central-1.aws.cloud.dgraph.io:443",
            "API_KEY",
        )
        .unwrap();
        let version = client.check_version();
        dbg!(&version);
        assert!(version.is_ok());
    }
}
