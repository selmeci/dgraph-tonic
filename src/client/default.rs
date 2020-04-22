use crate::client::{ClientState, ClientVariant, IClient};
use crate::{Endpoint, Endpoints, Result};
use async_trait::async_trait;
use failure::Error;
use http::Uri;
use std::convert::TryInto;
use tonic::transport::Channel;

///
/// Inner state for default Client
///
#[derive(Debug)]
#[doc(hidden)]
pub struct Default;

#[async_trait]
impl IClient for Default {
    async fn channel(&self, state: &ClientState) -> Result<Channel, Error> {
        let endpoint: Endpoint = state.any_endpoint().into();
        Ok(endpoint.connect().await?)
    }
}

///
/// Default client.
///
pub type Client = ClientVariant<Default>;

impl Client {
    ///
    /// Create new Dgraph client for interacting v DB.
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
    /// use dgraph_tonic::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     // vector of endpoints
    ///     let client = Client::new(vec!["http://127.0.0.1:19080", "http://127.0.0.1:19080"]).await.expect("Connected to Dgraph");
    ///     // one endpoint
    ///     let client = Client::new("http://127.0.0.1:19080").await.expect("Connected to Dgraph");
    /// }
    /// ```
    ///
    pub async fn new<S: TryInto<Uri>, E: Into<Endpoints<S>>>(endpoints: E) -> Result<Self, Error> {
        Ok(Self {
            state: Box::new(ClientState::new(Self::balance_list(endpoints)?)),
            extra: Default {},
        })
    }
}
