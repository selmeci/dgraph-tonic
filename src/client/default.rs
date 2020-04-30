use crate::client::lazy::{LazyChannel, LazyClient};
use crate::client::{balance_list, rnd_item, ClientState, ClientVariant, IClient};
use crate::{Endpoint, Endpoints, Result};
use async_trait::async_trait;
use failure::Error;
use http::Uri;
use std::convert::TryInto;
use tonic::transport::Channel;

///
/// Lazy initialization of gRPC channel
///
#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct LazyDefaultChannel {
    uri: Uri,
    channel: Option<Channel>,
}

impl LazyDefaultChannel {
    fn new(uri: Uri) -> Self {
        Self { uri, channel: None }
    }
}

#[async_trait]
impl LazyChannel for LazyDefaultChannel {
    async fn channel(&mut self) -> Result<Channel, Error> {
        if let Some(channel) = &self.channel {
            Ok(channel.to_owned())
        } else {
            let endpoint: Endpoint = self.uri.to_owned().into();
            let channel = endpoint.connect().await?;
            self.channel.replace(channel.to_owned());
            Ok(channel)
        }
    }
}

///
/// Inner state for default Client
///
#[derive(Debug)]
#[doc(hidden)]
pub struct Default {
    clients: Vec<LazyClient<LazyDefaultChannel>>,
}

#[async_trait]
impl IClient for Default {
    type Client = LazyClient<Self::Channel>;
    type Channel = LazyDefaultChannel;

    fn client(&self) -> Self::Client {
        rnd_item(&self.clients)
    }

    fn clients(self) -> Vec<Self::Client> {
        self.clients
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
    /// // vector of endpoints
    /// let client = Client::new(vec!["http://127.0.0.1:19080", "http://127.0.0.1:19080"]).expect("Dgraph client");
    /// // one endpoint
    /// let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
    /// ```
    ///
    pub fn new<S: TryInto<Uri>, E: Into<Endpoints<S>>>(endpoints: E) -> Result<Self, Error> {
        let extra = Default {
            clients: balance_list(endpoints)?
                .into_iter()
                .map(|uri| LazyClient::new(LazyDefaultChannel::new(uri)))
                .collect(),
        };
        let state = Box::new(ClientState::new());
        Ok(Self { state, extra })
    }
}
