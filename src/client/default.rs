use crate::client::lazy::{ILazyChannel, LazyClient};
use crate::client::{balance_list, rnd_item, ClientError, ClientState, ClientVariant, IClient};
use crate::{
    Endpoint, Endpoints, Result, TxnBestEffortType, TxnMutatedType, TxnReadOnlyType, TxnType,
};
use async_trait::async_trait;
use http::Uri;
use std::convert::TryInto;
use tonic::transport::Channel;

///
/// Lazy initialization of gRPC channel
///
#[derive(Clone, Debug)]
pub struct LazyChannel {
    uri: Uri,
    channel: Option<Channel>,
}

impl LazyChannel {
    fn new(uri: Uri) -> Self {
        Self { uri, channel: None }
    }
}

#[async_trait]
impl ILazyChannel for LazyChannel {
    async fn channel(&mut self) -> Result<Channel, ClientError> {
        if let Some(channel) = &self.channel {
            Ok(channel.to_owned())
        } else {
            let endpoint: Endpoint = self.uri.to_owned().into();
            let channel = endpoint
                .connect()
                .await
                .map_err(|err| ClientError::Transport(err.into()))?;
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
    clients: Vec<LazyClient<LazyChannel>>,
}

#[async_trait]
impl IClient for Default {
    type Client = LazyClient<Self::Channel>;
    type Channel = LazyChannel;

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

///
/// Txn over http
///
pub type Txn = TxnType<LazyClient<LazyChannel>>;

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
    pub fn new<S: TryInto<Uri>, E: Into<Endpoints<S>>>(endpoints: E) -> Result<Self, ClientError> {
        let extra = Default {
            clients: balance_list(endpoints)?
                .into_iter()
                .map(|uri| LazyClient::new(LazyChannel::new(uri)))
                .collect(),
        };
        let state = Box::new(ClientState::new());
        Ok(Self { state, extra })
    }
}
