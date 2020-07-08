use crate::api::dgraph_client::DgraphClient;
use crate::{ClientError, Result};
use async_trait::async_trait;
use std::fmt::Debug;
use tonic::transport::Channel;

///
/// gRPC channel is connected only on client request
///
#[async_trait]
pub trait ILazyChannel: Sync + Send + Debug + Clone {
    ///
    /// Try create and connect gRPC channel
    ///
    async fn channel(&mut self) -> Result<Channel, ClientError>;
}

///
/// gRPC client is connected only on first request
///
#[async_trait]
pub trait ILazyClient: Sync + Send + Debug + Clone {
    type Channel: ILazyChannel;

    ///
    /// initialize gRPC client on first use
    ///
    async fn client(&mut self) -> Result<&mut DgraphClient<Channel>, ClientError>;

    ///
    /// Return used lazy channel for client
    ///
    fn channel(self) -> Self::Channel;
}

///
/// gRPC lazy Dgraph client
///
#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct LazyClient<C: ILazyChannel> {
    channel: C,
    client: Option<DgraphClient<Channel>>,
}

impl<C: ILazyChannel> LazyClient<C> {
    pub fn new(channel: C) -> Self {
        Self {
            channel,
            client: None,
        }
    }

    async fn init(&mut self) -> Result<(), ClientError> {
        if self.client.is_none() {
            let client = DgraphClient::new(self.channel.channel().await?);
            self.client.replace(client);
        }
        Ok(())
    }
}

#[async_trait]
impl<C: ILazyChannel> ILazyClient for LazyClient<C> {
    type Channel = C;

    async fn client(&mut self) -> Result<&mut DgraphClient<Channel>, ClientError> {
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
