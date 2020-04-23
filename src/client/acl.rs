use crate::api::dgraph_client::DgraphClient;
use crate::api::{IDgraphClient, Jwt, LoginRequest};
use crate::client::lazy::{ILazyClient, LazyChannel};
use crate::client::{rnd_item, ClientVariant, IClient};
use crate::Result;
use async_trait::async_trait;
use failure::Error;
use prost::Message;
use std::sync::Mutex;
use tonic::codegen::Arc;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;

///
/// Acl gRPC lazy Dgraph client
///
#[derive(Clone, Debug)]
pub struct LazyAclClient<C: LazyChannel> {
    channel: C,
    access_jwt: Arc<Mutex<String>>,
    client: Option<DgraphClient<Channel>>,
}

impl<C: LazyChannel> LazyAclClient<C> {
    pub fn new(channel: C, access_jwt: Arc<Mutex<String>>) -> Self {
        Self {
            channel,
            access_jwt,
            client: None,
        }
    }

    async fn init(&mut self) -> Result<(), Error> {
        if self.client.is_none() {
            let channel = self.channel.channel().await?;
            let access_jwt = Arc::clone(&self.access_jwt);
            let client = DgraphClient::with_interceptor(channel, move |mut req: Request<()>| {
                let token = {
                    let access_jwt = access_jwt.lock().unwrap();
                    MetadataValue::from_str(&access_jwt).expect("gRPC metadata")
                };
                req.metadata_mut().insert("authorization", token.clone());
                Ok(req)
            });
            self.client.replace(client.to_owned());
        }
        Ok(())
    }
}

#[async_trait]
impl<C: LazyChannel> ILazyClient for LazyAclClient<C> {
    type Channel = C;

    async fn client(&mut self) -> Result<&mut DgraphClient<Channel>, Error> {
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
pub struct Acl<C: LazyChannel> {
    access_jwt: Arc<Mutex<String>>,
    refresh_jwt: Mutex<String>,
    clients: Vec<LazyAclClient<C>>,
}

#[async_trait]
impl<C: LazyChannel> IClient for Acl<C> {
    type Client = LazyAclClient<Self::Channel>;
    type Channel = C;

    fn client(&self) -> Self::Client {
        rnd_item(&self.clients)
    }

    fn clients(self) -> Vec<Self::Client> {
        self.clients
    }
}

///
/// Logged client.
///
pub type AclClient<C> = ClientVariant<Acl<C>>;

impl<S: IClient> ClientVariant<S> {
    ///
    /// Try to login. If login is success than consume original client and return client with acl turn on.
    ///
    /// # Arguments
    ///
    /// * `user_id`: User ID
    /// * `password`: User password
    ///
    /// # Errors
    ///
    ///
    /// # Examples
    ///
    /// ```
    /// use dgraph_tonic::Client;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
    ///     let logged = client.login("groot", "password").await.expect("Logged in");
    ///     // now you can use logged client for all operations over DB
    ///     Ok(())
    /// }
    /// ```
    ///
    pub async fn login<T: Into<String>>(
        self,
        user_id: T,
        password: T,
    ) -> Result<AclClient<S::Channel>, Error> {
        let mut stub = self.any_stub();
        let login = LoginRequest {
            userid: user_id.into(),
            password: password.into(),
            ..Default::default()
        };
        let resp = stub.login(login).await?;
        let jwt: Jwt = Jwt::decode(resp.json.as_slice())?;
        let access_jwt = Arc::new(Mutex::new(jwt.access_jwt));
        let clients = self
            .extra
            .clients()
            .into_iter()
            .map(|client| {
                let channel = client.channel();
                let client = LazyAclClient::new(channel, Arc::clone(&access_jwt));
                client
            })
            .collect::<Vec<LazyAclClient<S::Channel>>>();
        Ok(AclClient {
            state: self.state,
            extra: Acl {
                clients,
                access_jwt,
                refresh_jwt: Mutex::new(jwt.refresh_jwt),
            },
        })
    }
}

impl<C: LazyChannel> AclClient<C> {
    ///
    /// Return JWT refresh token
    ///
    pub fn refresh_jwt(&self) -> String {
        let token = self.extra.refresh_jwt.lock().unwrap();
        token.to_owned()
    }

    ///
    /// Try refresh actual login JWT tokens with new ones.
    ///
    /// # Errors
    ///
    /// * gRPC communication error
    /// * Dgraph error when refresh token is not valid
    ///
    /// # Examples
    ///
    /// ```
    /// use dgraph_tonic::Client;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
    ///     let logged = client.login("groot", "password").await.expect("Logged in");
    ///     // now you can use logged client for all operations over DB
    ///     logged.refresh_login().await.expect("Refreshed login");
    ///     Ok(())
    /// }
    /// ```
    ///
    pub async fn refresh_login(&self) -> Result<(), Error> {
        let mut stub = self.any_stub();
        let refresh_token = (&*self.extra.refresh_jwt.lock().unwrap()).to_owned();
        let login = LoginRequest {
            refresh_token,
            ..Default::default()
        };
        let resp = stub.login(login).await?;
        let jwt: Jwt = Jwt::decode(resp.json.as_slice())?;
        {
            let mut access_jwt = self.extra.access_jwt.lock().unwrap();
            *access_jwt = jwt.access_jwt;
        }
        {
            let mut refresh_jwt = self.extra.refresh_jwt.lock().unwrap();
            *refresh_jwt = jwt.refresh_jwt;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::Client;

    #[tokio::test]
    async fn login() {
        let client = Client::new("http://127.0.0.1:19080")
            .unwrap()
            .login("groot", "password")
            .await;
        if let Err(err) = &client {
            dbg!(err);
        }
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn refresh_login() {
        let client = Client::new("http://127.0.0.1:19080")
            .unwrap()
            .login("groot", "password")
            .await
            .expect("logged");
        let refresh = client.refresh_login().await;
        assert!(refresh.is_ok());
    }
}
