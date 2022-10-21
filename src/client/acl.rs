use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::{Request, Status};

use crate::api::dgraph_client::DgraphClient as DClient;
use crate::api::{IDgraphClient, Jwt, LoginRequest};
use crate::client::lazy::{ILazyChannel, ILazyClient};
#[cfg(feature = "tls")]
use crate::client::tls::LazyTlsChannel;
use crate::client::{rnd_item, ClientVariant, DgraphClient, DgraphInterceptorClient, IClient};
use crate::{LazyChannel, TxnBestEffortType, TxnMutatedType, TxnReadOnlyType, TxnType};

#[derive(Clone, Debug)]
pub struct AclInterceptor {
    access_jwt: Arc<Mutex<String>>,
}

impl Interceptor for AclInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let token = {
            let access_jwt = self.access_jwt.lock().unwrap();
            MetadataValue::from_str(&access_jwt).expect("gRPC metadata")
        };
        request.metadata_mut().insert("accessjwt", token);
        Ok(request)
    }
}

pub type DgraphAclClient = DgraphInterceptorClient<AclInterceptor>;

///
/// Acl gRPC lazy Dgraph client
///
#[derive(Clone, Debug)]
pub struct LazyAclClient<C: ILazyChannel> {
    channel: C,
    access_jwt: Arc<Mutex<String>>,
    client: Option<DgraphClient>,
}

impl<C: ILazyChannel> LazyAclClient<C> {
    pub fn new(channel: C, access_jwt: Arc<Mutex<String>>) -> Self {
        Self {
            channel,
            access_jwt,
            client: None,
        }
    }

    async fn init(&mut self) -> Result<()> {
        if self.client.is_none() {
            let channel = self.channel.channel().await?;
            let access_jwt = Arc::clone(&self.access_jwt);
            let interceptor = AclInterceptor { access_jwt };
            let client = DgraphClient::Acl {
                client: DClient::with_interceptor(channel, interceptor),
            };
            self.client.replace(client);
        }
        Ok(())
    }
}

#[async_trait]
impl<C: ILazyChannel> ILazyClient for LazyAclClient<C> {
    type Channel = C;

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
pub struct Acl<C: ILazyChannel> {
    access_jwt: Arc<Mutex<String>>,
    refresh_jwt: Mutex<String>,
    clients: Vec<LazyAclClient<C>>,
}

#[async_trait]
impl<C: ILazyChannel> IClient for Acl<C> {
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
pub type AclClientType<C> = ClientVariant<Acl<C>>;

///
/// Logged default client
///
pub type AclClient = AclClientType<LazyAclClient<LazyChannel>>;

///
/// Logged tls client
///
#[cfg(feature = "tls")]
pub type AclTlsClient = AclClientType<LazyAclClient<LazyTlsChannel>>;

///
/// Txn over http with Acl
///
pub type TxnAcl = TxnType<LazyAclClient<LazyChannel>>;

///
/// Readonly txn over http with Acl
///
pub type TxnAclReadOnly = TxnReadOnlyType<LazyAclClient<LazyChannel>>;

///
/// Best effort txn over http with Acl
///
pub type TxnAclBestEffort = TxnBestEffortType<LazyAclClient<LazyChannel>>;

///
/// Mutated txn over http with Acl
///
pub type TxnAclMutated = TxnMutatedType<LazyAclClient<LazyChannel>>;

///
/// Txn over http with AC:
///
#[cfg(feature = "tls")]
pub type TxnAclTls = TxnType<LazyAclClient<LazyTlsChannel>>;

///
/// Readonly txn over http with Acl
///
#[cfg(feature = "tls")]
pub type TxnAclTlsReadOnly = TxnReadOnlyType<LazyAclClient<LazyTlsChannel>>;

///
/// Best effort txn over http with Acl
///
#[cfg(feature = "tls")]
pub type TxnAclTlsBestEffort = TxnBestEffortType<LazyAclClient<LazyTlsChannel>>;

///
/// Mutated txn over http with Acl
///
#[cfg(feature = "tls")]
pub type TxnAclTlsMutated = TxnMutatedType<LazyAclClient<LazyTlsChannel>>;

struct Login<T: Into<String>> {
    user_id: T,
    password: T,
    #[cfg(feature = "dgraph-21-03")]
    namespace: Option<u64>,
}

impl<S: IClient> ClientVariant<S> {
    async fn do_login<T: Into<String>>(self, login: Login<T>) -> Result<AclClientType<S::Channel>> {
        let mut stub = self.any_stub();
        let login = LoginRequest {
            userid: login.user_id.into(),
            password: login.password.into(),
            #[cfg(feature = "dgraph-21-03")]
            namespace: login.namespace.unwrap_or_default(),
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
                LazyAclClient::new(channel, Arc::clone(&access_jwt))
            })
            .collect::<Vec<LazyAclClient<S::Channel>>>();
        Ok(AclClientType {
            state: self.state,
            extra: Acl {
                clients,
                access_jwt,
                refresh_jwt: Mutex::new(jwt.refresh_jwt),
            },
        })
    }

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
    ) -> Result<AclClientType<S::Channel>> {
        self.do_login(Login {
            password,
            user_id,
            #[cfg(feature = "dgraph-21-03")]
            namespace: None,
        })
        .await
    }

    ///
    /// Try to login. If login is success than consume original client and return client with acl turn on.
    ///
    /// # Arguments
    ///
    /// * `user_id`: User ID
    /// * `password`: User password
    /// * `namespace`: Namespace Id
    ///
    /// # Errors
    ///
    ///
    /// # Examples
    ///
    /// In the example above, the client logs into namespace 0 using username `groot` and password `password`. Once logged in, the client can perform all the operations allowed to the groot user of namespace 0.
    ///
    /// ```
    /// use dgraph_tonic::Client;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
    ///     let logged = client.login_into_namespace("groot", "password", 0).await.expect("Logged in");
    ///     // now you can use logged client for all operations over DB
    ///     Ok(())
    /// }
    /// ```
    ///
    #[cfg(feature = "dgraph-21-03")]
    pub async fn login_into_namespace<T: Into<String>>(
        self,
        user_id: T,
        password: T,
        namespace: u64,
    ) -> Result<AclClientType<S::Channel>> {
        self.do_login(Login {
            password,
            user_id,
            #[cfg(feature = "dgraph-21-03")]
            namespace: Some(namespace),
        })
        .await
    }
}

impl<C: ILazyChannel> AclClientType<C> {
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
    pub async fn refresh_login(&self) -> Result<()> {
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

    #[cfg(feature = "dgraph-21-03")]
    #[tokio::test]
    async fn login_into_namespace() {
        let client = Client::new("http://127.0.0.1:19080")
            .unwrap()
            .login_into_namespace("groot", "password", 0)
            .await;
        if let Err(err) = &client {
            dbg!(err);
        }
        assert!(client.is_ok());
    }

    #[cfg(feature = "dgraph-21-03")]
    #[tokio::test]
    async fn deny_login_into_namespace() {
        let client = Client::new("http://127.0.0.1:19080")
            .unwrap()
            .login_into_namespace("groot", "password", 123)
            .await;
        assert!(client.is_err());
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
