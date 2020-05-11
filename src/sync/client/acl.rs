use crate::client::acl::LazyAclClient;
use crate::client::lazy::LazyChannel;
#[cfg(feature = "tls")]
use crate::client::tls::LazyTlsChannel;
use crate::client::{AclClient as AsyncAclClient, IClient as IAsyncClient, LazyDefaultChannel};
use crate::sync::client::{ClientVariant, IClient};
use crate::sync::txn::Txn as SyncTxn;
use crate::txn::Txn;
use crate::Result;
use async_trait::async_trait;
use failure::Error;

///
/// Inner state for logged Client
///
#[derive(Debug)]
#[doc(hidden)]
pub struct Acl<C: LazyChannel> {
    async_client: AsyncAclClient<C>,
}

#[async_trait]
impl<C: LazyChannel> IClient for Acl<C> {
    type AsyncClient = AsyncAclClient<C>;
    type Client = LazyAclClient<Self::Channel>;
    type Channel = C;

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

    fn new_txn(&self) -> Txn<Self::Client> {
        self.async_client_ref().new_txn()
    }

    async fn login<T: Into<String> + Send + Sync>(
        self,
        _user_id: T,
        _password: T,
    ) -> Result<AsyncAclClient<Self::Channel>, Error> {
        Ok(self.async_client)
    }
}

///
/// Logged client.
///
pub type AclClient<C> = ClientVariant<Acl<C>>;

///
/// Logged default client
///
pub type AclDefaultClient = AclClient<LazyAclClient<LazyDefaultChannel>>;

///
/// Logged tls client
///
#[cfg(feature = "tls")]
pub type AclTlsClient = AclClient<LazyAclClient<LazyTlsChannel>>;

///
/// Txn over http with AC:
///
pub type DefaultAclTxn = SyncTxn<LazyAclClient<LazyDefaultChannel>>;

///
/// Txn over http with AC:
///
#[cfg(feature = "tls")]
pub type TlsAclTxn = SyncTxn<LazyAclClient<LazyTlsChannel>>;

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
    /// use dgraph_tonic::sync::Client;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
    ///     let logged = client.login("groot", "password").expect("Logged in");
    ///     // now you can use logged client for all operations over DB
    ///     Ok(())
    /// }
    /// ```
    ///
    pub fn login<T: Into<String> + Send + Sync>(
        self,
        user_id: T,
        password: T,
    ) -> Result<AclClient<S::Channel>, Error> {
        let async_client = {
            let mut rt = self.state.rt.lock().expect("Tokio runtime");
            let client = self.extra;
            rt.block_on(async move { client.login(user_id, password).await })?
        };
        Ok(AclClient {
            state: self.state,
            extra: Acl { async_client },
        })
    }
}

impl<C: LazyChannel> AclClient<C> {
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
    /// use dgraph_tonic::sync::Client;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
    ///     let logged = client.login("groot", "password").expect("Logged in");
    ///     // now you can use logged client for all operations over DB
    ///     logged.refresh_login().expect("Refreshed login");
    ///     Ok(())
    /// }
    /// ```
    ///
    pub fn refresh_login(&self) -> Result<(), Error> {
        let mut rt = self.state.rt.lock().expect("Tokio runtime");
        rt.block_on(async { self.extra.async_client.refresh_login().await })
    }
}

#[cfg(test)]
mod tests {
    use crate::sync::Client;

    #[test]
    fn login() {
        let client = Client::new("http://127.0.0.1:19080")
            .unwrap()
            .login("groot", "password");
        if let Err(err) = &client {
            dbg!(err);
        }
        assert!(client.is_ok());
    }

    #[test]
    fn refresh_login() {
        let client = Client::new("http://127.0.0.1:19080")
            .unwrap()
            .login("groot", "password")
            .expect("logged");
        let refresh = client.refresh_login();
        assert!(refresh.is_ok());
    }
}
