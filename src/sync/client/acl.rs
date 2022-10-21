use anyhow::Result;
use async_trait::async_trait;

use crate::client::acl::LazyAclClient;
use crate::client::lazy::ILazyChannel;
#[cfg(feature = "tls")]
use crate::client::tls::LazyTlsChannel;
use crate::client::{AclClientType as AsyncAclClient, IClient as IAsyncClient, LazyChannel};
use crate::sync::client::{ClientVariant, IClient};
use crate::sync::txn::TxnType as SyncTxn;
use crate::sync::{TxnBestEffortType, TxnMutatedType, TxnReadOnlyType};
use crate::txn::TxnType;

///
/// Inner state for logged Client
///
#[derive(Debug)]
#[doc(hidden)]
pub struct Acl<C: ILazyChannel> {
    async_client: AsyncAclClient<C>,
}

#[async_trait]
impl<C: ILazyChannel> IClient for Acl<C> {
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

    fn new_txn(&self) -> TxnType<Self::Client> {
        self.async_client_ref().new_txn()
    }

    async fn login<T: Into<String> + Send + Sync>(
        self,
        _user_id: T,
        _password: T,
    ) -> Result<AsyncAclClient<Self::Channel>> {
        Ok(self.async_client)
    }

    #[cfg(feature = "dgraph-21-03")]
    async fn login_into_namespace<T: Into<String> + Send + Sync>(
        self,
        _user_id: T,
        _password: T,
        _namespace: u64,
    ) -> Result<AsyncAclClient<Self::Channel>> {
        Ok(self.async_client)
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
/// Txn over http with AC:
///
pub type TxnAcl = SyncTxn<LazyAclClient<LazyChannel>>;

///
/// Readonly txn over http with Acl
///
pub type TxnAlcReadOnly = TxnReadOnlyType<LazyAclClient<LazyChannel>>;

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
pub type TxnAclTls = SyncTxn<LazyAclClient<LazyTlsChannel>>;

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
    ) -> Result<AclClientType<S::Channel>> {
        let async_client = {
            let client = self.extra;
            self.state
                .rt
                .block_on(async move { client.login(user_id, password).await })?
        };
        Ok(AclClientType {
            state: self.state,
            extra: Acl { async_client },
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
    /// In the example above, the client logs into namespace 0 using username `groot` and password `password`. Once logged in, the client can perform all the operations allowed to the groot user of namespace 0.
    ///
    /// ```
    /// use dgraph_tonic::sync::Client;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = Client::new("http://127.0.0.1:19080").expect("Dgraph client");
    ///     let logged = client.login_into_namespace("groot", "password", 0).expect("Logged in");
    ///     // now you can use logged client for all operations over DB
    ///     Ok(())
    /// }
    /// ```
    ///
    #[cfg(feature = "dgraph-21-03")]
    pub fn login_into_namespace<T: Into<String> + Send + Sync>(
        self,
        user_id: T,
        password: T,
        namespace: u64,
    ) -> Result<AclClientType<S::Channel>> {
        let async_client = {
            let client = self.extra;
            self.state.rt.block_on(async move {
                client
                    .login_into_namespace(user_id, password, namespace)
                    .await
            })?
        };
        Ok(AclClientType {
            state: self.state,
            extra: Acl { async_client },
        })
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
    pub fn refresh_login(&self) -> Result<()> {
        self.state
            .rt
            .block_on(async { self.extra.async_client.refresh_login().await })
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
    #[cfg(feature = "dgraph-21-03")]
    fn login_into_namespace() {
        let client = Client::new("http://127.0.0.1:19080")
            .unwrap()
            .login_into_namespace("groot", "password", 0);
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
