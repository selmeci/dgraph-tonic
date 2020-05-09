///! Transactions is modeled with principles of [The Typestate Pattern in Rust](http://cliffle.com/blog/rust-typestate/)
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::{Send, Sync};
use std::ops::{Deref, DerefMut};

use crate::client::ILazyClient;
use crate::errors::DgraphError;
use crate::stub::Stub;
pub use crate::txn::best_effort::BestEffortTxn;
pub use crate::txn::default::Txn;
pub use crate::txn::mutated::{Mutate, MutatedTxn};
pub use crate::txn::read_only::ReadOnlyTxn;
use crate::IDgraphClient;
use crate::{Request, Response, TxnContext};

use async_trait::async_trait;

pub(crate) mod best_effort;
pub(crate) mod default;
pub(crate) mod mutated;
pub(crate) mod read_only;

///
/// Transaction state.
/// Hold txn context and Dgraph client for communication.
///
#[derive(Clone, Debug)]
pub struct TxnState<C: ILazyClient> {
    stub: Stub<C>,
    context: TxnContext,
}

///
/// Each transaction variant must implement this state trait.
///
pub trait IState: Send + Sync + Clone {
    fn query_request<C: ILazyClient>(
        &self,
        state: &TxnState<C>,
        query: String,
        vars: HashMap<String, String>,
    ) -> Request;
}

///
/// Type state for Transaction variants
///
#[derive(Clone, Debug)]
pub struct TxnVariant<S: IState, C: ILazyClient> {
    state: Box<TxnState<C>>,
    extra: S,
}

impl<S: IState, C: ILazyClient> Deref for TxnVariant<S, C> {
    type Target = Box<TxnState<C>>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<S: IState, C: ILazyClient> DerefMut for TxnVariant<S, C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

///
/// All Dgaph transaction types can performe a queries
///
#[async_trait]
pub trait Query: Send + Sync {
    ///
    /// You can run a query by calling `txn.query(q)`.
    ///
    /// # Arguments
    ///
    /// * `query`: GraphQL+- query
    ///
    /// # Errors
    ///
    /// If transaction is not initialized properly, return `EmptyTxn` error.
    ///
    /// gRPC errors can be returned also.
    ///
    /// # Example
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use dgraph_tonic::{Client, Response, Query};
    /// use serde::Deserialize;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::{AclClient, LazyDefaultChannel};
    ///
    /// #[cfg(not(feature = "acl"))]
    /// async fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// async fn client() -> AclClient<LazyDefaultChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").await.expect("Acl client")
    /// }    
    ///
    /// #[derive(Deserialize, Debug)]
    /// struct Person {
    ///   uid: String,
    ///   name: String,
    /// }
    ///
    /// #[derive(Deserialize, Debug)]
    /// struct Persons {
    ///   all: Vec<Person>
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let q = r#"query all($a: string) {
    ///     all(func: eq(name, "Alice")) {
    ///       uid
    ///       name
    ///     }
    ///   }"#;
    ///
    ///   let client = client().await;
    ///   let mut txn = client.new_read_only_txn();
    ///   let resp: Response = txn.query(q).await.expect("Query response");
    ///   let persons: Persons = resp.try_into().expect("Persons");
    /// }
    /// ```
    ///
    async fn query<Q>(&mut self, query: Q) -> Result<Response, DgraphError>
    where
        Q: Into<String> + Send + Sync;

    ///
    /// You can run a query with defined variables by calling `txn.query_with_vars(q, vars)`.
    ///
    /// # Arguments
    ///
    /// * `query`: GraphQL+- query
    /// * `vars`: map of variables
    ///
    /// # Errors
    ///
    /// If transaction is not initialized properly, return `EmptyTxn` error.
    ///
    /// gRPC errors can be returned also.
    ///
    /// # Example
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use dgraph_tonic::{Client, Response, Query};
    /// use serde::Deserialize;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::{AclClient, LazyDefaultChannel};
    ///
    /// #[cfg(not(feature = "acl"))]
    /// async fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// async fn client() -> AclClient<LazyDefaultChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").await.expect("Acl client")
    /// }     
    ///
    /// #[derive(Deserialize, Debug)]
    /// struct Person {
    ///     uid: String,
    ///     name: String,
    /// }
    ///
    /// #[derive(Deserialize, Debug)]
    /// struct Persons {
    ///     all: Vec<Person>
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let q = r#"query all($a: string) {
    ///         all(func: eq(name, $a)) {
    ///         uid
    ///         name
    ///         }
    ///     }"#;
    ///
    ///     let mut vars = HashMap::new();
    ///     vars.insert("$a", "Alice");
    ///
    ///     let client = client().await;
    ///     let mut txn = client.new_read_only_txn();
    ///     let resp: Response = txn.query_with_vars(q, vars).await.expect("query response");
    ///     let persons: Persons = resp.try_into().expect("Persons");
    /// }
    /// ```    
    async fn query_with_vars<Q, K, V>(
        &mut self,
        query: Q,
        vars: HashMap<K, V>,
    ) -> Result<Response, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync;
}

#[async_trait]
impl<S: IState, C: ILazyClient> Query for TxnVariant<S, C> {
    async fn query<Q>(&mut self, query: Q) -> Result<Response, DgraphError>
    where
        Q: Into<String> + Send + Sync,
    {
        self.query_with_vars(query, HashMap::<String, String, _>::new())
            .await
    }

    async fn query_with_vars<Q, K, V>(
        &mut self,
        query: Q,
        vars: HashMap<K, V>,
    ) -> Result<Response, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
    {
        let vars = vars.into_iter().fold(HashMap::new(), |mut tmp, (k, v)| {
            tmp.insert(k.into(), v.into());
            tmp
        });
        let request = self.extra.query_request(&self.state, query.into(), vars);
        let response = match self.stub.query(request).await {
            Ok(response) => response,
            Err(err) => {
                return Err(DgraphError::GrpcError(err));
            }
        };
        match response.txn.as_ref() {
            Some(src) => self.context.merge_context(src)?,
            None => return Err(DgraphError::EmptyTxn),
        };
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    use serde_derive::{Deserialize, Serialize};

    use crate::client::Client;
    #[cfg(feature = "acl")]
    use crate::client::{AclClient, LazyDefaultChannel};
    use crate::{Mutate, Mutation};

    #[cfg(not(feature = "acl"))]
    async fn client() -> Client {
        Client::new("http://127.0.0.1:19080").unwrap()
    }

    #[cfg(feature = "acl")]
    async fn client() -> AclClient<LazyDefaultChannel> {
        let default = Client::new("http://127.0.0.1:19080").unwrap();
        default.login("groot", "password").await.unwrap()
    }

    #[derive(Serialize, Deserialize, Default, Debug)]
    struct Person {
        uid: String,
        name: String,
    }

    #[derive(Serialize, Deserialize, Default, Debug)]
    pub struct UidJson {
        pub uids: Vec<Uid>,
    }

    #[derive(Serialize, Deserialize, Default, Debug)]
    pub struct Uid {
        pub uid: String,
    }

    #[tokio::test]
    async fn mutate_and_commit_now() {
        let client = client().await;
        let txn = client.new_mutated_txn();
        let p = Person {
            uid: "_:alice".to_string(),
            name: "Alice".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate_and_commit_now(mu).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn commit() {
        let client = client().await;
        let mut txn = client.new_mutated_txn();
        //first mutation
        let p = Person {
            uid: "_:alice".to_string(),
            name: "Alice".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu).await;
        assert!(response.is_ok());
        //second mutation
        let p = Person {
            uid: "_:mike".to_string(),
            name: "Mike".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu).await;
        assert!(response.is_ok());
        //commit
        let commit = txn.commit().await;
        assert!(commit.is_ok())
    }

    #[cfg(feature = "dgraph-1-1")]
    #[tokio::test]
    async fn upsert() {
        let client = client().await;
        let mut txn = client.new_mutated_txn();
        //first mutation
        let p = Person {
            uid: "_:alice".to_string(),
            name: "Alice".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu).await;
        assert!(response.is_ok());
        //second mutation
        let p = Person {
            uid: "_:mike".to_string(),
            name: "Mike".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu).await;
        assert!(response.is_ok());
        //commit
        let commit = txn.commit().await;
        assert!(commit.is_ok());
        //upser all alices with email
        let query = r#"
          query {
              user as var(func: eq(name, "Alice"))
          }"#;
        let mut mu = Mutation::new();
        mu.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);
        let txn = client.new_mutated_txn();
        let response = txn.upsert(query, mu).await;
        assert!(response.is_ok())
    }

    #[cfg(feature = "dgraph-1-1")]
    #[tokio::test]
    async fn upsert_with_vars() {
        let client = client().await;
        let mut txn = client.new_mutated_txn();
        //first mutation
        let p = Person {
            uid: "_:alice".to_string(),
            name: "Alice".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu).await;
        assert!(response.is_ok());
        //second mutation
        let p = Person {
            uid: "_:mike".to_string(),
            name: "Mike".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu).await;
        assert!(response.is_ok());
        //commit
        let commit = txn.commit().await;
        assert!(commit.is_ok());
        //upser all alices with email
        let query = r#"
          query alices($a: string) {
              user as var(func: eq(name, $a))
          }"#;
        let mut mu = Mutation::new();
        mu.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);
        let mut vars = HashMap::new();
        vars.insert("$a", "Alice");
        let txn = client.new_mutated_txn();
        let response = txn.upsert_with_vars(query, vars, vec![mu]).await;
        assert!(response.is_ok())
    }

    #[tokio::test]
    async fn query() {
        let client = client().await;
        let mut txn = client.new_read_only_txn();
        let query = r#"{
            uids(func: eq(name, "Alice")) {
                uid
            }
        }"#;
        let response = txn.query(query).await;
        assert!(response.is_ok());
        let mut json: UidJson = response.unwrap().try_into().unwrap();
        assert!(json.uids.pop().is_some());
    }

    #[tokio::test]
    async fn query_with_vars() {
        let client = client().await;
        let mut txn = client.new_read_only_txn();
        let query = r#"query all($a: string) {
            uids(func: eq(name, $a)) {
              uid
            }
          }"#;
        let mut vars = HashMap::new();
        vars.insert("$a", "Alice");
        let response = txn.query_with_vars(query, vars).await;
        assert!(response.is_ok());
        let mut json: UidJson = response.unwrap().try_into().unwrap();
        assert!(json.uids.pop().is_some());
    }
}
