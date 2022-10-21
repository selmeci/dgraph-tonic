use std::collections::HashMap;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};

use anyhow::Result;
use async_trait::async_trait;

pub use crate::sync::txn::best_effort::TxnBestEffortType;
pub use crate::sync::txn::default::TxnType;
pub use crate::sync::txn::mutated::{Mutate, MutationResponse, TxnMutatedType};
pub use crate::sync::txn::read_only::TxnReadOnlyType;
use crate::Response;

pub(crate) mod best_effort;
pub(crate) mod default;
pub(crate) mod mutated;
pub(crate) mod read_only;

///
/// Transaction state.
/// Hold Tokio runtime
///
#[derive(Clone, Debug)]
pub struct TxnState {}

///
/// Each transaction variant must implement this state trait.
///
#[async_trait]
pub trait IState: Send + Sync + Clone {
    fn query_with_vars<Q, K, V>(&mut self, query: Q, vars: HashMap<K, V>) -> Result<Response>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync;

    #[cfg(any(feature = "dgraph-1-1", feature = "dgraph-21-03"))]
    fn query_rdf_with_vars<Q, K, V>(&mut self, query: Q, vars: HashMap<K, V>) -> Result<Response>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync;
}

///
/// Type state for Transaction variants
///
#[derive(Clone)]
pub struct TxnVariant<S: IState> {
    state: Box<TxnState>,
    extra: S,
}

impl<S: IState> Deref for TxnVariant<S> {
    type Target = Box<TxnState>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<S: IState> DerefMut for TxnVariant<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

///
/// All Dgaph transaction types can performe a queries
///
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
    /// use dgraph_tonic::Response;
    /// use dgraph_tonic::sync::{Query, Client};
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::sync::AclClientType;
    /// use serde::Deserialize;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::LazyChannel;
    ///
    /// #[cfg(not(feature = "acl"))]
    /// fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// fn client() -> AclClientType<LazyChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").expect("Acl client")
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
    /// fn main() {
    ///     let q = r#"query all($a: string) {
    ///     all(func: eq(name, "Alice")) {
    ///       uid
    ///       name
    ///     }
    ///   }"#;
    ///
    ///   let client = client();
    ///   let mut txn = client.new_read_only_txn();
    ///   let resp: Response = txn.query(q).expect("Query response");
    ///   let persons: Persons = resp.try_into().expect("Persons");
    /// }
    /// ```
    ///
    fn query<Q>(&mut self, query: Q) -> Result<Response>
    where
        Q: Into<String> + Send + Sync;

    ///
    /// You can run a query with rdf response by calling `txn.query_rdf(q)`.
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
    /// use dgraph_tonic::Response;
    /// use dgraph_tonic::sync::{Query, Client};
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::sync::AclClientType;
    /// use serde::Deserialize;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::LazyChannel;
    ///
    /// #[cfg(not(feature = "acl"))]
    /// fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// fn client() -> AclClientType<LazyChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").expect("Acl client")
    /// }
    ///
    /// fn main() {
    ///     let q = r#"query all($a: string) {
    ///     all(func: eq(name, "Alice")) {
    ///       uid
    ///       name
    ///     }
    ///   }"#;
    ///
    ///   let client = client();
    ///   let mut txn = client.new_read_only_txn();
    ///   let resp: Response = txn.query_rdf(q).expect("Query response");
    ///   println!("{}",String::from_utf8(resp.rdf).unwrap());
    /// }
    /// ```
    ///
    #[cfg(any(feature = "dgraph-1-1", feature = "dgraph-21-03"))]
    fn query_rdf<Q>(&mut self, query: Q) -> Result<Response>
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
    /// use dgraph_tonic::Response;
    /// use dgraph_tonic::sync::{Query, Client};
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::sync::AclClientType;
    /// use serde::Deserialize;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::LazyChannel;
    ///
    /// #[cfg(not(feature = "acl"))]
    /// fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// fn client() -> AclClientType<LazyChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").expect("Acl client")
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
    /// fn main() {
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
    ///     let client = client();
    ///     let mut txn = client.new_read_only_txn();
    ///     let resp: Response = txn.query_with_vars(q, vars).expect("query response");
    ///     let persons: Persons = resp.try_into().expect("Persons");
    /// }
    /// ```
    fn query_with_vars<Q, K, V>(&mut self, query: Q, vars: HashMap<K, V>) -> Result<Response>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync;

    ///
    /// You can run a query with defined variables and rdf response by calling `txn.query_rdf_with_vars(q, vars)`.
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
    /// use dgraph_tonic::Response;
    /// use dgraph_tonic::sync::{Query, Client};
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::sync::AclClientType;
    /// use serde::Deserialize;
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::LazyChannel;
    ///
    /// #[cfg(not(feature = "acl"))]
    /// fn client() -> Client {
    ///     Client::new("http://127.0.0.1:19080").expect("Dgraph client")
    /// }
    ///
    /// #[cfg(feature = "acl")]
    /// fn client() -> AclClientType<LazyChannel> {
    ///     let default = Client::new("http://127.0.0.1:19080").unwrap();
    ///     default.login("groot", "password").expect("Acl client")
    /// }
    ///
    /// fn main() {
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
    ///     let client = client();
    ///     let mut txn = client.new_read_only_txn();
    ///     let resp: Response = txn.query_rdf_with_vars(q, vars).expect("query response");
    ///     println!("{}",String::from_utf8(resp.rdf).unwrap());
    /// }
    /// ```
    ///
    #[cfg(any(feature = "dgraph-1-1", feature = "dgraph-21-03"))]
    fn query_rdf_with_vars<Q, K, V>(&mut self, query: Q, vars: HashMap<K, V>) -> Result<Response>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync;
}

impl<S: IState> Query for TxnVariant<S> {
    fn query<Q>(&mut self, query: Q) -> Result<Response>
    where
        Q: Into<String> + Send + Sync,
    {
        self.query_with_vars(query, HashMap::<String, String, _>::with_capacity(0))
    }

    #[cfg(any(feature = "dgraph-1-1", feature = "dgraph-21-03"))]
    fn query_rdf<Q>(&mut self, query: Q) -> Result<Response>
    where
        Q: Into<String> + Send + Sync,
    {
        self.query_rdf_with_vars(query, HashMap::<String, String, _>::with_capacity(0))
    }

    fn query_with_vars<Q, K, V>(&mut self, query: Q, vars: HashMap<K, V>) -> Result<Response>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
    {
        self.extra.query_with_vars(query, vars)
    }

    #[cfg(any(feature = "dgraph-1-1", feature = "dgraph-21-03"))]
    fn query_rdf_with_vars<Q, K, V>(&mut self, query: Q, vars: HashMap<K, V>) -> Result<Response>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
    {
        self.extra.query_rdf_with_vars(query, vars)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use serde_derive::{Deserialize, Serialize};

    #[cfg(feature = "acl")]
    use crate::client::LazyChannel;
    #[cfg(feature = "acl")]
    use crate::sync::client::AclClientType;
    use crate::sync::client::Client;
    use crate::sync::{Mutate, Query};
    use crate::Mutation;

    #[cfg(not(feature = "acl"))]
    fn client() -> Client {
        Client::new("http://127.0.0.1:19080").unwrap()
    }

    #[cfg(feature = "acl")]
    fn client() -> AclClientType<LazyChannel> {
        let default = Client::new("http://127.0.0.1:19080").unwrap();
        default.login("groot", "password").unwrap()
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

    fn insert_data() {
        let client = client();
        let txn = client.new_mutated_txn();
        let p = Person {
            uid: "_:alice".to_string(),
            name: "Alice".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate_and_commit_now(mu);
        assert!(response.is_ok());
    }

    #[test]
    fn mutate_and_commit_now() {
        let client = client();
        let txn = client.new_mutated_txn();
        let p = Person {
            uid: "_:alice".to_string(),
            name: "Alice".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate_and_commit_now(mu);
        assert!(response.is_ok());
    }

    #[test]
    fn commit() {
        let client = client();
        let mut txn = client.new_mutated_txn();
        //first mutation
        let p = Person {
            uid: "_:alice".to_string(),
            name: "Alice".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu);
        assert!(response.is_ok());
        //second mutation
        let p = Person {
            uid: "_:mike".to_string(),
            name: "Mike".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu);
        assert!(response.is_ok());
        //commit
        let commit = txn.commit();
        assert!(commit.is_ok())
    }

    #[test]
    #[cfg(any(feature = "dgraph-1-1", feature = "dgraph-21-03"))]
    fn upsert() {
        let client = client();
        let mut txn = client.new_mutated_txn();
        //first mutation
        let p = Person {
            uid: "_:alice".to_string(),
            name: "Alice".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu);
        assert!(response.is_ok());
        //second mutation
        let p = Person {
            uid: "_:mike".to_string(),
            name: "Mike".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu);
        assert!(response.is_ok());
        //commit
        let commit = txn.commit();
        assert!(commit.is_ok());
        //upser all alices with email
        let query = r#"
          query {
              user as var(func: eq(name, "Alice"))
          }"#;
        let mut mu = Mutation::new();
        mu.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);
        let mut txn = client.new_mutated_txn();
        assert!(txn.upsert(query, mu).is_ok());
        assert!(txn.commit().is_ok());
    }

    #[test]
    #[cfg(any(feature = "dgraph-1-1", feature = "dgraph-21-03"))]
    fn upsert_and_commit_now() {
        let client = client();
        let mut txn = client.new_mutated_txn();
        //first mutation
        let p = Person {
            uid: "_:alice".to_string(),
            name: "Alice".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu);
        assert!(response.is_ok());
        //second mutation
        let p = Person {
            uid: "_:mike".to_string(),
            name: "Mike".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu);
        assert!(response.is_ok());
        //commit
        let commit = txn.commit();
        assert!(commit.is_ok());
        //upser all alices with email
        let query = r#"
          query {
              user as var(func: eq(name, "Alice"))
          }"#;
        let mut mu = Mutation::new();
        mu.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);
        let txn = client.new_mutated_txn();
        let response = txn.upsert_and_commit_now(query, mu);
        assert!(response.is_ok())
    }

    #[test]
    #[cfg(any(feature = "dgraph-1-1", feature = "dgraph-21-03"))]
    fn upsert_with_vars() {
        let client = client();
        let mut txn = client.new_mutated_txn();
        //first mutation
        let p = Person {
            uid: "_:alice".to_string(),
            name: "Alice".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu);
        assert!(response.is_ok());
        //second mutation
        let p = Person {
            uid: "_:mike".to_string(),
            name: "Mike".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu);
        assert!(response.is_ok());
        //commit
        let commit = txn.commit();
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
        let mut txn = client.new_mutated_txn();
        assert!(txn.upsert_with_vars(query, vars, vec![mu]).is_ok());
        assert!(txn.commit().is_ok());
    }

    #[test]
    #[cfg(any(feature = "dgraph-1-1", feature = "dgraph-21-03"))]
    fn upsert_with_vars_and_commit_now() {
        let client = client();
        let mut txn = client.new_mutated_txn();
        //first mutation
        let p = Person {
            uid: "_:alice".to_string(),
            name: "Alice".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu);
        assert!(response.is_ok());
        //second mutation
        let p = Person {
            uid: "_:mike".to_string(),
            name: "Mike".to_string(),
        };
        let mut mu = Mutation::new();
        mu.set_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu);
        assert!(response.is_ok());
        //commit
        let commit = txn.commit();
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
        let response = txn.upsert_with_vars_and_commit_now(query, vars, vec![mu]);
        assert!(response.is_ok())
    }

    #[test]
    fn query() {
        let client = client();
        client
            .set_schema("name: string @index(exact) .")
            .expect("Schema is not updated");
        insert_data();
        let mut txn = client.new_read_only_txn();
        let query = r#"{
            uids(func: eq(name, "Alice")) {
                uid
            }
        }"#;
        let response = txn.query(query);
        assert!(response.is_ok());
        let mut json: UidJson = response.unwrap().try_into().unwrap();
        assert!(json.uids.pop().is_some());
    }

    #[test]
    fn query_with_vars() {
        let client = client();
        client
            .set_schema("name: string @index(exact) .")
            .expect("Schema is not updated");
        insert_data();
        let mut txn = client.new_read_only_txn();
        let query = r#"query all($a: string) {
            uids(func: eq(name, $a)) {
              uid
            }
          }"#;
        let mut vars = HashMap::new();
        vars.insert("$a", "Alice");
        let response = txn.query_with_vars(query, vars);
        assert!(response.is_ok());
        let mut json: UidJson = response.unwrap().try_into().unwrap();
        assert!(json.uids.pop().is_some());
    }

    #[test]
    fn best_effort_txn_query() {
        let client = client();
        client
            .set_schema("name: string @index(exact) .")
            .expect("Schema is not updated");
        insert_data();
        std::thread::sleep(Duration::from_secs(1));
        let mut txn = client.new_best_effort_txn();
        let query = r#"{
            uids(func: eq(name, "Alice")) {
                uid
            }
        }"#;
        let response = txn.query(query);
        assert!(response.is_ok());
        let mut json: UidJson = response.unwrap().try_into().unwrap();
        assert!(json.uids.pop().is_some());
    }

    #[test]
    fn best_effort_txn_query_with_vars() {
        let client = client();
        client
            .set_schema("name: string @index(exact) .")
            .expect("Schema is not updated");
        insert_data();
        let mut txn = client.new_best_effort_txn();
        let query = r#"query all($a: string) {
            uids(func: eq(name, $a)) {
              uid
            }
          }"#;
        let mut vars = HashMap::new();
        vars.insert("$a", "Alice");
        let response = txn.query_with_vars(query, vars);
        assert!(response.is_ok());
        let mut json: UidJson = response.unwrap().try_into().unwrap();
        assert!(json.uids.pop().is_some());
    }
}
