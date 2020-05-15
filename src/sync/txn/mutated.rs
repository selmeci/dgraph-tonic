use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::client::ILazyClient;
use crate::errors::DgraphError;
use crate::sync::txn::{IState, Query, TxnType, TxnVariant};
use crate::txn::mutated::Mutate as AsyncMutate;
#[cfg(feature = "dgraph-1-1")]
use crate::txn::mutated::UpsertMutation;
use crate::txn::TxnMutatedType as AsyncMutatedTxn;
#[cfg(feature = "dgraph-1-0")]
use crate::Assigned;
use crate::Query as AsyncQuery;
use crate::Response;
use crate::{Mutation, Result};
use tokio::runtime::Runtime;

///
/// In Dgraph v1.0.x is mutation response represented as Assigned object
///
#[cfg(feature = "dgraph-1-0")]
pub type MutationResponse = Assigned;
///
/// In Dgraph v1.1.x is mutation response represented as Response object
///
#[cfg(feature = "dgraph-1-1")]
pub type MutationResponse = Response;

///
/// Inner state for transaction which can modify data in DB.
///
#[derive(Clone, Debug)]
pub struct Mutated<C: ILazyClient> {
    pub(crate) rt: Arc<Mutex<Runtime>>,
    pub(crate) async_txn: Arc<Mutex<AsyncMutatedTxn<C>>>,
}

#[async_trait]
impl<C: ILazyClient> IState for Mutated<C> {
    fn query_with_vars<Q, K, V>(
        &mut self,
        query: Q,
        vars: HashMap<K, V, RandomState>,
    ) -> Result<Response, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
    {
        let mut rt = self.rt.lock().expect("Tokio Runtime");
        let async_txn = Arc::clone(&self.async_txn);
        rt.block_on(async move {
            let mut async_txn = async_txn.lock().expect("Async Txn");
            async_txn.query_with_vars(query, vars).await
        })
    }
}

///
/// Transaction variant with mutations support.
///
pub type TxnMutatedType<C> = TxnVariant<Mutated<C>>;

impl<C: ILazyClient> TxnType<C> {
    ///
    /// Create new transaction for mutation operations.
    ///
    pub fn mutated(self) -> TxnMutatedType<C> {
        let rt = self.extra.rt;
        let txn = self
            .extra
            .async_txn
            .lock()
            .expect("Txn")
            .to_owned()
            .mutated();
        TxnVariant {
            state: self.state,
            extra: Mutated {
                rt,
                async_txn: Arc::new(Mutex::new(txn)),
            },
        }
    }
}

///
/// Allowed mutation operation in Dgraph
///
pub trait Mutate: Query {
    ///
    /// Discard transaction
    ///
    /// # Errors
    ///
    /// Return gRPC error.
    ///
    fn discard(self) -> Result<(), DgraphError>;

    ///
    /// Commit transaction
    ///
    /// # Errors
    ///
    /// Return gRPC error.
    ///
    fn commit(self) -> Result<(), DgraphError>;

    ///
    /// Adding or removing data in Dgraph is called a mutation.
    ///
    /// # Arguments
    ///
    /// * `mu`: required mutations
    ///
    /// # Errors
    ///
    /// * `GrpcError`: there is error in communication or server does not accept mutation
    /// * `MissingTxnContext`: there is error in txn setup
    ///
    /// # Example
    ///
    /// ```
    /// use dgraph_tonic::Mutation;
    /// use dgraph_tonic::sync::{Mutate, Client};
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::sync::AclClientType;
    /// use serde::Serialize;
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
    ///#[derive(Serialize)]
    /// struct Person {
    ///   uid: String,
    ///   name: String,
    /// }
    ///
    /// fn main() {
    ///    let p = Person {
    ///        uid:  "_:alice".into(),
    ///        name: "Alice".into(),
    ///    };
    ///
    ///    let mut mu = Mutation::new();
    ///    mu.set_set_json(&p).expect("JSON");
    ///
    ///    let client = client();
    ///    let mut txn = client.new_mutated_txn();
    ///    let result = txn.mutate(mu).expect("failed to create data");
    ///    txn.commit().expect("Txn is not commited");
    /// }
    /// ```
    ///
    fn mutate(&mut self, mu: Mutation) -> Result<MutationResponse, DgraphError>;

    ///
    /// Adding or removing data in Dgraph is called a mutation.
    ///
    /// Sometimes, you only want to commit a mutation, without querying anything further.
    /// In such cases, you can use this function to indicate that the mutation must be immediately
    /// committed.
    ///
    /// # Arguments
    ///
    /// * `mu`: required mutations
    ///
    /// # Errors
    ///
    /// * `GrpcError`: there is error in communication or server does not accept mutation
    /// * `MissingTxnContext`: there is error in txn setup
    ///
    /// # Example
    ///
    /// ```
    /// use dgraph_tonic::Mutation;
    /// use dgraph_tonic::sync::{Client, Mutate};
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::sync::AclClientType;
    /// use serde::Serialize;
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
    ///#[derive(Serialize)]
    /// struct Person {
    ///   uid: String,
    ///   name: String,
    /// }
    ///
    /// fn main() {
    ///    let p = Person {
    ///        uid:  "_:alice".into(),
    ///        name: "Alice".into(),
    ///    };
    ///
    ///    let mut mu = Mutation::new();
    ///    mu.set_set_json(&p).expect("JSON");
    ///
    ///    let client = client();
    ///    let txn = client.new_mutated_txn();
    ///    let result = txn.mutate_and_commit_now(mu).expect("failed to create data");
    /// }
    /// ```
    ///
    fn mutate_and_commit_now(self, mu: Mutation) -> Result<MutationResponse, DgraphError>;

    ///
    /// This function allows you to run upserts consisting of one query and one or more mutations.
    /// Transaction is commited.
    ///
    ///
    /// # Arguments
    ///
    /// * `q`: Dgraph query
    /// * `mu`: required mutations
    ///
    /// # Errors
    ///
    /// * `GrpcError`: there is error in communication or server does not accept mutation
    /// * `MissingTxnContext`: there is error in txn setup
    ///
    /// # Example
    ///
    /// Upsert with one mutation
    /// ```
    /// use dgraph_tonic::{Mutation, Operation};
    /// use dgraph_tonic::sync::{Client, Mutate};
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::sync::AclClientType;
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
    ///     let q = r#"
    ///         query {
    ///             user as var(func: eq(email, "wrong_email@dgraph.io"))
    ///         }"#;
    ///
    ///     let mut mu = Mutation::new();
    ///     mu.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);
    ///
    ///     let client = client();
    ///     let op = Operation {
    ///         schema: "email: string @index(exact) .".into(),
    ///         ..Default::default()
    ///     };
    ///     client.alter(op).expect("Schema is not updated");
    ///     let txn = client.new_mutated_txn();
    ///     // Upsert: If wrong_email found, update the existing data or else perform a new mutation.
    ///     let response = txn.upsert(q, mu).expect("failed to upsert data");
    /// }
    /// ```
    ///
    /// Upsert with more mutations
    /// ```
    /// use dgraph_tonic::{Mutation, Operation};
    /// use dgraph_tonic::sync::{Client, Mutate};
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::sync::AclClientType;
    /// use std::collections::HashMap;
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
    ///     let q = r#"
    ///         query {
    ///             user as var(func: eq(email, "wrong_email@dgraph.io"))
    ///         }"#;
    ///
    ///     let mut mu_1 = Mutation::new();
    ///     mu_1.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);
    ///     mu_1.set_cond("@if(eq(len(user), 1))");
    ///
    ///     let mut mu_2 = Mutation::new();
    ///     mu_2.set_set_nquads(r#"uid(user) <email> "another_email@dgraph.io" ."#);
    ///     mu_2.set_cond("@if(eq(len(user), 2))");
    ///
    ///     let client = client();
    ///     let op = Operation {
    ///         schema: "email: string @index(exact) .".into(),
    ///         ..Default::default()
    ///     };
    ///     client.alter(op).expect("Schema is not updated");
    ///     let txn = client.new_mutated_txn();
    ///     // Upsert: If wrong_email found, update the existing data or else perform a new mutation.
    ///     let response = txn.upsert(q, vec![mu_1, mu_2]).expect("failed to upsert data");
    /// }
    /// ```
    ///
    #[cfg(feature = "dgraph-1-1")]
    fn upsert<Q, M>(&mut self, query: Q, mu: M) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        M: Into<UpsertMutation> + Send + Sync;

    #[cfg(feature = "dgraph-1-1")]
    fn upsert_and_commit_now<Q, M>(self, query: Q, mu: M) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        M: Into<UpsertMutation> + Send + Sync;

    ///
    /// This function allows you to run upserts with query variables consisting of one query and one
    /// ore more mutations.
    ///
    ///
    /// # Arguments
    ///
    /// * `q`: Dgraph query
    /// * `mu`: required mutations
    /// * `vars`: query variables
    ///
    /// # Errors
    ///
    /// * `GrpcError`: there is error in communication or server does not accept mutation
    /// * `MissingTxnContext`: there is error in txn setup
    ///
    /// # Example
    ///
    /// Upsert with only one mutation
    /// ```
    /// use dgraph_tonic::{Mutation, Operation};
    /// use dgraph_tonic::sync::{Client, Mutate};
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::sync::AclClientType;
    /// use std::collections::HashMap;
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
    ///     let q = r#"
    ///         query users($email: string) {
    ///             user as var(func: eq(email, $email))
    ///         }"#;
    ///     let mut vars = HashMap::new();
    ///     vars.insert("$email", "wrong_email@dgraph.io");
    ///
    ///     let mut mu = Mutation::new();
    ///     mu.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);
    ///
    ///     let client = client();
    ///     let op = Operation {
    ///         schema: "email: string @index(exact) .".into(),
    ///         ..Default::default()
    ///     };
    ///     client.alter(op).expect("Schema is not updated");
    ///     let txn = client.new_mutated_txn();
    ///     // Upsert: If wrong_email found, update the existing data or else perform a new mutation.
    ///     let response = txn.upsert_with_vars(q, vars, mu).expect("failed to upsert data");
    /// }
    /// ```
    ///
    /// Upsert with more mutations
    /// ```
    /// use dgraph_tonic::{Mutation, Operation};
    /// use dgraph_tonic::sync::{Client, Mutate};
    /// #[cfg(feature = "acl")]
    /// use dgraph_tonic::sync::AclClientType;
    /// use std::collections::HashMap;
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
    ///     let q = r#"
    ///         query users($email: string) {
    ///             user as var(func: eq(email, $email))
    ///         }"#;
    ///     let mut vars = HashMap::new();
    ///     vars.insert("$email","wrong_email@dgraph.io");
    ///
    ///     let mut mu_1 = Mutation::new();
    ///     mu_1.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);
    ///     mu_1.set_cond("@if(eq(len(user), 1))");
    ///
    ///     let mut mu_2 = Mutation::new();
    ///     mu_2.set_set_nquads(r#"uid(user) <email> "another_email@dgraph.io" ."#);
    ///     mu_2.set_cond("@if(eq(len(user), 2))");
    ///
    ///     let client = client();
    ///     let op = Operation {
    ///         schema: "email: string @index(exact) .".into(),
    ///         ..Default::default()
    ///     };
    ///     client.alter(op).expect("Schema is not updated");
    ///     let txn = client.new_mutated_txn();
    ///     // Upsert: If wrong_email found, update the existing data or else perform a new mutation.
    ///     let response = txn.upsert_with_vars(q, vars, vec![mu_1, mu_2]).expect("failed to upsert data");
    /// }
    /// ```
    ///
    #[cfg(feature = "dgraph-1-1")]
    fn upsert_with_vars<Q, K, V, M>(
        &mut self,
        query: Q,
        vars: HashMap<K, V>,
        mu: M,
    ) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
        M: Into<UpsertMutation> + Send + Sync;

    #[cfg(feature = "dgraph-1-1")]
    fn upsert_with_vars_and_commit_now<Q, K, V, M>(
        self,
        query: Q,
        vars: HashMap<K, V>,
        mu: M,
    ) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
        M: Into<UpsertMutation> + Send + Sync;
}

impl<C: ILazyClient> Mutate for TxnMutatedType<C> {
    fn discard(self) -> Result<(), DgraphError> {
        let mut rt = self.extra.rt.lock().expect("Tokio Runtime");
        let async_txn = Arc::clone(&self.extra.async_txn);
        rt.block_on(async move {
            let async_txn = async_txn.lock().expect("MutatedTxn").to_owned();
            async_txn.discard().await
        })
    }

    fn commit(self) -> Result<(), DgraphError> {
        let mut rt = self.extra.rt.lock().expect("Tokio Runtime");
        let async_txn = Arc::clone(&self.extra.async_txn);
        rt.block_on(async move {
            let async_txn = async_txn.lock().expect("MutatedTxn").to_owned();
            async_txn.commit().await
        })
    }

    fn mutate(&mut self, mu: Mutation) -> Result<MutationResponse, DgraphError> {
        let mut rt = self.extra.rt.lock().expect("Tokio Runtime");
        let async_txn = Arc::clone(&self.extra.async_txn);
        rt.block_on(async move {
            let mut async_txn = async_txn.lock().expect("MutatedTxn");
            async_txn.mutate(mu).await
        })
    }

    fn mutate_and_commit_now(self, mu: Mutation) -> Result<MutationResponse, DgraphError> {
        let mut rt = self.extra.rt.lock().expect("Tokio Runtime");
        let async_txn = Arc::clone(&self.extra.async_txn);
        rt.block_on(async move {
            let async_txn = async_txn.lock().expect("MutatedTxn").to_owned();
            async_txn.mutate_and_commit_now(mu).await
        })
    }

    #[cfg(feature = "dgraph-1-1")]
    fn upsert<Q, M>(&mut self, query: Q, mu: M) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        M: Into<UpsertMutation> + Send + Sync,
    {
        let mut rt = self.extra.rt.lock().expect("Tokio Runtime");
        let async_txn = Arc::clone(&self.extra.async_txn);
        rt.block_on(async move {
            let mut async_txn = async_txn.lock().expect("MutatedTxn").to_owned();
            async_txn.upsert(query, mu).await
        })
    }

    #[cfg(feature = "dgraph-1-1")]
    fn upsert_and_commit_now<Q, M>(self, query: Q, mu: M) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        M: Into<UpsertMutation> + Send + Sync,
    {
        let mut rt = self.extra.rt.lock().expect("Tokio Runtime");
        let async_txn = Arc::clone(&self.extra.async_txn);
        rt.block_on(async move {
            let mut async_txn = async_txn.lock().expect("MutatedTxn").to_owned();
            async_txn.upsert_and_commit_now(query, mu).await
        })
    }

    #[cfg(feature = "dgraph-1-1")]
    fn upsert_with_vars<Q, K, V, M>(
        &mut self,
        query: Q,
        vars: HashMap<K, V>,
        mu: M,
    ) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
        M: Into<UpsertMutation> + Send + Sync,
    {
        let mut rt = self.extra.rt.lock().expect("Tokio Runtime");
        let async_txn = Arc::clone(&self.extra.async_txn);
        rt.block_on(async move {
            let mut async_txn = async_txn.lock().expect("MutatedTxn").to_owned();
            async_txn.upsert_with_vars(query, vars, mu).await
        })
    }

    #[cfg(feature = "dgraph-1-1")]
    fn upsert_with_vars_and_commit_now<Q, K, V, M>(
        self,
        query: Q,
        vars: HashMap<K, V>,
        mu: M,
    ) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
        M: Into<UpsertMutation> + Send + Sync,
    {
        let mut rt = self.extra.rt.lock().expect("Tokio Runtime");
        let async_txn = Arc::clone(&self.extra.async_txn);
        rt.block_on(async move {
            let mut async_txn = async_txn.lock().expect("MutatedTxn").to_owned();
            async_txn.upsert_with_vars_and_commit_now(query, vars, mu).await
        })
    }
}
