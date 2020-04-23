use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use async_trait::async_trait;

use crate::client::ILazyClient;
use crate::errors::DgraphError;
use crate::txn::default::Base;
use crate::txn::{IState, Txn, TxnState, TxnVariant};
#[cfg(feature = "dgraph-1-0")]
use crate::Assigned;
use crate::IDgraphClient;
#[cfg(feature = "dgraph-1-1")]
use crate::Response;
use crate::{Mutation, Request};

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
    base: Base<C>,
    mutated: bool,
}

///
/// Upsert mutation can be defined with one or more mutations
///
#[cfg(feature = "dgraph-1-1")]
pub struct UpsertMutation {
    mu: Vec<Mutation>,
}

#[cfg(feature = "dgraph-1-1")]
impl From<Vec<Mutation>> for UpsertMutation {
    fn from(mu: Vec<Mutation>) -> Self {
        Self { mu }
    }
}

#[cfg(feature = "dgraph-1-1")]
impl From<Mutation> for UpsertMutation {
    fn from(mu: Mutation) -> Self {
        Self { mu: vec![mu] }
    }
}

#[async_trait]
impl<C: ILazyClient> IState for Mutated<C> {
    ///
    /// Do same query like default transaction
    ///
    fn query_request<S: ILazyClient>(
        &self,
        state: &TxnState<S>,
        query: String,
        vars: HashMap<String, String, RandomState>,
    ) -> Request {
        self.base.query_request(state, query, vars)
    }
}

///
/// Transaction variant with mutations support.
///
pub type MutatedTxn<C> = TxnVariant<Mutated<C>, C>;

impl<C: ILazyClient> Txn<C> {
    ///
    /// Create new transaction for mutation operations.
    ///
    pub fn mutated(self) -> MutatedTxn<C> {
        TxnVariant {
            state: self.state,
            extra: Mutated {
                base: self.extra,
                mutated: false,
            },
        }
    }
}

impl<C: ILazyClient> MutatedTxn<C> {
    #[cfg(feature = "dgraph-1-0")]
    async fn do_mutation<Q, K, V>(
        &mut self,
        _query: Q,
        _vars: HashMap<K, V>,
        mut mu: Mutation,
        commit_now: bool,
    ) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
    {
        self.extra.mutated = true;
        mu.commit_now = commit_now;
        mu.start_ts = self.context.start_ts;
        let assigned = match self.client.mutate(mu).await {
            Ok(assigned) => assigned,
            Err(err) => {
                return Err(DgraphError::GrpcError(err));
            }
        };
        match assigned.context.as_ref() {
            Some(src) => self.context.merge_context(src)?,
            None => return Err(DgraphError::MissingTxnContext),
        }
        Ok(assigned)
    }

    #[cfg(feature = "dgraph-1-1")]
    async fn do_mutation<Q, K, V, M>(
        &mut self,
        query: Q,
        vars: HashMap<K, V>,
        mu: M,
        commit_now: bool,
    ) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
        M: Into<UpsertMutation>,
    {
        self.extra.mutated = true;
        let vars = vars.into_iter().fold(HashMap::new(), |mut tmp, (k, v)| {
            tmp.insert(k.into(), v.into());
            tmp
        });
        let mu: UpsertMutation = mu.into();
        let request = Request {
            query: query.into(),
            vars,
            start_ts: self.context.start_ts,
            commit_now,
            mutations: mu.mu,
            ..Default::default()
        };
        let response = match self.client.do_request(request).await {
            Ok(response) => response,
            Err(err) => {
                return Err(DgraphError::GrpcError(err));
            }
        };
        match response.txn.as_ref() {
            Some(txn) => self.context.merge_context(txn)?,
            None => return Err(DgraphError::MissingTxnContext),
        }
        Ok(response)
    }

    async fn commit_or_abort(self) -> Result<(), DgraphError> {
        let extra = self.extra;
        let state = *self.state;
        if !extra.mutated {
            return Ok(());
        };
        let mut client = state.client;
        let txn = state.context;
        match client.commit_or_abort(txn).await {
            Ok(_txn_context) => Ok(()),
            Err(err) => Err(DgraphError::GrpcError(err)),
        }
    }

    ///
    /// Discard transaction
    ///
    /// # Errors
    ///
    /// Return gRPC error.
    ///
    pub async fn discard(mut self) -> Result<(), DgraphError> {
        self.context.aborted = true;
        self.commit_or_abort().await
    }

    ///
    /// Commit transaction
    ///
    /// # Errors
    ///
    /// Return gRPC error.
    ///
    pub async fn commit(self) -> Result<(), DgraphError> {
        self.commit_or_abort().await
    }

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
    /// use dgraph_tonic::{Client, Mutation};
    /// use serde::Serialize;
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
    ///#[derive(Serialize)]
    /// struct Person {
    ///   uid: String,
    ///   name: String,
    /// }
    /// #[tokio::main]
    /// async fn main() {   
    ///    let p = Person {
    ///        uid:  "_:alice".into(),
    ///        name: "Alice".into(),
    ///    };
    ///
    ///    let mut mu = Mutation::new();
    ///    mu.set_set_json(&p).expect("JSON");
    ///
    ///    let client = client().await;
    ///    let mut txn = client.new_mutated_txn();
    ///    let result = txn.mutate(mu).await.expect("failed to create data");
    ///    txn.commit().await.expect("Txn is not commited");
    /// }
    /// ```
    ///
    pub async fn mutate(&mut self, mu: Mutation) -> Result<MutationResponse, DgraphError> {
        self.do_mutation("", HashMap::<String, String>::with_capacity(0), mu, false)
            .await
    }

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
    /// use dgraph_tonic::{Client, Mutation};
    /// use serde::Serialize;
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
    ///#[derive(Serialize)]
    /// struct Person {
    ///   uid: String,
    ///   name: String,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///    let p = Person {
    ///        uid:  "_:alice".into(),
    ///        name: "Alice".into(),
    ///    };
    ///
    ///    let mut mu = Mutation::new();
    ///    mu.set_set_json(&p).expect("JSON");
    ///
    ///    let client = client().await;
    ///    let txn = client.new_mutated_txn();
    ///    let result = txn.mutate_and_commit_now(mu).await.expect("failed to create data");
    /// }
    /// ```
    ///
    pub async fn mutate_and_commit_now(
        mut self,
        mu: Mutation,
    ) -> Result<MutationResponse, DgraphError> {
        self.do_mutation("", HashMap::<String, String>::with_capacity(0), mu, true)
            .await
    }

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
    /// use dgraph_tonic::{Client, Mutation, Operation};
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
    /// #[tokio::main]
    /// async fn main() {
    ///     let q = r#"
    ///         query {
    ///             user as var(func: eq(email, "wrong_email@dgraph.io"))
    ///         }"#;
    ///
    ///     let mut mu = Mutation::new();
    ///     mu.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);
    ///
    ///     let client = client().await;
    ///     let op = Operation {
    ///         schema: "email: string @index(exact) .".into(),
    ///         ..Default::default()
    ///     };
    ///     client.alter(op).await.expect("Schema is not updated");    
    ///     let txn = client.new_mutated_txn();
    ///     // Upsert: If wrong_email found, update the existing data or else perform a new mutation.
    ///     let response = txn.upsert(q, mu).await.expect("failed to upsert data");
    /// }
    /// ```
    ///
    /// Upsert with more mutations
    /// ```
    /// use dgraph_tonic::{Client, Mutation, Operation};
    /// use std::collections::HashMap;
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
    /// #[tokio::main]
    /// async fn main() {
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
    ///     let client = client().await;
    ///     let op = Operation {
    ///         schema: "email: string @index(exact) .".into(),
    ///         ..Default::default()
    ///     };
    ///     client.alter(op).await.expect("Schema is not updated");
    ///     let txn = client.new_mutated_txn();
    ///     // Upsert: If wrong_email found, update the existing data or else perform a new mutation.
    ///     let response = txn.upsert(q, vec![mu_1, mu_2]).await.expect("failed to upsert data");
    /// }
    /// ```      
    ///
    #[cfg(feature = "dgraph-1-1")]
    pub async fn upsert<Q, M>(mut self, query: Q, mu: M) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        M: Into<UpsertMutation>,
    {
        self.do_mutation(query, HashMap::<String, String>::with_capacity(0), mu, true)
            .await
    }

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
    /// use dgraph_tonic::{Client, Mutation, Operation};
    /// use std::collections::HashMap;
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
    /// #[tokio::main]
    /// async fn main() {
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
    ///     let client = client().await;
    ///     let op = Operation {
    ///         schema: "email: string @index(exact) .".into(),
    ///         ..Default::default()
    ///     };
    ///     client.alter(op).await.expect("Schema is not updated");
    ///     let txn = client.new_mutated_txn();
    ///     // Upsert: If wrong_email found, update the existing data or else perform a new mutation.
    ///     let response = txn.upsert_with_vars(q, vars, mu).await.expect("failed to upsert data");
    /// }
    /// ```
    ///
    /// Upsert with more mutations
    /// ```
    /// use dgraph_tonic::{Client, Mutation, Operation};
    /// use std::collections::HashMap;
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
    /// #[tokio::main]
    /// async fn main() {
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
    ///     let client = client().await;
    ///     let op = Operation {
    ///         schema: "email: string @index(exact) .".into(),
    ///         ..Default::default()
    ///     };
    ///     client.alter(op).await.expect("Schema is not updated");    
    ///     let txn = client.new_mutated_txn();
    ///     // Upsert: If wrong_email found, update the existing data or else perform a new mutation.
    ///     let response = txn.upsert_with_vars(q, vars, vec![mu_1, mu_2]).await.expect("failed to upsert data");
    /// }
    /// ```    
    ///
    #[cfg(feature = "dgraph-1-1")]
    pub async fn upsert_with_vars<Q, K, V, M>(
        mut self,
        query: Q,
        vars: HashMap<K, V>,
        mu: M,
    ) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
        M: Into<UpsertMutation>,
    {
        self.do_mutation(query, vars, mu, true).await
    }
}
