use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use async_trait::async_trait;

use crate::errors::DgraphError;
use crate::txn::default::Base;
use crate::txn::{IState, TxnState, TxnVariant};
#[cfg(feature = "dgraph-1-0")]
use crate::Assigned;
use crate::IDgraphClient;
#[cfg(feature = "dgraph-1-1")]
use crate::Response;
use crate::{Mutation, Request};

///
/// In dGraph v1.0.x is mutation response represented as Assigned object
///
#[cfg(feature = "dgraph-1-0")]
pub type MutationResponse = Assigned;
///
/// In dGraph v1.1.x is mutation response represented as Response object
///
#[cfg(feature = "dgraph-1-1")]
pub type MutationResponse = Response;

#[derive(Clone, Debug)]
pub struct Mutated {
    base: Base,
    mutated: bool,
}

#[async_trait]
impl IState for Mutated {
    fn query_request(
        &self,
        state: &TxnState,
        query: String,
        vars: HashMap<String, String, RandomState>,
    ) -> Request {
        self.base.query_request(state, query, vars)
    }
}

pub type MutatedTxn = TxnVariant<Mutated>;

impl TxnVariant<Base> {
    pub fn mutated(self) -> MutatedTxn {
        TxnVariant {
            state: self.state,
            extra: Mutated {
                base: self.extra,
                mutated: false,
            },
        }
    }
}

impl TxnVariant<Mutated> {
    #[cfg(feature = "dgraph-1-0")]
    async fn do_mutation<Q, K, V>(
        &mut self,
        query: Q,
        vars: HashMap<K, V>,
        mut mu: Mutation,
    ) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
    {
        self.extra.mutated = true;
        mu.start_ts = self.context.start_ts;
        let assigned = match self.client.mutate(mu).await {
            Ok(assigned) => assigned,
            Err(err) => {
                return Err(DgraphError::GrpcError(err.to_string()));
            }
        };
        match assigned.context.as_ref() {
            Some(src) => self.context.merge_context(src)?,
            None => return Err(DgraphError::MissingTxnContext),
        }
        Ok(assigned)
    }

    #[cfg(feature = "dgraph-1-1")]
    async fn do_mutation<Q, K, V>(
        &mut self,
        query: Q,
        vars: HashMap<K, V>,
        mu: Mutation,
    ) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
    {
        self.extra.mutated = true;
        let vars = vars.into_iter().fold(HashMap::new(), |mut tmp, (k, v)| {
            tmp.insert(k.into(), v.into());
            tmp
        });
        let request = Request {
            query: query.into(),
            vars,
            start_ts: self.context.start_ts,
            commit_now: mu.commit_now,
            mutations: vec![mu],
            ..Default::default()
        };
        let response = match self.client.do_request(request).await {
            Ok(response) => response,
            Err(err) => {
                return Err(DgraphError::GrpcError(err.to_string()));
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
            Err(err) => Err(DgraphError::GrpcError(err.to_string())),
        }
    }

    pub async fn discard(mut self) -> Result<(), DgraphError> {
        self.context.aborted = true;
        self.commit_or_abort().await
    }

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
    ///
    ///#[derive(Serialize)]
    /// struct Person {
    ///   uid: String,
    ///   name: String,
    /// }
    ///
    /// let p = Person {
    ///   uid:  "_:alice".into(),
    ///   name: "Alice".into(),
    /// };
    ///
    /// let mu = Mutation::new().with_set_json(&p)?;
    ///
    /// let txn = client.new_mutated_txn();
    /// let result = txn.mutate(mu).await.expect("failed to create data");
    /// txn.commit().await?;
    /// ```
    ///
    pub async fn mutate(&mut self, mut mu: Mutation) -> Result<MutationResponse, DgraphError> {
        mu.commit_now = false;
        self.do_mutation("", HashMap::<String, String>::with_capacity(0), mu)
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
    ///
    ///#[derive(Serialize)]
    /// struct Person {
    ///   uid: String,
    ///   name: String,
    /// }
    ///
    /// let p = Person {
    ///   uid:  "_:alice".into(),
    ///   name: "Alice".into(),
    /// };
    ///
    /// let mu = Mutation::new().with_set_json(&p)?;
    ///
    /// let txn = client.new_mutated_txn();
    /// let result = txn.mutate_and_commit_now(mu).await.expect("failed to create data");
    /// ```
    ///
    pub async fn mutate_and_commit_now(
        mut self,
        mut mu: Mutation,
    ) -> Result<MutationResponse, DgraphError> {
        mu.commit_now = true;
        self.do_mutation("", HashMap::<String, String>::with_capacity(0), mu)
            .await
    }

    ///
    /// Adding or removing data in Dgraph is called a mutation.
    ///
    /// This function allows you to run upserts consisting of one query and one mutation.
    ///
    ///
    /// # Arguments
    ///
    /// * `q`: dGraph query
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
    ///
    /// let q = r#"
    ///   query {
    ///       user as var(func: eq(email, "wrong_email@dgraph.io"))
    ///   }"#;
    ///
    /// let mut mu = Mutation::new();
    /// mu.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);
    ///
    /// let txn = client.new_mutated_txn();
    /// // Upsert: If wrong_email found, update the existing data or else perform a new mutation.
    /// let response = txn.query_and_mutate(q, mu).await.expect("failed to upsert data");
    /// txn.commit().await?;
    /// ```
    ///
    #[cfg(feature = "dgraph-1-1")]
    pub async fn query_and_mutate<Q>(
        &mut self,
        query: Q,
        mut mu: Mutation,
    ) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
    {
        mu.commit_now = false;
        self.do_mutation(query, HashMap::<String, String>::with_capacity(0), mu)
            .await
    }

    ///
    /// Adding or removing data in Dgraph is called a mutation.
    ///
    /// This function allows you to run upserts consisting of one query and one mutation.
    ///
    /// Sometimes, you only want to commit a mutation, without querying anything further.
    /// In such cases, you can use this function to indicate that the mutation must be immediately
    /// committed.
    ///
    /// # Arguments
    ///
    /// * `q`: dGraph query
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
    ///
    /// let q = r#"
    ///   query {
    ///       user as var(func: eq(email, "wrong_email@dgraph.io"))
    ///   }"#;
    ///
    /// let mut mu = Mutation::new();
    /// mu.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);
    ///
    /// let txn = client.new_mutated_txn();
    /// // Upsert: If wrong_email found, update the existing data or else perform a new mutation.
    /// let response = txn.query_and_mutate_and_commit_now(q, mu).await.expect("failed to upsert data");
    /// ```
    ///
    #[cfg(feature = "dgraph-1-1")]
    pub async fn query_and_mutate_and_commit_now<Q>(
        mut self,
        query: Q,
        mut mu: Mutation,
    ) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
    {
        mu.commit_now = true;
        self.do_mutation(query, HashMap::<String, String>::with_capacity(0), mu)
            .await
    }

    ///
    /// Adding or removing data in Dgraph is called a mutation.
    ///
    /// This function allows you to run upserts consisting of one query and one mutation.
    ///
    ///
    /// # Arguments
    ///
    /// * `q`: dGraph query
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
    /// ```
    /// use dgraph_tonic::{Client, Mutation};
    /// use std::collections::HashMap;
    ///
    /// let q = r#"
    ///   query {
    ///       user as var(func: eq(email, $email))
    ///   }"#;
    /// let mut vars = HashMap::new();
    /// vars.insert("$email",wrong_email@dgraph.io);
    ///
    /// let mut mu = Mutation::new();
    /// mu.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);
    ///
    /// let txn = client.new_mutated_txn();
    /// // Upsert: If wrong_email found, update the existing data or else perform a new mutation.
    /// let response = txn.query_with_vars_and_mutate(q, vars, mu).await.expect("failed to upsert data");
    /// txn.commit().await?;
    /// ```
    ///
    #[cfg(feature = "dgraph-1-1")]
    pub async fn query_with_vars_and_mutate<Q, K, V>(
        &mut self,
        query: Q,
        vars: HashMap<K, V>,
        mut mu: Mutation,
    ) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
    {
        mu.commit_now = false;
        self.do_mutation(query, vars, mu).await
    }

    ///
    /// Adding or removing data in Dgraph is called a mutation.
    ///
    /// This function allows you to run upserts consisting of one query and one mutation.
    ///
    /// Sometimes, you only want to commit a mutation, without querying anything further.
    /// In such cases, you can use this function to indicate that the mutation must be immediately
    /// committed.
    ///
    /// # Arguments
    ///
    /// * `q`: dGraph query
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
    /// ```
    /// use dgraph_tonic::{Client, Mutation};
    /// use std::collections::HashMap;
    ///
    /// let q = r#"
    ///   query {
    ///       user as var(func: eq(email, $email))
    ///   }"#;
    /// let mut vars = HashMap::new();
    /// vars.insert("$email",wrong_email@dgraph.io);
    ///
    /// let mut mu = Mutation::new();
    /// mu.set_set_nquads(r#"uid(user) <email> "correct_email@dgraph.io" ."#);
    ///
    /// let txn = client.new_mutated_txn();
    /// // Upsert: If wrong_email found, update the existing data or else perform a new mutation.
    /// let response = txn.query_with_vars_and_mutate_and_commit_now(q, vars, mu).await.expect("failed to upsert data");
    /// ```
    ///
    #[cfg(feature = "dgraph-1-1")]
    pub async fn query_with_vars_and_mutate_and_commit_now<Q, K, V>(
        mut self,
        query: Q,
        vars: HashMap<K, V>,
        mut mu: Mutation,
    ) -> Result<MutationResponse, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
    {
        mu.commit_now = true;
        self.do_mutation(query, vars, mu).await
    }
}
