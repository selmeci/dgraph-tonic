use std::collections::HashMap;
use std::hash::Hash;
use std::marker::{Send, Sync};
use std::ops::{Deref, DerefMut};

use async_trait::async_trait;

use crate::errors::DgraphError;
use crate::stub::Stub;
pub use crate::txn::best_effort::BestEffortTxn;
pub use crate::txn::default::Txn;
pub use crate::txn::mutated::MutatedTxn;
pub use crate::txn::read_only::ReadOnlyTxn;
use crate::IDgraphClient;
use crate::{Request, Response, TxnContext};

mod best_effort;
mod default;
mod mutated;
mod read_only;

#[derive(Clone)]
pub struct TxnState {
    client: Stub,
    context: TxnContext,
}

#[async_trait]
pub trait IState: Send + Sync + Clone {
    async fn commit_or_abort(&self, _state: TxnState) -> Result<(), DgraphError> {
        Ok(())
    }

    fn query_request(
        &self,
        state: &TxnState,
        query: String,
        vars: HashMap<String, String>,
    ) -> Request;
}

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

impl<S: IState> TxnVariant<S> {
    async fn commit_or_abort(self) -> Result<(), DgraphError> {
        let extra = self.extra;
        let state = *self.state;
        extra.commit_or_abort(state).await
    }

    pub async fn discard(mut self) -> Result<(), DgraphError> {
        self.context.aborted = true;
        self.commit_or_abort().await
    }

    pub async fn query<Q>(&mut self, query: Q) -> Result<Response, DgraphError>
    where
        Q: Into<String> + Send + Sync,
    {
        self.query_with_vars(query, HashMap::<String, _, _>::new())
            .await
    }

    pub async fn query_with_vars<Q, K>(
        &mut self,
        query: Q,
        vars: HashMap<K, Q>,
    ) -> Result<Response, DgraphError>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
    {
        let vars = vars.into_iter().fold(HashMap::new(), |mut tmp, (k, v)| {
            tmp.insert(k.into(), v.into());
            tmp
        });
        let request = self.extra.query_request(&self.state, query.into(), vars);
        let response = match IDgraphClient::query(&mut self.client, request).await {
            Ok(response) => response,
            Err(err) => {
                return Err(DgraphError::GrpcError(err.to_string()));
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
    use serde_derive::{Deserialize, Serialize};

    use crate::{Client, Mutation};

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
        let client = Client::new(vec!["http://127.0.0.1:19080"]).await.unwrap();
        let txn = client.new_txn().mutated();
        let p = Person {
            uid: "_:alice".to_string(),
            name: "Alice".to_string(),
        };
        let mu = Mutation::new().with_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate_and_commit_now(mu).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn commit() {
        let client = Client::new(vec!["http://127.0.0.1:19080"]).await.unwrap();
        let mut txn = client.new_txn().mutated();
        //first mutation
        let p = Person {
            uid: "_:alice".to_string(),
            name: "Alice".to_string(),
        };
        let mu = Mutation::new().with_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu).await;
        assert!(response.is_ok());
        //second mutation
        let p = Person {
            uid: "_:mike".to_string(),
            name: "Mike".to_string(),
        };
        let mu = Mutation::new().with_set_json(&p).expect("Invalid JSON");
        let response = txn.mutate(mu).await;
        assert!(response.is_ok());
        //commit
        let commit = txn.commit().await;
        assert!(commit.is_ok())
    }

    #[tokio::test]
    async fn query() {
        let client = Client::new(vec!["http://127.0.0.1:19080"]).await.unwrap();
        let mut txn = client.new_txn().read_only();
        let query = r#"{
            uids(func: eq(name, "Alice")) {
                uid
            }
        }"#;
        let response = txn.query(query).await;
        assert!(response.is_ok());
        let json: UidJson = response.unwrap().try_into().unwrap();
        assert_eq!(json.uids[0].uid, "0x1");
    }
}
