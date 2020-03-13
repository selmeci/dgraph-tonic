use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::{Send, Sync};
use std::ops::{Deref, DerefMut};

use log::error;

use async_trait::async_trait;

pub use crate::async_client::txn::best_effort::BestEffortTxn;
pub use crate::async_client::txn::default::Txn;
pub use crate::async_client::txn::mutated::MutatedTxn;
pub use crate::async_client::txn::read_only::ReadOnlyTxn;
use crate::async_client::{Client, IDgraphClient};
use crate::errors::DgraphError;
use crate::{Request, Response, TxnContext};

mod best_effort;
mod default;
mod mutated;
mod read_only;

#[derive(Clone, Debug)]
pub struct TxnState {
    client: Client,
    context: TxnContext,
}

#[async_trait]
pub trait IState {
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
pub struct TxnVariant<S: IState + Debug + Send + Sync + Clone> {
    state: Box<TxnState>,
    extra: S,
}

impl<S: IState + Debug + Send + Sync + Clone> Deref for TxnVariant<S> {
    type Target = Box<TxnState>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<S: IState + Debug + Send + Sync + Clone> DerefMut for TxnVariant<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl<S: IState + Debug + Send + Sync + Clone> TxnVariant<S> {
    async fn commit_or_abort(self) -> Result<(), DgraphError> {
        let extra = self.extra;
        let state = *self.state;
        extra.commit_or_abort(state).await
    }

    pub fn downcast(self) -> TxnState {
        *self.state
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
        let response = match self.client.query(request).await {
            Ok(response) => response,
            Err(err) => {
                error!("Cannot query dGraph. err: {:?}", err);
                return Err(DgraphError::GrpcError);
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
    use serde_json;
    use tokio::runtime::Runtime;

    use crate::Mutation;

    use super::*;

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

    #[test]
    fn mutate_and_commit_now() {
        let mut rt = Runtime::new().unwrap();
        let client = rt
            .block_on(Client::new(vec!["http://127.0.0.1:19080"].into_iter()))
            .unwrap();
        let txn = client.new_txn().mutated();
        let p = Person {
            uid: "_:alice".to_string(),
            name: "Alice".to_string(),
        };
        let pb = serde_json::to_vec(&p).expect("Invalid json");
        let mu = Mutation {
            set_json: pb,
            ..Default::default()
        };
        let response = rt.block_on(txn.mutate_and_commit_now(mu));
        assert!(response.is_ok());
    }

    #[test]
    fn commit() {
        let mut rt = Runtime::new().unwrap();
        let client = rt
            .block_on(Client::new(vec!["http://127.0.0.1:19080"].into_iter()))
            .unwrap();
        let mut txn = client.new_txn().mutated();
        //first mutation
        let p = Person {
            uid: "_:alice".to_string(),
            name: "Alice".to_string(),
        };
        let pb = serde_json::to_vec(&p).expect("Invalid json");
        let mu = Mutation {
            set_json: pb,
            ..Default::default()
        };
        let response = rt.block_on(txn.mutate(mu));
        assert!(response.is_ok());
        //second mutation
        let p = Person {
            uid: "_:mike".to_string(),
            name: "Mike".to_string(),
        };
        let pb = serde_json::to_vec(&p).expect("Invalid json");
        let mu = Mutation {
            set_json: pb,
            ..Default::default()
        };
        let response = rt.block_on(txn.mutate(mu));
        assert!(response.is_ok());
        //commit
        let commit = rt.block_on(txn.commit());
        assert!(commit.is_ok())
    }

    #[test]
    fn query() {
        let mut rt = Runtime::new().unwrap();
        let client = rt
            .block_on(Client::new(vec!["http://127.0.0.1:19080"].into_iter()))
            .unwrap();
        let mut txn = client.new_txn().read_only();
        let query = r#"{
            uids(func: eq(name, "Alice")) {
                uid
            }
        }"#;
        let response = rt.block_on(txn.query(query));
        assert!(response.is_ok());
        let json: UidJson = serde_json::from_slice(&response.unwrap().json).unwrap();
        assert_eq!(json.uids[0].uid, "0x1");
    }
}
