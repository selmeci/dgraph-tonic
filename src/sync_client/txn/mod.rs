use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::{Send, Sync};
use std::ops::{Deref, DerefMut};

use log::error;

use crate::errors::DgraphError;
pub use crate::sync_client::txn::best_effort::BestEffortTxn;
pub use crate::sync_client::txn::default::Txn;
pub use crate::sync_client::txn::mutated::MutatedTxn;
pub use crate::sync_client::txn::read_only::ReadOnlyTxn;
use crate::sync_client::{Client, IDgraphClient};
use crate::{Request, Response, TxnContext};

mod best_effort;
mod default;
mod mutated;
mod read_only;

#[derive(Clone, Debug)]
pub struct TxnState<'a> {
    client: &'a Client,
    context: TxnContext,
}

pub trait IState {
    fn commit_or_abort(&self, state: TxnState<'_>) -> Result<(), DgraphError> {
        let _ = state;
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
pub struct TxnVariant<'a, S: IState + Debug + Send + Sync + Clone> {
    state: Box<TxnState<'a>>,
    extra: S,
}

impl<'a, S: IState + Debug + Send + Sync + Clone> Deref for TxnVariant<'a, S> {
    type Target = Box<TxnState<'a>>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<'a, S: IState + Debug + Send + Sync + Clone> DerefMut for TxnVariant<'a, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl<'a, S: IState + Debug + Send + Sync + Clone> TxnVariant<'a, S> {
    fn commit_or_abort(self) -> Result<(), DgraphError> {
        let extra = self.extra;
        let state = *self.state;
        extra.commit_or_abort(state)
    }

    pub fn discard(mut self) -> Result<(), DgraphError> {
        self.context.aborted = true;
        self.commit_or_abort()
    }

    pub fn query<Q>(&mut self, query: Q) -> Result<Response, DgraphError>
    where
        Q: Into<String> + Send + Sync,
    {
        self.query_with_vars(query, HashMap::<String, _, _>::new())
    }

    pub fn query_with_vars<Q, K>(
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
        let response = match self.client.query(request) {
            Ok(response) => response,
            Err(err) => {
                error!("Cannot query dGraph. err: {:?}", err);
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
    use serde_json;

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
        let client = Client::new(vec!["http://127.0.0.1:19080"].into_iter()).unwrap();
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
        let response = txn.mutate_and_commit_now(mu);
        assert!(response.is_ok());
    }

    #[test]
    fn commit() {
        let client = Client::new(vec!["http://127.0.0.1:19080"].into_iter()).unwrap();
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
        let response = txn.mutate(mu);
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
        let response = txn.mutate(mu);
        assert!(response.is_ok());
        //commit
        let commit = txn.commit();
        assert!(commit.is_ok())
    }

    #[test]
    fn query() {
        let client = Client::new(vec!["http://127.0.0.1:19080"].into_iter()).unwrap();
        let mut txn = client.new_txn().read_only();
        let query = r#"{
            uids(func: eq(name, "Alice")) {
                uid
            }
        }"#;
        let response = txn.query(query);
        assert!(response.is_ok());
        let json: UidJson = serde_json::from_slice(&response.unwrap().json).unwrap();
        assert_eq!(json.uids[0].uid, "0x1");
    }
}
