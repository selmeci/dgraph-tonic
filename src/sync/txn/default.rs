use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use tokio::runtime::Runtime;

use crate::client::ILazyClient;
use crate::sync::txn::{IState, TxnState, TxnVariant};
use crate::txn::TxnType as AsyncTxn;
use crate::{Query, Response};

///
/// Inner state for default transaction
///
#[derive(Clone, Debug)]
pub struct Base<C: ILazyClient> {
    pub(crate) rt: Arc<Mutex<Runtime>>,
    pub(crate) async_txn: Arc<Mutex<AsyncTxn<C>>>,
}

#[async_trait]
impl<C: ILazyClient> IState for Base<C> {
    fn query_with_vars<Q, K, V>(
        &mut self,
        query: Q,
        vars: HashMap<K, V, RandomState>,
    ) -> Result<Response>
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

    fn query_rdf_with_vars<Q, K, V>(&mut self, query: Q, vars: HashMap<K, V>) -> Result<Response>
    where
        Q: Into<String> + Send + Sync,
        K: Into<String> + Send + Sync + Eq + Hash,
        V: Into<String> + Send + Sync,
    {
        let mut rt = self.rt.lock().expect("Tokio Runtime");
        let async_txn = Arc::clone(&self.async_txn);
        rt.block_on(async move {
            let mut async_txn = async_txn.lock().expect("Async Txn");
            async_txn.query_rdf_with_vars(query, vars).await
        })
    }
}

///
/// Default transaction state
///
pub type TxnType<C> = TxnVariant<Base<C>>;

impl<C: ILazyClient> TxnType<C> {
    ///
    /// Create new default transaction which can do query operations.
    ///
    pub fn new(rt: Arc<Mutex<Runtime>>, async_txn: AsyncTxn<C>) -> TxnType<C> {
        Self {
            state: Box::new(TxnState {}),
            extra: Base {
                rt,
                async_txn: Arc::new(Mutex::new(async_txn)),
            },
        }
    }
}
