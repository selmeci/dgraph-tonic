use std::collections::HashMap;
use std::fmt::Debug;

use crate::client::ILazyClient;
use crate::sync::txn::{IState, ReadOnlyTxn, TxnVariant};
use crate::txn::BestEffortTxn as AsyncBestEffortTxn;
use crate::{DgraphError, Query, Response, Result};
use async_trait::async_trait;
use std::collections::hash_map::RandomState;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

///
/// Inner state for read only transaction
///
#[derive(Clone, Debug)]
pub struct BestEffort<C: ILazyClient> {
    pub(crate) rt: Arc<Mutex<Runtime>>,
    pub(crate) async_txn: Arc<Mutex<AsyncBestEffortTxn<C>>>,
}

#[async_trait]
impl<C: ILazyClient> IState for BestEffort<C> {
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
/// Best effort variant of transaction
///
pub type BestEffortTxn<C> = TxnVariant<BestEffort<C>>;

impl<C: ILazyClient> ReadOnlyTxn<C> {
    ///
    /// Create best effort transaction from read only state
    ///
    pub fn best_effort(self) -> BestEffortTxn<C> {
        let rt = self.extra.rt;
        let txn = self
            .extra
            .async_txn
            .lock()
            .expect("Txn")
            .to_owned()
            .best_effort();
        TxnVariant {
            state: self.state,
            extra: BestEffort {
                rt,
                async_txn: Arc::new(Mutex::new(txn)),
            },
        }
    }
}
