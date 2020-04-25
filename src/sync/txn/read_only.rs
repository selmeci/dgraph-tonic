use std::collections::HashMap;
use std::fmt::Debug;

use crate::client::ILazyClient;
use crate::sync::txn::{IState, Txn, TxnVariant};
use crate::txn::ReadOnlyTxn as AsyncReadOnlyTxn;
use crate::{DgraphError, Response, Result};
use async_trait::async_trait;
use std::collections::hash_map::RandomState;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

///
/// Inner state for read only transaction
///
#[derive(Clone, Debug)]
pub struct ReadOnly<C: ILazyClient> {
    pub(crate) rt: Arc<Mutex<Runtime>>,
    pub(crate) async_txn: Arc<Mutex<AsyncReadOnlyTxn<C>>>,
}

#[async_trait]
impl<C: ILazyClient> IState for ReadOnly<C> {
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
/// ReadOnly variant of transaction
///
pub type ReadOnlyTxn<C> = TxnVariant<ReadOnly<C>>;

impl<C: ILazyClient> Txn<C> {
    ///
    /// Create new read only transaction from default transaction state
    ///
    pub fn read_only(self) -> ReadOnlyTxn<C> {
        let rt = self.extra.rt;
        let txn = self
            .extra
            .async_txn
            .lock()
            .expect("Txn")
            .to_owned()
            .read_only();
        TxnVariant {
            state: self.state,
            extra: ReadOnly {
                rt,
                async_txn: Arc::new(Mutex::new(txn)),
            },
        }
    }
}
