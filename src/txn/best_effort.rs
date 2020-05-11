use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::fmt::Debug;

use crate::client::ILazyClient;
use crate::txn::read_only::ReadOnly;
use crate::txn::{IState, TxnReadOnlyType, TxnState, TxnVariant};
use crate::Request;

///
/// Inner state for best effort transaction
///
#[derive(Clone, Debug)]
pub struct BestEffort<C: ILazyClient> {
    read_only: ReadOnly<C>,
}

impl<C: ILazyClient> IState for BestEffort<C> {
    ///
    /// Update read only query with best_effort flag
    ///
    fn query_request<S: ILazyClient>(
        &self,
        state: &TxnState<S>,
        query: String,
        vars: HashMap<String, String, RandomState>,
    ) -> Request {
        let mut request = self.read_only.query_request(state, query, vars);
        request.best_effort = true;
        request
    }
}

///
/// Best effort variant of read only transaction
///
pub type TxnBestEffortType<C> = TxnVariant<BestEffort<C>, C>;

impl<C: ILazyClient> TxnReadOnlyType<C> {
    ///
    /// Create best effort transaction from read only state
    ///
    pub fn best_effort(self) -> TxnBestEffortType<C> {
        TxnVariant {
            state: self.state,
            extra: BestEffort {
                read_only: self.extra,
            },
        }
    }
}
