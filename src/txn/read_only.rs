use std::collections::HashMap;
use std::fmt::Debug;

use crate::client::ILazyClient;
use crate::txn::default::Base;
use crate::txn::{IState, TxnState, TxnType, TxnVariant};
use crate::Request;

///
/// Inner state for read only transaction
///
#[derive(Clone, Debug)]
pub struct ReadOnly<C: ILazyClient> {
    base: Base<C>,
}

impl<C: ILazyClient> IState for ReadOnly<C> {
    ///
    /// Update query request with read_only flag
    ///
    fn query_request<S: ILazyClient>(
        &self,
        state: &TxnState<S>,
        query: String,
        vars: HashMap<String, String>,
    ) -> Request {
        let mut request = self.base.query_request(state, query, vars);
        request.read_only = true;
        request
    }
}

///
/// ReadOnly variant of transaction
///
pub type ReadOnlyTxn<C> = TxnVariant<ReadOnly<C>, C>;

impl<C: ILazyClient> TxnType<C> {
    ///
    /// Create new read only transaction from default transaction state
    ///
    pub fn read_only(self) -> ReadOnlyTxn<C> {
        TxnVariant {
            state: self.state,
            extra: ReadOnly { base: self.extra },
        }
    }
}
