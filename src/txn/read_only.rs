use std::collections::HashMap;
use std::fmt::Debug;

use crate::txn::default::Base;
use crate::txn::{IState, TxnState, TxnVariant};
use crate::Request;

///
/// Inner state for read only transaction
///
#[derive(Clone, Debug)]
pub struct ReadOnly {
    base: Base,
}

impl IState for ReadOnly {
    ///
    /// Update query request with read_only flag
    ///
    fn query_request(
        &self,
        state: &TxnState,
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
pub type ReadOnlyTxn = TxnVariant<ReadOnly>;

impl TxnVariant<Base> {
    ///
    /// Create new read only transaction from default transaction state
    ///
    pub fn read_only(self) -> ReadOnlyTxn {
        TxnVariant {
            state: self.state,
            extra: ReadOnly { base: self.extra },
        }
    }
}
