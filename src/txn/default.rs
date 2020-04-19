use std::collections::HashMap;
use std::fmt::Debug;

use crate::stub::Stub;
use crate::txn::{IState, TxnState, TxnVariant};
use crate::Request;

///
/// Inner state for default transaction
///
#[derive(Clone, Debug)]
pub struct Base;

impl IState for Base {
    ///
    /// Create dGraph request within transaction.
    ///
    fn query_request(
        &self,
        state: &TxnState,
        query: String,
        vars: HashMap<String, String>,
    ) -> Request {
        Request {
            query,
            vars,
            start_ts: state.context.start_ts,
            ..Default::default()
        }
    }
}

///
/// Default transaction state
///
pub type Txn = TxnVariant<Base>;

impl TxnVariant<Base> {
    ///
    /// Create new default transaction which can do query operations.
    ///
    pub fn new(client: Stub) -> Txn {
        Self {
            state: Box::new(TxnState {
                context: Default::default(),
                client,
            }),
            extra: Base {},
        }
    }
}
