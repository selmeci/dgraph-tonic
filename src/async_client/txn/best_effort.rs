use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::fmt::Debug;

use crate::async_client::txn::read_only::ReadOnly;
use crate::async_client::txn::{IState, TxnState, TxnVariant};
use crate::Request;

#[derive(Clone, Debug)]
pub struct BestEffort {
    read_only: ReadOnly,
}

impl IState for BestEffort {
    fn query_request(
        &self,
        state: &TxnState,
        query: String,
        vars: HashMap<String, String, RandomState>,
    ) -> Request {
        let mut request = self.read_only.query_request(state, query, vars);
        request.best_effort = true;
        request
    }
}

pub type BestEffortTxn<'a> = TxnVariant<'a, BestEffort>;

impl<'a> TxnVariant<'a, ReadOnly> {
    pub fn best_effort(self) -> BestEffortTxn<'a> {
        TxnVariant {
            state: self.state,
            extra: BestEffort {
                read_only: self.extra,
            },
        }
    }
}
