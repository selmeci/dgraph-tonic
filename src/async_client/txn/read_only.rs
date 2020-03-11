use crate::async_client::txn::default::Base;
use crate::async_client::txn::{IState, TxnState, TxnVariant};
use crate::Request;
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub struct ReadOnly {
    base: Base,
}

impl IState for ReadOnly {
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

impl TxnVariant<Base> {
    pub fn read_only(self) -> TxnVariant<ReadOnly> {
        TxnVariant {
            state: self.state,
            extra: ReadOnly { base: self.extra },
        }
    }
}
