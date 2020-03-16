use std::collections::HashMap;
use std::fmt::Debug;

use crate::async_client::txn::default::Base;
use crate::async_client::txn::{IState, TxnState, TxnVariant};
use crate::Request;

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

pub type ReadOnlyTxn<'a> = TxnVariant<'a, ReadOnly>;

impl<'a> TxnVariant<'a, Base> {
    pub fn read_only(self) -> ReadOnlyTxn<'a> {
        TxnVariant {
            state: self.state,
            extra: ReadOnly { base: self.extra },
        }
    }
}
