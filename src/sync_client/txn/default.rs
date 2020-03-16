use std::collections::HashMap;
use std::fmt::Debug;

use crate::sync_client::txn::{IState, TxnState, TxnVariant};
use crate::sync_client::Client;
use crate::Request;

#[derive(Clone, Debug)]
pub struct Base;

impl IState for Base {
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

pub type Txn<'a> = TxnVariant<'a, Base>;

impl<'a> TxnVariant<'a, Base> {
    pub fn new(client: &'a Client) -> Txn<'a> {
        Self {
            state: Box::new(TxnState {
                context: Default::default(),
                client,
            }),
            extra: Base {},
        }
    }
}
