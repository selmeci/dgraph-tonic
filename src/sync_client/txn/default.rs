use crate::sync_client::txn::{IState, TxnState, TxnVariant};
use crate::sync_client::Client;
use crate::Request;
use std::collections::HashMap;
use std::fmt::Debug;

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

pub type Txn = TxnVariant<Base>;

impl TxnVariant<Base> {
    pub fn new(client: Client) -> TxnVariant<Base> {
        Self {
            state: Box::new(TxnState {
                context: Default::default(),
                client,
            }),
            extra: Base {},
        }
    }
}
