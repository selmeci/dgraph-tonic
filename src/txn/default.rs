use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;

use crate::client::ILazyClient;
use crate::stub::Stub;
use crate::txn::{IState, TxnState, TxnVariant};
use crate::Request;

///
/// Inner state for default transaction
///
#[derive(Clone, Debug)]
pub struct Base<C: ILazyClient> {
    mark: PhantomData<C>,
}

impl<C: ILazyClient> IState for Base<C> {
    ///
    /// Create Dgraph request within transaction.
    ///
    fn query_request<S: ILazyClient>(
        &self,
        state: &TxnState<S>,
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
pub type TxnType<C> = TxnVariant<Base<C>, C>;

impl<C: ILazyClient> TxnType<C> {
    ///
    /// Create new default transaction which can do query operations.
    ///
    pub fn new(stub: Stub<C>) -> TxnType<C> {
        Self {
            state: Box::new(TxnState {
                context: Default::default(),
                stub,
            }),
            extra: Base {
                mark: PhantomData {},
            },
        }
    }
}
