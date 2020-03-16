use std::convert::TryInto;
use std::fmt::{self, Debug, Formatter};
use std::path::Path;
use std::sync::Mutex;

use failure::Error;
use tokio::runtime::{Builder, Runtime};
use tonic::transport::Endpoint;
use tonic::Status;

use crate::async_client::{Client as AsyncClient, IDgraphClient as AsyncIDgraphClient};
pub use crate::sync_client::txn::{BestEffortTxn, MutatedTxn, ReadOnlyTxn, Txn};
use crate::{
    Assigned, ClientResult, Mutation, Operation, Payload, Request as DgraphRequest,
    Response as DgraphResponse, TxnContext, Version,
};

mod txn;

pub trait IDgraphClient: Clone + Sized {
    fn login(&self, user_id: String, password: String) -> ClientResult<DgraphResponse, Status>;

    fn query(&self, query: DgraphRequest) -> ClientResult<DgraphResponse, Status>;

    fn mutate(&self, mu: Mutation) -> ClientResult<Assigned, Status>;

    fn alter(&self, op: Operation) -> ClientResult<Payload, Status>;

    fn commit_or_abort(&self, txn: TxnContext) -> ClientResult<TxnContext, Status>;

    fn check_version(&self) -> ClientResult<Version, Status>;
}

pub struct Client {
    rt: Mutex<Runtime>,
    client: AsyncClient,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        unimplemented!()
    }
}

impl Debug for Client {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SyncDgraphClient")
    }
}

impl Client {
    fn rt() -> Mutex<Runtime> {
        Mutex::new(
            Builder::new()
                .basic_scheduler()
                .enable_all()
                .build()
                .unwrap(),
        )
    }

    pub fn new<S: TryInto<Endpoint>>(endpoints: impl Iterator<Item = S>) -> Result<Self, Error> {
        let rt = Self::rt();
        let client = {
            let mut me_rt = rt.lock().unwrap();
            me_rt.block_on(AsyncClient::new(endpoints))?
        };
        Ok(Self { rt, client })
    }

    pub fn new_with_tls_client_auth<S: TryInto<Endpoint>>(
        domain_name: impl Into<String>,
        endpoints: impl Iterator<Item = S>,
        server_root_ca_cert: impl AsRef<Path>,
        client_cert: impl AsRef<Path>,
        client_key: impl AsRef<Path>,
    ) -> Result<Self, Error> {
        let rt = Self::rt();
        let client = {
            let mut me_rt = rt.lock().unwrap();
            me_rt.block_on(AsyncClient::new_with_tls_client_auth(
                domain_name,
                endpoints,
                server_root_ca_cert,
                client_cert,
                client_key,
            ))?
        };
        Ok(Self { rt, client })
    }

    pub fn new_txn(&self) -> Txn {
        Txn::new(&self)
    }
}

impl IDgraphClient for Client {
    fn login(&self, user_id: String, password: String) -> ClientResult<DgraphResponse, Status> {
        let mut rt = self.rt.lock().unwrap();
        rt.block_on(self.client.login(user_id, password))
    }

    fn query(&self, query: DgraphRequest) -> ClientResult<DgraphResponse, Status> {
        let mut rt = self.rt.lock().unwrap();
        rt.block_on(self.client.query(query))
    }

    fn mutate(&self, mu: Mutation) -> ClientResult<Assigned, Status> {
        let mut rt = self.rt.lock().unwrap();
        rt.block_on(self.client.mutate(mu))
    }

    fn alter(&self, op: Operation) -> ClientResult<Payload, Status> {
        let mut rt = self.rt.lock().unwrap();
        rt.block_on(self.client.alter(op))
    }

    fn commit_or_abort(&self, txn: TxnContext) -> ClientResult<TxnContext, Status> {
        let mut rt = self.rt.lock().unwrap();
        rt.block_on(self.client.commit_or_abort(txn))
    }

    fn check_version(&self) -> ClientResult<Version, Status> {
        let mut rt = self.rt.lock().unwrap();
        rt.block_on(self.client.check_version())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alter() {
        let client = Client::new(vec!["http://127.0.0.1:19080"].into_iter()).unwrap();
        let op = Operation {
            schema: "name: string @index(exact) .".into(),
            ..Default::default()
        };
        let response = client.alter(op);
        assert!(response.is_ok());
    }
}
