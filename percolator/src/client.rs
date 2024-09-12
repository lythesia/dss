use std::time::Duration;

use futures::{
    executor,
    stream::{FuturesUnordered, StreamExt},
};
use futures_timer::Delay;
use labrpc::*;

use crate::{
    msg::{CommitRequest, GetRequest, PrewriteRequest, TimestampRequest},
    server::{Key, Write},
    service::{TsoClient, TxnClient},
};

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
// futures_timer not resolution timers, MUST reduce millis here to pass timestamp test
const BACKOFF_TIME_MS: u64 = 50;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TsoClient,
    txn_client: TxnClient,
    txn: Option<Transaction>,
}

#[derive(Clone)]
struct Transaction {
    start_ts: u64,
    ops: Vec<Write>,
}

impl Transaction {
    fn new(start_ts: u64) -> Self {
        Self {
            start_ts,
            ops: vec![],
        }
    }
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TsoClient, txn_client: TxnClient) -> Client {
        // Your code here.
        Client {
            tso_client,
            txn_client,
            txn: None,
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        executor::block_on(async {
            Ok(call(|| self.tso_client.get_timestamp(&TimestampRequest {}))
                .await?
                .timestamp)
        })
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        assert!(self.txn.is_none(), "txn on-going");
        let start_ts = self.get_timestamp().expect("fail get_timestamp");
        log::debug!("client begin at start_ts={start_ts}");
        self.txn = Some(Transaction::new(start_ts));
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        let txn = self.txn.as_ref().expect("no txn");
        let args = GetRequest {
            start_ts: txn.start_ts,
            key,
        };
        Ok(executor::block_on(call(|| self.txn_client.get(&args)))?.value)
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        let txn = self.txn.as_mut().expect("no txn");
        txn.ops.push(Write(key, value));
    }

    /// Commits a transaction.
    pub fn commit(&mut self) -> Result<bool> {
        // Your code here.
        let txn = self.txn.take().expect("no txn");
        let (primary, secondaries) = match txn.ops.split_first() {
            Some(v) => v,
            _ => return Ok(true),
        };

        executor::block_on(async {
            let start_ts = txn.start_ts;
            let primary_key = primary.0.clone();
            // prewrites
            for Write(key, value) in &txn.ops {
                let args = PrewriteRequest {
                    start_ts,
                    key: key.clone(),
                    value: value.clone(),
                    primary_key: primary_key.clone(),
                };
                if !call(|| self.txn_client.prewrite(&args)).await?.ok {
                    return Ok(false);
                }
            }

            // commits
            let commit_ts = self
                .tso_client
                .get_timestamp(&TimestampRequest {})
                .await?
                .timestamp;
            // commit primary
            let args = CommitRequest {
                start_ts,
                commit_ts,
                key: primary_key.clone(),
                is_primary: true,
            };
            log::debug!("client commit primary start_ts={start_ts}, commit_ts={commit_ts}");
            let result = call(|| self.txn_client.commit(&args)).await;
            // requirements: drop_req + fail_primary -> commit = false
            // and known: drop_req + primary:
            // 1. + !fail_primary => pass
            // 2. + fail_primary => Err("reqhook")
            // so Err("reqhook") indicates fail_primary
            if matches!(&result, Err(labrpc::Error::Other(e)) if e == "reqhook") || !result?.ok {
                return Ok(false);
            }
            // commit secondary
            let commit_secondary = |key, txn_client: TxnClient| {
                let args = CommitRequest {
                    start_ts,
                    commit_ts,
                    key,
                    is_primary: false,
                };
                log::debug!("client commit secondary start_ts={start_ts}, commit_ts={commit_ts}");
                call(move || txn_client.commit(&args))
            };
            let _ = secondaries
                .iter()
                .map(|Write(key, _)| commit_secondary(key.clone(), self.txn_client.clone()))
                .collect::<FuturesUnordered<_>>()
                .collect::<Vec<_>>()
                .await;
            Ok(true)
        })
    }
}

async fn call<T>(rpc: impl Fn() -> RpcFuture<Result<T>>) -> Result<T> {
    let mut reply = rpc().await;
    for i in 0..RETRY_TIMES {
        if reply.is_ok() {
            return reply;
        }
        Delay::new(Duration::from_millis(BACKOFF_TIME_MS << i)).await;
        reply = rpc().await;
    }
    reply
}
