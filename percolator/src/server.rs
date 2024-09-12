use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::u64;

use futures_timer::Delay;

use crate::msg::*;
use crate::service::*;
use crate::*;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: Duration = Duration::from_millis(100);

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
    counter: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        // Your code here.
        let timestamp = self.counter.fetch_add(1, Ordering::Relaxed);
        Ok(TimestampResponse { timestamp })
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

// todo: I think mix data/ptr/lock here is a bad design
#[derive(Clone, PartialEq)]
pub enum Value {
    Vector(Vec<u8>),    // data OR
    Primary(Instant),   // primary lock
    Secondary(Vec<u8>), // pointer to primary lock (which is key of primary row)
    Timestamp(u64),     // pointer to data
}

impl Value {
    fn primary_lock(&self) -> &Instant {
        match self {
            Value::Primary(v) => v,
            _ => panic!("value not primary lock"),
        }
    }

    fn secondary_lock(&self) -> &Vec<u8> {
        match self {
            Value::Secondary(k) => k,
            _ => panic!("value not secondary lock"),
        }
    }

    fn data(&self) -> &Vec<u8> {
        match self {
            Value::Vector(v) => v,
            _ => panic!("value not data"),
        }
    }

    fn timestamp(&self) -> u64 {
        match self {
            Value::Timestamp(v) => *v,
            _ => panic!("value not timestamp"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Write(pub Vec<u8>, pub Vec<u8>);

pub enum Column {
    Data,
    Lock,
    Write,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
    write: BTreeMap<Key, Value>, // `Value` MUST be `Timestamp = start_ts` of txn, points to `data[(key, starts)]`
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.
        let start_ts = ts_start_inclusive.unwrap_or(0);
        let end_ts = ts_end_inclusive.unwrap_or(u64::MAX);
        assert!(
            start_ts <= end_ts,
            "start_ts {} > end_ts {}",
            start_ts,
            end_ts
        );
        let range = (key.clone(), start_ts)..=(key, end_ts);

        match column {
            Column::Data => self.data.range(range).last(),
            Column::Lock => self.lock.range(range).last(),
            Column::Write => self.write.range(range).last(),
        }
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        let key = (key, ts);
        match column {
            Column::Data => self.data.insert(key, value),
            Column::Lock => self.lock.insert(key, value),
            Column::Write => self.write.insert(key, value),
        };
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, ts: u64) -> Option<Value> {
        // Your code here.
        let key = (key, ts);
        match column {
            Column::Data => self.data.remove(&key),
            Column::Lock => self.lock.remove(&key),
            Column::Write => self.write.remove(&key),
        }
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        // Your code here.
        self.back_off_maybe_clean_up_lock(req.start_ts, req.key.clone())
            .await;

        let mut store = self.data.lock().unwrap();
        // find start_ts of last commit at write column
        let start_ts = match store.read(req.key.clone(), Column::Write, None, Some(req.start_ts)) {
            Some((_, v)) => v.timestamp(),
            _ => return Ok(GetResponse { value: vec![] }),
        };
        // find value at data column with start_ts
        let (_, value) = store
            .read(req.key, Column::Data, Some(start_ts), Some(start_ts))
            .expect("Column::Data no such key");
        Ok(GetResponse {
            value: value.data().clone(),
        })
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        // Your code here.
        let mut store = self.data.lock().unwrap();
        // if ts(0..) has lock => other txn locked
        if store.read(req.key.clone(), Column::Lock, None, None).is_some() ||
        // if ts(start_ts..) has write => other txn may NOT commit
        // 也就是说必须保证当前txn开始前其他txn都是complete的状态
            store.read(req.key.clone(), Column::Write, Some(req.start_ts), None).is_some()
        {
            return Ok(PrewriteResponse { ok: false });
        }

        // add `lock`
        store.write(
            req.key.clone(),
            Column::Lock,
            req.start_ts,
            // if key == primary_key: primary lock
            // else: secondary lock (points to primary lock)
            if req.key == req.primary_key {
                Value::Primary(Instant::now())
            } else {
                Value::Secondary(req.primary_key)
            },
        );

        // add `data`
        store.write(
            req.key,
            Column::Data,
            req.start_ts,
            Value::Vector(req.value),
        );

        Ok(PrewriteResponse { ok: true })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        // Your code here.
        let mut store = self.data.lock().unwrap();

        // clear lock
        let lock = store.erase(req.key.clone(), Column::Lock, req.start_ts);
        // no locks?
        if req.is_primary && lock.is_none() {
            return Ok(CommitResponse { ok: false });
        }

        // add `write`, values points to `data` at start_ts
        store.write(
            req.key,
            Column::Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );

        Ok(CommitResponse { ok: true })
    }
}

const INTERVAL: Duration = Duration::from_millis(50);
impl MemoryStorage {
    async fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        // Your code here.
        // rules if txn A (current txn) see's txn B's lock:
        // case.1: if B no primary_lock, and write column has `commit_ts` => means B has started commit process,
        // then roll-forward B: clear locks + `commit_ts`s to write column
        // case.2: if B no primary_lock, and write column no `commit_ts` => means B has been roll-back,
        // then do clean up for B
        // case.3: if B has primary_lock, and TTL expired then:
        // clear B's primary_lock, then others
        // case.4: if B has primary_lock, and TTL not expired, then:
        // has to wait B commit or rollback by its client
        loop {
            {
                let mut store = self.data.lock().unwrap();
                let (start_ts, lock) =
                    match store.read(key.clone(), Column::Lock, None, Some(start_ts)) {
                        // if ts(0..=start_ts) has lock => other txn locked
                        // get its start_ts and lock
                        Some(((_, start_ts), lock)) => (*start_ts, lock),
                        // no lock => ok to proceed current txn
                        _ => break,
                    };

                let (primary_key, primary_lock) = match lock {
                    // if primary_lock: get primary_key
                    Value::Primary(lock) => (key.clone(), lock),
                    // else (secondary_lock)
                    Value::Secondary(primary_key) => {
                        let primary_key = primary_key.clone();
                        match store.read(
                            primary_key.clone(),
                            Column::Lock,
                            Some(start_ts),
                            Some(start_ts),
                        ) {
                            // if exist primary_lock
                            Some((_, lock)) => (primary_key, lock.primary_lock()),
                            // if no primary_lock
                            _ => {
                                // check write column if `commit_ts`
                                match store.read(
                                    primary_key.clone(),
                                    Column::Write,
                                    Some(start_ts),
                                    None,
                                ) {
                                    // case.1
                                    Some(((_, commit_ts), _)) => {
                                        let commit_ts = *commit_ts;
                                        // roll-forward for case.1
                                        let _ = store.erase(key.clone(), Column::Lock, start_ts);
                                        store.write(
                                            key.clone(),
                                            Column::Write,
                                            commit_ts,
                                            Value::Timestamp(start_ts),
                                        );
                                        break;
                                    }
                                    // case.2
                                    _ => {
                                        let _ = store.erase(key.clone(), Column::Lock, start_ts);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    _ => panic!("value not lock"),
                };

                // till now primary_lock exist, chekc if expired
                // case.3
                if primary_lock.elapsed() >= TTL {
                    // clear primary_lock
                    let _ = store.erase(primary_key.clone(), Column::Lock, start_ts);
                    // clear primary data
                    let _ = store.erase(primary_key.clone(), Column::Data, start_ts);
                    break;
                }
                // else case.4
            }
            // cond case.4
            Delay::new(INTERVAL).await;
        }
    }
}

// refs:
// https://cloud.tencent.com/developer/article/1880397
// https://cn.pingcap.com/blog/percolator-and-txn/
// https://github.com/Smith-Cruise/TinyKV-White-Paper/blob/main/Project4-Transaction.md
