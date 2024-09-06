use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant},
};

use crate::proto::kvraftpb::Op as PbOp;
use crate::proto::kvraftpb::*;

enum Op {
    #[allow(dead_code)]
    Put(String, String),
    #[allow(dead_code)]
    Append(String, String),
}

pub struct Clerk {
    // `name` MUST be uniq for `Clerk`
    pub name: String,
    pub servers: Vec<(String, KvClient)>,
    // You will have to modify this struct.
    // sequential req id
    seq_id: AtomicU64,
    // track last leader
    leader: AtomicU64,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<(String, KvClient)>) -> Clerk {
        // You'll have to add code here.
        let svrs = servers.iter().map(|(v, _)| v.clone()).collect::<Vec<_>>();
        log::debug!("Clerk {name} servers: {}", svrs.join(","));
        Clerk {
            name,
            servers,
            seq_id: AtomicU64::new(1),
            leader: AtomicU64::new(0),
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { /* do something */ }
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.
        let seq_id = self.seq_id.fetch_add(1, Ordering::SeqCst);
        let args = GetRequest {
            key,
            clerk_name: self.name.clone(),
            clerk_seq: seq_id,
        };

        let mut leader = self.leader.load(Ordering::SeqCst) as usize;
        let start = Instant::now();
        loop {
            log::debug!("{self:?} -> {}: {args:?}", self.servers[leader].0);
            match futures::executor::block_on(self.servers[leader].1.get(&args)) {
                Ok(reply) => {
                    log::debug!("{self:?} recv {reply:?}");
                    if !reply.wrong_leader && reply.err.is_empty() {
                        self.leader.store(leader as u64, Ordering::SeqCst);
                        log::debug!("{self:?} done: {args:?} in {:?}", start.elapsed());
                        return reply.value;
                    }
                }
                Err(e) => {
                    log::warn!("{self:?} {args:?}: {e}");
                }
            }
            leader = (leader + 1) % self.servers.len();
            thread::sleep(Duration::from_millis(50));
        }
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        // You will have to modify this function.
        let args = match op {
            Op::Put(key, value) => {
                let seq_id = self.seq_id.fetch_add(1, Ordering::SeqCst);
                PutAppendRequest {
                    key,
                    value,
                    op: PbOp::Put.into(),
                    clerk_name: self.name.clone(),
                    clerk_seq: seq_id,
                }
            }
            Op::Append(key, value) => {
                let seq_id = self.seq_id.fetch_add(1, Ordering::SeqCst);
                PutAppendRequest {
                    key,
                    value,
                    op: PbOp::Append.into(),
                    clerk_name: self.name.clone(),
                    clerk_seq: seq_id,
                }
            }
        };
        let mut leader = self.leader.load(Ordering::SeqCst) as usize;
        let start = Instant::now();
        loop {
            log::debug!("{self:?} -> {}: {args:?}", self.servers[leader].0);
            match futures::executor::block_on(self.servers[leader].1.put_append(&args)) {
                Ok(reply) => {
                    log::debug!("{self:?} recv {reply:?}");
                    if !reply.wrong_leader && reply.err.is_empty() {
                        self.leader.store(leader as u64, Ordering::SeqCst);
                        log::debug!("{self:?} done: {args:?} in {:?}", start.elapsed());
                        return;
                    }
                }
                Err(e) => {
                    log::warn!("{self:?} {args:?}: {e}");
                }
            }
            leader = (leader + 1) % self.servers.len();
            thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
