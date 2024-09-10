use std::collections::HashMap;
use std::iter::FromIterator;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use educe::Educe;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::channel::oneshot;
use futures::executor::ThreadPool;
use futures::future::FutureExt;
use futures::stream::StreamExt;

use crate::proto::kvraftpb::*;
use crate::raft::{self, ApplyMsg, AtomicRaftRole, RaftRole};

pub struct KvServer {
    pub rf: raft::Node,
    // Your definitions here.
    // rx of apply_ch
    apply_ch: UnboundedReceiver<ApplyMsg>,
    // channel with `Node`
    event_tx: UnboundedSender<KvEvent>, // only for passing to `Node`, coz `new()` does not return tx
    event_rx: UnboundedReceiver<KvEvent>,
    // shared meta
    meta: KvMeta,
    // underlay store
    kv: Arc<ArcSwap<DashMap<String, String>>>,
    // shared for tasks
    state: Arc<SharedState>,
}

#[derive(Clone)]
struct KvMeta {
    me: usize,
    role: Arc<AtomicRaftRole>,
    term: Arc<AtomicU64>,
}

impl std::fmt::Display for KvMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "KVS-{}[{}][{}]",
            self.me,
            self.role.load(Ordering::SeqCst),
            self.term.load(Ordering::SeqCst)
        )
    }
}

struct SharedState {
    // idempotent purpose
    clerks: ArcSwap<DashMap<String, u64>>, // clerk_name -> seq
    // keep track of pending log index, index -> reply_chan_to_clerk
    index_ch: DashMap<u64, oneshot::Sender<u64>>, // u64 == term of command
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // handle to rf core
    rf: raft::Node,
}

#[derive(Educe)]
#[educe(Debug)]
enum KvEvent {
    Get(
        GetRequest,
        #[educe(Debug(ignore))] oneshot::Sender<labrpc::Result<GetReply>>,
    ),
    PutAppend(
        PutAppendRequest,
        #[educe(Debug(ignore))] oneshot::Sender<labrpc::Result<PutAppendReply>>,
    ),
    Kill,
}

// 如何确定从apply_ch拿到的commit msg该转发给哪个clerk?
// 或者说, 如果用map{index => clerk_chan}来track一个clerk发起的一次调用
// 是否有可能index被重复使用? 即下次map.insert发现index已经有了
// 可能, 比如server落后很久, 然后遇到install snapshot, 此时index会reset
// 两种处理:
// 1. 处理snapshot的时候就把map的stale index全部clear
// 2. 1不处理, 直到发现index冲突了, 给index->chan发送reply后clear
// 感觉1更合适, 可以保证无index冲突
// 潜在问题?
// A. index reset发生在raft层, 而kvserver需要通过apply_ch拿, 中间有gap,
// 有可能kvserver在clear之前就出现冲突, 因为此时没来得及从apply_ch获取snapshot,
// 而client已经发送命令并触发了raft.start(), 此时被冲突的entry一定不会被commit,
// 应当返回err reply
// 所以, 1和2可以都做 == defensive programming
// B. apply_ch拿到的KvCommand有可能和map里的clerk的name/seq不匹配?
// 已知:
// 1. map里一定是通过raft.start()得到index后insert的entry
// 2. apply_ch一定是有序的
// 那么假设cmd1已经通过raft.start, 进入map, 但在apply_ch还没拿到他的commit rec,
// 此时cmd2到来, 且raft层发生了snapshot和index reset, 那么在apply_ch中现在应该是:
// [cmd1, snapshot] (以及后续会进入的cmd2), 那么此时cmd1的chan在cmd2调用raft.start()
// 以后一定会被处理(因为它将被drop), 后续按序处理snapshot和cmd2, 且cmd2一定被正确reply
// 所以无需检查clerk的name/seq
// 当然可以做双重保险, 实际上command里的clerk_name/seq只是在raft.start()时用于track对应的
// channel, 从apply_ch里获得虽然是原样的command, 但其实不care了

// 另外一种完全不同处理:
// kvserver处理请求的时候一定会等apply_ch的commit info(不是死等, 会有timeout), 如果没收到,
// 则返回error, 且clear这个index的chan, 这里采用了这种方法

// 另外更正上面的一个错误: kvserver从apply_ch拿到的commit不一定要转发(因为可能没有clerk连接到自己),
// 但是相应的clerk->seq要进行update, 以及自己的kvstore也要update, 这是为了之后clerk连接过来的请求
// 可以直接处理: 如果是重复的PutAppend, 直接返回Ok; 如果是Get(且seq<=clerk->seq), 直接返回kv.get()

enum ExecErr {
    Raft(raft::errors::Error),
    Custom(String),
}

#[derive(Message)]
struct Snapshot {
    #[prost(map = "string,string", tag = "1")]
    pub kv: HashMap<String, String>,
    #[prost(map = "string,uint64", tag = "2")]
    pub clerks: HashMap<String, u64>,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.

        let (tx, apply_ch) = unbounded();
        let snapshot = persister.snapshot();
        let rf = raft::Raft::new(servers, me, persister, tx);
        let rf = raft::Node::new(rf); // this runs raft instance on its own thread pool
        let (event_tx, event_rx) = unbounded();
        let role = rf.role.clone();
        let term = rf.term.clone();
        let kv = Arc::new(ArcSwap::from_pointee(DashMap::new()));
        let state = Arc::new(SharedState {
            clerks: ArcSwap::from_pointee(DashMap::new()),
            index_ch: Default::default(),
            maxraftstate,
            rf: rf.clone(),
        });

        let me = KvMeta { me, role, term };
        Self::load_snapshot(&me, &state, &kv, &snapshot);

        Self {
            rf,
            apply_ch,
            event_tx,
            event_rx,
            meta: me,
            kv,
            state,
        }
    }

    async fn run(self) {
        let Self {
            rf,
            mut apply_ch,
            event_tx,
            mut event_rx,
            meta,
            kv,
            state,
        } = self;
        log::debug!("{meta} start loop");

        let me = meta.clone();
        let state2 = state.clone();
        let kv2 = kv.clone();
        let event_task = async move {
            while let Some(evt) = event_rx.next().await {
                match evt {
                    KvEvent::Get(args, tx) => Self::on_get(&me, &rf, &state2, &kv2, args, tx).await,
                    KvEvent::PutAppend(args, tx) => {
                        Self::on_put_append(&me, &rf, &state2, args, tx).await
                    }
                    KvEvent::Kill => {
                        log::debug!("{me} kill signal!");
                        rf.kill();
                        break;
                    }
                }
            }
        };

        let me = meta.clone();
        let apply_ch_task = async move {
            while let Some(msg) = apply_ch.next().await {
                Self::on_apply_msg(&me, &state, &kv, msg).await
            }
            let _ = event_tx.unbounded_send(KvEvent::Kill);
        };

        futures::join!(event_task, apply_ch_task);

        log::debug!("{meta} exit loop");
    }

    async fn on_apply_msg(
        me: &KvMeta,
        state: &Arc<SharedState>,
        kv: &Arc<ArcSwap<DashMap<String, String>>>,
        msg: ApplyMsg,
    ) {
        let notify = |term: u64, index: u64| {
            if let Some((_, sender)) = state.index_ch.remove(&index) {
                log::debug!("{me} noti applied: term={term}, index={index}");
                if sender.send(term).is_err() {
                    log::error!("{me} index_ch[{index}] rx dropped?");
                }
            }
        };

        match msg {
            ApplyMsg::Command { data, term, index } => {
                let cmd = match labcodec::decode::<KvCommand>(&data) {
                    Ok(v) => v,
                    Err(_) => {
                        log::error!("{me} cannot decode ApplyMsg");
                        return;
                    }
                };
                log::debug!("{me} recv ApplyMsg: term={term}, index={index}, cmd={cmd:?}");

                let clerks = state.clerks.load();
                let applied_seq = match clerks.get(&cmd.clerk_name) {
                    Some(v) => *v,
                    // followers apply logs too, they likely have no connected clerks
                    // but need to track them, so they can respond on future commands
                    _ => 0,
                };
                let KvCommand {
                    key,
                    value,
                    op,
                    clerk_name,
                    clerk_seq,
                } = cmd;

                // seq already seen
                if applied_seq >= clerk_seq {
                    notify(term, index);
                    return;
                }

                // apply to kv
                let kv = kv.load();
                let op_log = match KvOp::from_i32(op) {
                    Some(KvOp::KvGet) => format!("get {key}"),
                    Some(KvOp::KvPut) => {
                        let l = format!("put {key}={value}");
                        log::debug!("{me} +kv: \"{key}\"=\"{value}\"");
                        kv.insert(key, value);
                        l
                    }
                    Some(KvOp::KvAppend) => {
                        let l = format!("append {key}+={value}");
                        kv.entry(key.clone())
                            .and_modify(|val| {
                                *val += &value;
                                log::debug!("{me} *kv: \"{key}\"=\"{}\"", *val);
                            })
                            .or_insert_with(|| {
                                log::debug!("{me} +kv: \"{key}\"=\"{value}\"");
                                value
                            });
                        l
                    }
                    None => Default::default(),
                };
                // 注意这里update为最新seq, 否则可能重复apply
                let prev_seq = clerks.insert(clerk_name.clone(), clerk_seq);
                log::debug!(
                    "{me} update clerk[{clerk_name}].seq = {clerk_seq}(op: {op_log})(prev_seq={prev_seq:?}"
                );
                notify(term, index);

                // try save snapshot
                Self::try_save_snapshot(me, state, &kv, index);
            }
            ApplyMsg::Snapshot { data, term, index } => {
                log::debug!(
                    "{me} recv Snapshot: term={term}, index={index}, size={}",
                    data.len()
                );
                let ok = state.rf.cond_install_snapshot(term, index, &data);
                if ok {
                    Self::load_snapshot(me, state, kv, &data);
                }
            }
        }
    }

    fn try_save_snapshot(
        me: &KvMeta,
        state: &Arc<SharedState>,
        kv: &Arc<DashMap<String, String>>,
        index: u64,
    ) {
        let state_size = match state.rf.state_size() {
            Ok(v) => v,
            Err(_) => {
                log::error!("{me} cannot get raft_state_size");
                return;
            }
        };
        if !matches!(state.maxraftstate, Some(max_size) if max_size <= state_size ) {
            return;
        }

        let snapshot = Snapshot {
            kv: HashMap::from_iter(DashMap::clone(kv)),
            clerks: HashMap::from_iter(DashMap::clone(&state.clerks.load())),
        };
        let mut buf = Vec::new();
        labcodec::encode(&snapshot, &mut buf).unwrap();
        log::debug!("{me} save_snapshot to index={index}, size={}", buf.len());
        state.rf.snapshot(index, &buf); // tell raft kvs has created snapshot
    }

    fn load_snapshot(
        me: &KvMeta,
        state: &Arc<SharedState>,
        kv: &Arc<ArcSwap<DashMap<String, String>>>,
        data: &[u8],
    ) {
        if data.is_empty() {
            log::warn!("{me} empty snapshot? clear all");
            state.clerks.load().clear();
            kv.load().clear();
            return;
        }

        log::debug!("{me} load_snapshot size={}", data.len());
        let snapshot = labcodec::decode::<Snapshot>(data).unwrap();
        let clerks_new = DashMap::from_iter(snapshot.clerks);
        state.clerks.store(Arc::new(clerks_new));
        let kv_new = DashMap::from_iter(snapshot.kv);
        kv.store(Arc::new(kv_new));
    }

    async fn on_put_append(
        me: &KvMeta,
        rf: &raft::Node,
        state: &Arc<SharedState>,
        args: PutAppendRequest,
        tx: oneshot::Sender<labrpc::Result<PutAppendReply>>,
    ) {
        log::debug!("{me} recv {args:?}");
        let PutAppendRequest {
            key,
            value,
            op,
            clerk_name,
            clerk_seq,
        } = args;

        let cmd = KvCommand {
            key,
            value,
            op,
            clerk_name,
            clerk_seq,
        };

        let reply = match Self::exec_command(me, rf, state, cmd).await {
            Ok(_) => Ok(Default::default()),
            Err(e) => match e {
                ExecErr::Raft(e) => err_to_put_reply(e),
                ExecErr::Custom(err) => Ok(PutAppendReply {
                    wrong_leader: false,
                    err,
                }),
            },
        };

        if let Err(o) = tx.send(reply) {
            log::debug!("{me} {o:?} dropped due to client timeout")
        }
    }

    async fn on_get(
        me: &KvMeta,
        rf: &raft::Node,
        state: &Arc<SharedState>,
        kv: &Arc<ArcSwap<DashMap<String, String>>>,
        args: GetRequest,
        tx: oneshot::Sender<labrpc::Result<GetReply>>,
    ) {
        log::debug!("{me} recv {args:?}");
        let GetRequest {
            key,
            clerk_name,
            clerk_seq,
        } = args;

        let cmd = KvCommand {
            key: key.clone(),
            value: Default::default(),
            op: KvOp::KvGet.into(),
            clerk_name,
            clerk_seq,
        };

        let reply = match Self::exec_command(me, rf, state, cmd).await {
            Ok(_) => {
                let value = match kv.load().get(&key) {
                    Some(e) => e.value().clone(),
                    _ => Default::default(),
                };
                Ok(GetReply {
                    wrong_leader: false,
                    err: Default::default(),
                    value,
                })
            }
            Err(e) => match e {
                ExecErr::Raft(e) => err_to_get_reply(e),
                ExecErr::Custom(err) => Ok(GetReply {
                    wrong_leader: false,
                    err,
                    value: Default::default(),
                }),
            },
        };

        if let Err(o) = tx.send(reply) {
            log::debug!("{me} {o:?} dropped due to client timeout")
        }
    }

    async fn exec_command(
        me: &KvMeta,
        rf: &raft::Node,
        state: &Arc<SharedState>,
        cmd: KvCommand,
    ) -> Result<(), ExecErr> {
        let seq = *state
            .clerks
            .load()
            .entry(cmd.clerk_name.clone())
            .or_insert(0);
        // already processed
        if seq >= cmd.clerk_seq {
            log::debug!("{me} clerk[{}].seq={seq} already applied", cmd.clerk_name);
            return Ok(()); // we MUST return ok here, coz `seq` has been applied
        }

        let (index, term) = match rf.start(&cmd) {
            Ok(v) => v,
            Err(e) => return Err(ExecErr::Raft(e)),
        };
        log::debug!("{me} waiting apply log_index = {index}");

        // await apply_ch notify, then forward to `tx`?
        let (op_tx, op_rx) = oneshot::channel::<u64>();

        assert!(!state.index_ch.contains_key(&index));
        state.index_ch.insert(index, op_tx);

        // 注意: 不能在这里死等`op_rx`的结果, 因为:
        // op_rx是等apply_ch, 也就是command commit的结果, 而当figure.8的case发生时, old term的command
        // 不会随AE而apply, 需要等到在**当前**term的新command触发AE以后, 一起commit, 而新command通过`event_rx`
        // 获取, 此时old command仍在`event_rx`的处理block中(死等apply), 形成了死锁
        // 简单来说: event_rx的旧command等apply -> apply等新command触发 -> 新command要从event_rx读, A->B->A
        let mut op_rx = op_rx.fuse();
        let mut delay = futures_timer::Delay::new(Duration::from_millis(1000)).fuse();

        let ret = futures::select! {
            result = op_rx => {
                result
                .map_err(|_| {
                    log::error!("{me} index_ch[{index}].tx dropped, what happened?");
                    ExecErr::Custom("unknown error".into())
                })
                .and_then(|op_term| {
                    // leader changed
                    log::debug!("{me} got applied term={op_term}, index={index}");
                    if op_term != term {
                        return Err(ExecErr::Raft(raft::errors::Error::NotLeader));
                    }
                    Ok(())
                })
            }
            _ = delay => {
                let e: Result<(), _> = Err(ExecErr::Raft(raft::errors::Error::Rpc(labrpc::Error::Timeout)));
                e
            }
        };
        state.index_ch.remove(&index);
        ret
    }
}

impl KvServer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {}
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your definitions here.
    // channel with `KvServer`
    event_tx: UnboundedSender<KvEvent>,
    // passing from `KvServer`
    role: Arc<AtomicRaftRole>,
    term: Arc<AtomicU64>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        // Your code here.
        let node = Self {
            event_tx: kv.event_tx.clone(),
            role: kv.meta.role.clone(),
            term: kv.meta.term.clone(),
        };
        // drives `KvServer` here
        let pool = ThreadPool::new().unwrap();
        pool.spawn_ok(async move { kv.run().await });
        node
    }

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call kvraft::Node::kill only.
        // self.server.kill();

        // Your code here, if desired.
        let _ = self.event_tx.unbounded_send(KvEvent::Kill);
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        raft::State {
            term: self.term.load(Ordering::SeqCst),
            is_leader: self.role.load(Ordering::SeqCst) == RaftRole::Leader,
        }
    }
}

#[async_trait::async_trait]
impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn get(&self, arg: GetRequest) -> labrpc::Result<GetReply> {
        // Your code here.
        let (tx, rx) = oneshot::channel();
        let evt = KvEvent::Get(arg, tx);
        let _ = self.event_tx.unbounded_send(evt);
        match rx.await {
            Ok(v) => v,
            Err(e) => Err(labrpc::Error::Recv(e)),
        }
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn put_append(&self, arg: PutAppendRequest) -> labrpc::Result<PutAppendReply> {
        // Your code here.
        let (tx, rx) = oneshot::channel();
        let evt = KvEvent::PutAppend(arg, tx);
        let _ = self.event_tx.unbounded_send(evt);
        match rx.await {
            Ok(v) => v,
            Err(e) => Err(labrpc::Error::Recv(e)),
        }
    }
}

fn err_to_put_reply(e: raft::errors::Error) -> labrpc::Result<PutAppendReply> {
    let (wrong_leader, err) = err_to_desc(e)?;
    Ok(PutAppendReply { wrong_leader, err })
}

fn err_to_get_reply(e: raft::errors::Error) -> labrpc::Result<GetReply> {
    let (wrong_leader, err) = err_to_desc(e)?;
    Ok(GetReply {
        wrong_leader,
        err,
        value: Default::default(),
    })
}

/// (wrong_leader?, err_msg)
fn err_to_desc(e: raft::errors::Error) -> labrpc::Result<(bool, String)> {
    match e {
        raft::errors::Error::Encode(e) => Ok((false, e.to_string())),
        raft::errors::Error::Decode(e) => Ok((false, e.to_string())),
        raft::errors::Error::Rpc(e) => Err(e),
        raft::errors::Error::NotLeader => Ok((true, "not leader".into())),
    }
}

// kvs refs:
// https://github.com/springfieldking/mit-6.824-golabs-2018
// https://github.com/makisevon/dss
