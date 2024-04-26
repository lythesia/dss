use std::cmp;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use atomic_enum::atomic_enum;
use educe::Educe;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::channel::oneshot;
use futures::executor::ThreadPool;
use futures::future::{Fuse, FutureExt};
use futures::stream::StreamExt;
use futures_timer::Delay;
use rand::Rng;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[atomic_enum]
#[derive(PartialEq)]
enum RaftRole {
    Killed = 0,
    Candidate,
    Follower,
    Leader,
}

#[derive(Educe)]
#[educe(Debug)]
enum RaftEvent {
    RequestVote(
        RequestVoteArgs,
        #[educe(Debug(ignore))] oneshot::Sender<RequestVoteReply>,
    ),
    ToLeader,
    AppendEntries(
        AppendEntriesArgs,
        #[educe(Debug(ignore))] oneshot::Sender<AppendEntriesReply>,
    ),
    ToFollower(u64),
    Kill,
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    // elect state
    role: Arc<AtomicRaftRole>,
    term: Arc<AtomicU64>,
    voted_for: Option<usize>,
    // timers
    heartbeat_timeout: Duration,
    election_timeout: Duration,
    // event channel (operate in async ctx?)
    event_tx: UnboundedSender<RaftEvent>, // acutually no need to keep this
    event_rx: UnboundedReceiver<RaftEvent>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is `peers[me]`. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. `apply_ch` is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        _apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let (event_tx, event_rx) = unbounded();
        let mut rf = Raft {
            peers,
            persister,
            me,
            role: Arc::new(AtomicRaftRole::new(RaftRole::Follower)),
            term: Arc::new(AtomicU64::new(0)),
            voted_for: None,
            heartbeat_timeout: heartbeat_timeout_ms(),
            election_timeout: election_timeout_ms(),
            event_tx,
            event_rx,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        // crate::your_code_here((rf, apply_ch))
        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> oneshot::Receiver<(usize, Result<RequestVoteReply>)> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let peer_clone = peer.clone();
        // let (tx, rx) = channel();
        // peer.spawn(async move {
        //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
        //     tx.send(res);
        // });
        // rx
        // ```
        // crate::your_code_here((server, args, tx, rx))
        let term = self.term.load(Ordering::SeqCst);
        debug!(
            "raft-{}[{term}] send_request_vote to {server}: {args:?}",
            self.me
        );
        let (tx, rx) = oneshot::channel();
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        peer.spawn(async move {
            let resp = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            if tx.send((server, resp)).is_err() {
                error!("send_request_vote: rx dropped?");
            }
        });
        rx
    }

    fn send_append_entries(
        &self,
        server: usize,
        args: AppendEntriesArgs,
    ) -> oneshot::Receiver<Result<AppendEntriesReply>> {
        let (tx, rx) = oneshot::channel();
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        peer.spawn(async move {
            let resp = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
            if tx.send(resp).is_err() {
                error!("send_append_entries: rx dropped?");
            }
        });
        rx
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        self.snapshot(0, &[]);
        self.persist();
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }

    async fn run(mut self) {
        info!(
            "raft-{} started: election={}ms",
            self.me,
            self.election_timeout.as_millis()
        );
        loop {
            match self.role.load(Ordering::SeqCst) {
                RaftRole::Killed => break,
                RaftRole::Candidate => self.run_cadidate().await,
                RaftRole::Follower => self.run_follower().await,
                RaftRole::Leader => self.run_leader().await,
            }
        }
        info!("raft-{} killed", self.me);
    }

    fn broadcast_heartbeat(&self) {
        let term = self.term.load(Ordering::SeqCst);
        let args = AppendEntriesArgs {
            term,
            leader: self.me as u64,
        };
        log::debug!("raft-{}[{term}] broadcast heartbeat", self.me);
        for svr in 0..self.peers.len() {
            if svr == self.me {
                continue;
            }
            let rx = self.send_append_entries(svr, args.clone());
            let me = self.me;
            let event_tx = self.event_tx.clone();
            self.peers[me].spawn(async move {
                if let Ok(result) = rx.await {
                    match result {
                        Ok(r) => {
                            if !r.success && term < r.term {
                                let _ = event_tx.unbounded_send(RaftEvent::ToFollower(r.term));
                            } else if r.success {
                                // todo: what about r.term?
                                debug!("raft-{me} recv send_append_entries reply: {r:?}");
                            } else {
                                // !r.success && term >= r.term
                                debug!("raft-{me} recv send_append_entries reply: {r:?}");
                            }
                        }
                        Err(e) => {
                            error!("raft-{me} send_append_entries reply error from raft-{svr}: {e}")
                        }
                    }
                }
            });
        }
    }

    // fn on_request_vote(..) {..}

    // todo: not used in election
    // fn leader_on_append_entries(&mut self, args: &AppendEntriesArgs) {}

    fn leader_on_event(&mut self, evt: RaftEvent) -> bool {
        match evt {
            RaftEvent::RequestVote(args, tx) => {
                let (reply, to_follower) = self.on_request_vote(&args);
                let _ = tx.send(reply);
                if to_follower {
                    self.become_follower(args.term);
                    return true;
                }
            }
            RaftEvent::ToFollower(term) => {
                self.become_follower(term);
                return true;
            }
            RaftEvent::Kill => {
                debug!("raft-{} kill signal!", self.me);
                self.role.store(RaftRole::Killed, Ordering::SeqCst);
                return true;
            }
            unknown => {
                warn!("raft-{} unknown event: {unknown:?}", self.me);
            }
        }
        false
    }

    async fn run_leader(&mut self) {
        let mut heartbeat_timer = Delay::new(self.heartbeat_timeout).fuse();
        loop {
            futures::select! {
                _ = heartbeat_timer => {
                    self.broadcast_heartbeat();
                    // reset timer
                    heartbeat_timer = Delay::new(self.heartbeat_timeout).fuse();
                },
                opt = self.event_rx.next() => match opt {
                    Some(evt) => {
                        if self.leader_on_event(evt) {
                            break;
                        }
                    }
                    _ => {
                        error!("raft-{} event channel closed?", self.me);
                        break;
                    }
                }
            }
        }
    }

    // (reply, transfer?)
    fn on_request_vote(&mut self, args: &RequestVoteArgs) -> (RequestVoteReply, bool) {
        let curr_term = self.term.load(Ordering::SeqCst);
        let granted = RequestVoteReply {
            term: args.term,
            vote_granted: true,
        };
        let rejected = RequestVoteReply {
            term: curr_term,
            vote_granted: false,
        };
        if args.term > curr_term {
            debug!("raft-{}[{curr_term}] grant vote: {args:?}", self.me);
            (granted, true)
        } else {
            debug!("raft-{}[{curr_term}] reject vote: {args:?}", self.me);
            (rejected, false)
        }
    }

    // (reply, transfer)
    // todo: deal with real AE later
    fn follower_on_append_entries(
        &mut self,
        args: &AppendEntriesArgs,
    ) -> (AppendEntriesReply, bool) {
        let curr_term = self.term.load(Ordering::SeqCst);
        let accepted = AppendEntriesReply {
            term: args.term,
            success: true,
        };
        let rejected = AppendEntriesReply {
            term: curr_term,
            success: false,
        };
        match args.term.cmp(&curr_term) {
            cmp::Ordering::Greater => (accepted, true),
            cmp::Ordering::Less => (rejected, false),
            _ => (accepted, false), // likely heartbeat?
        }
    }

    fn follower_on_event(&mut self, evt: RaftEvent, timer: &mut Fuse<Delay>) -> bool {
        match evt {
            RaftEvent::RequestVote(args, tx) => {
                let (reply, reenter) = self.on_request_vote(&args);
                let _ = tx.send(reply);
                if reenter {
                    self.become_follower(args.term);
                    return true;
                }
            }
            RaftEvent::AppendEntries(args, tx) => {
                let (reply, reenter) = self.follower_on_append_entries(&args);
                let _ = tx.send(reply);
                if reenter {
                    self.become_follower(args.term);
                    return true;
                } else {
                    // reset timer
                    *timer = Delay::new(self.election_timeout).fuse();
                }
            }
            RaftEvent::Kill => {
                debug!("raft-{} kill signal!", self.me);
                self.role.store(RaftRole::Killed, Ordering::SeqCst);
                return true;
            }
            unknown => {
                warn!("raft-{} unknown event: {unknown:?}", self.me);
            }
        }
        false
    }

    async fn run_follower(&mut self) {
        let mut election_timer = Delay::new(self.election_timeout).fuse();
        loop {
            futures::select! {
                opt = self.event_rx.next() => match opt {
                    Some(evt) => {
                        if self.follower_on_event(evt, &mut election_timer) {
                            break;
                        }
                    },
                    _ => {
                        error!("raft-{} event channel closed?", self.me);
                        break;
                    }
                },
                _ = election_timer => {
                    self.become_candidate();
                    break;
                }
            }
        }
    }

    fn broadcast_request_vote(&self) {
        let term = self.term.load(Ordering::SeqCst);
        let args = RequestVoteArgs {
            term,
            candidate: self.me as u64,
        };

        let me = self.me;
        let tot = self.peers.len();
        let votes = Arc::new(AtomicUsize::new(1)); // self +1
        for svr in 0..self.peers.len() {
            if svr == self.me {
                continue;
            }
            let rx = self.send_request_vote(svr, args.clone());
            let votes = votes.clone();
            let event_tx = self.event_tx.clone();
            self.peers[me].spawn(async move {
                if let Ok((from, result)) = rx.await {
                    match result {
                        Ok(r) => {
                            if r.vote_granted {
                                votes.fetch_add(1, Ordering::SeqCst);
                            }
                            debug!("raft-{me}[{term}] requset_vote reply from raft-{from}: {r:?}, votes = {}/{tot}", votes.load(Ordering::SeqCst));
                        }
                        Err(e) => {
                            error!("raft-{me}[{term}] request_vote reply error from raft-{from}: {e}")
                        }
                    }
                    // once quorum is set, to leader immediately
                    if votes.load(Ordering::SeqCst) > tot / 2 {
                        let _ = event_tx.unbounded_send(RaftEvent::ToLeader);
                    }
                }
            });
        }
    }

    // (reply, transfer?)
    fn candidate_on_request_vote(&mut self, args: &RequestVoteArgs) -> (RequestVoteReply, bool) {
        let curr_term = self.term.load(Ordering::SeqCst);
        let granted = RequestVoteReply {
            term: args.term,
            vote_granted: true,
        };
        let rejected = RequestVoteReply {
            term: curr_term,
            vote_granted: false,
        };
        match args.term.cmp(&curr_term) {
            cmp::Ordering::Less => {
                debug!("raft-{}[{curr_term}] reject vote: {args:?}", self.me);
                (rejected, false)
            }
            cmp::Ordering::Greater => {
                debug!("raft-{}[{curr_term}] grant vote: {args:?}", self.me);
                (granted, true)
            }
            _ => match self.voted_for {
                Some(voted) => {
                    if voted as u64 != args.candidate {
                        debug!(
                            "raft-{}[{curr_term}] reject vote: {args:?}, voted {voted} already",
                            self.me
                        );
                        (rejected, false)
                    } else {
                        debug!(
                            "raft-{}[{curr_term}] grant (repeating?) vote: {args:?}, voted {voted}",
                            self.me
                        );
                        (granted, false)
                    }
                }
                _ => {
                    error!(
                        "raft-{}[{curr_term}] SHOULD have voted self(or other) but not!",
                        self.me
                    );
                    (rejected, false)
                }
            },
        }
    }

    // (reply, to_follower?)
    fn candidate_on_append_entries(
        &mut self,
        args: &AppendEntriesArgs,
    ) -> (AppendEntriesReply, bool) {
        let curr_term = self.term.load(Ordering::SeqCst);
        let accepted = AppendEntriesReply {
            term: args.term,
            success: true,
        };
        let rejected = AppendEntriesReply {
            term: curr_term,
            success: false,
        };
        if args.term < curr_term {
            debug!("raft-{}[{curr_term}] reject ae: {args:?}", self.me);
            (rejected, false)
        } else {
            debug!("raft-{}[{curr_term}] accept ae: {args:?}", self.me,);
            (accepted, true)
        }
    }

    fn candidate_on_event(&mut self, evt: RaftEvent) -> bool {
        match evt {
            RaftEvent::RequestVote(args, tx) => {
                let (reply, to_follower) = self.candidate_on_request_vote(&args);
                let _ = tx.send(reply);
                if to_follower {
                    self.become_follower(args.term);
                    return true;
                } else {
                    // stay
                }
            }
            RaftEvent::ToLeader => {
                self.become_leader();
                return true;
            }
            RaftEvent::AppendEntries(args, tx) => {
                let (reply, to_follower) = self.candidate_on_append_entries(&args);
                let _ = tx.send(reply);
                if to_follower {
                    self.become_follower(args.term);
                    return true;
                } else {
                    // stay
                }
            }
            RaftEvent::Kill => {
                debug!("raft-{} kill signal!", self.me);
                self.role.store(RaftRole::Killed, Ordering::SeqCst);
                return true;
            }
            unknown => {
                warn!("raft-{} unknown event: {unknown:?}", self.me);
            }
        }
        false
    }

    async fn run_cadidate(&mut self) {
        let mut election_timer = Delay::new(self.election_timeout).fuse();
        self.broadcast_request_vote();
        loop {
            futures::select! {
                _ = election_timer => {
                    // issue new election
                    self.become_candidate();
                    break;
                }
                opt = self.event_rx.next() => match opt {
                    Some(evt) => {
                        if self.candidate_on_event(evt) {
                            break;
                        }
                    },
                    _ => {
                        error!("raft-{} event channel closed?", self.me);
                        break;
                    }
                }
            }
        }
    }

    fn become_candidate(&mut self) {
        let prev_term = self.term.fetch_add(1, Ordering::SeqCst);
        self.role.store(RaftRole::Candidate, Ordering::SeqCst);
        self.voted_for = Some(self.me);
        info!(
            "raft-{}[{prev_term}] => candiate[{}]",
            self.me,
            self.term.load(Ordering::SeqCst)
        );
    }

    fn become_follower(&mut self, term: u64) {
        let prev_term = self.term.swap(term, Ordering::SeqCst);
        self.role.store(RaftRole::Follower, Ordering::SeqCst);
        self.voted_for = None;
        info!("raft-{}[{prev_term}] => follower[{term}]", self.me);
    }

    fn become_leader(&mut self) {
        let term = self.term.load(Ordering::SeqCst);
        self.role.store(RaftRole::Leader, Ordering::SeqCst);
        self.voted_for = None;
        info!("raft-{}[{term}] => leader[{term}]", self.me);
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    // shared state
    role: Arc<AtomicRaftRole>,
    term: Arc<AtomicU64>,
    // event chan
    event_tx: UnboundedSender<RaftEvent>,
    // executor
    #[allow(unused)]
    pool: ThreadPool,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        // crate::your_code_here(raft)
        // Node::new will acutally run raft instance
        let role = raft.role.clone();
        let term = raft.term.clone();
        let event_tx = raft.event_tx.clone();

        let pool = ThreadPool::new().unwrap();
        pool.spawn_ok(raft.run());

        Self {
            role,
            term,
            event_tx,
            pool,
        }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        self.term.load(Ordering::SeqCst)
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        self.role.load(Ordering::SeqCst) == RaftRole::Leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        if let Err(e) = self.event_tx.unbounded_send(RaftEvent::Kill) {
            error!("kill failed: {e}");
        }
    }

    /// A service wants to switch to snapshot.
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        // crate::your_code_here(args)
        let (tx, rx) = oneshot::channel();
        let evt = RaftEvent::RequestVote(args, tx);
        let _ = self.event_tx.unbounded_send(evt);
        rx.await.map_err(labrpc::Error::Recv)
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let (tx, rx) = oneshot::channel();
        let evt = RaftEvent::AppendEntries(args, tx);
        let _ = self.event_tx.unbounded_send(evt);
        rx.await.map_err(labrpc::Error::Recv)
    }
}

// utils
const HEARTBEAT_TIMEOUT: u64 = 100;
fn heartbeat_timeout_ms() -> Duration {
    Duration::from_millis(HEARTBEAT_TIMEOUT)
}

const ELECTION_TIMEOUT_LO: u64 = 500; // at least > 3x heartbeat approx.
const ELECTION_TIMEOUT_HI: u64 = 1000; // gap between each timer > RTT of raft rpc, so make upperbound large enough
const ELECTION_TIMEOUT_GAP: u64 = 20; // assume gap = 20ms is enough for RTT

fn election_timeout_ms() -> Duration {
    // stepify with ELECTION_TIMEOUT_GAP
    let ms = rand::thread_rng().gen_range(
        ELECTION_TIMEOUT_LO / ELECTION_TIMEOUT_GAP,
        ELECTION_TIMEOUT_HI / ELECTION_TIMEOUT_GAP,
    );
    Duration::from_millis(ms * ELECTION_TIMEOUT_GAP)
}

// election refs:
// https://stackoverflow.com/questions/77381650/leader-election-and-appendentries-rejection
// https://stackoverflow.com/questions/57868272/should-raft-follower-update-term-on-receiving-vote-request-with-higher-term
