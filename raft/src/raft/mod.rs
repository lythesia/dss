use std::cmp;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use atomic_enum::atomic_enum;
use educe::Educe;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::channel::oneshot;
use futures::executor::ThreadPool;
use futures::future::{Fuse, FutureExt};
use futures::sink::SinkExt;
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

impl std::fmt::Display for RaftRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let r = match self {
            RaftRole::Killed => "KIA",
            RaftRole::Candidate => "CAN",
            RaftRole::Follower => "FLR",
            RaftRole::Leader => "LDR",
        };
        write!(f, "{}", r)
    }
}

#[derive(Educe)]
#[educe(Debug)]
enum RaftEvent {
    RequestVote(
        RequestVoteArgs,
        #[educe(Debug(ignore))] oneshot::Sender<RequestVoteReply>,
    ),
    RequestVoteReply {
        svr: usize,
        result: Result<RequestVoteReply>,
    },
    AppendEntries(
        AppendEntriesArgs,
        #[educe(Debug(ignore))] oneshot::Sender<AppendEntriesReply>,
    ),
    AppendEntriesReply {
        svr: usize,
        result: Result<AppendEntriesReply>,
        last_index_on_success: usize,
    },
    StartCommand(
        #[educe(Debug(ignore))] Vec<u8>,
        #[educe(Debug(ignore))] mpsc::SyncSender<Result<(u64, u64)>>, // use sync channel for external use
    ),
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
    // todo: refactoring, here Arc + Atomic is only for exposed info (to test/caller)
    role: Arc<AtomicRaftRole>,
    term: Arc<AtomicU64>,
    voted_for: Option<usize>,
    // timers
    heartbeat_timeout: Duration,
    // NOTE: we MUST NOT use fixed election_timeout here, even it's randomized
    // consider a 3 server case:
    // 1. 0(leader) 1 2 on term 1, all have logs commit to index 5
    // 2. 1 disconnected
    // 3. 0 2, continue advancing serveral logs, to index 5+x
    // 4. since 1 is isolated, it may trigger multiple rounds of election, we make it term 2 for simplicity
    // 5. 1 connected again
    // 6. 1 send RV to all, which make 0 2 to follower, but 0 2 will reject 1 coz its log not up-to-date
    // 7. but 1 has smallest election_timeout and it's FIXED!
    // 8. 1 keep triggering election, but definitely to fail, while 0 2 just increasing their terms and rejecting
    // 9. cluster will never pick out a leader
    // fix is to use newly randomized timeout each time when someone triggers election
    // election_timeout: Duration, // IT'S WRONG!
    // event channel (operate in async ctx?)
    event_tx: UnboundedSender<RaftEvent>, // acutually no need to keep this
    event_rx: UnboundedReceiver<RaftEvent>,

    // logs
    log: Vec<LogEntry>,
    next_index: Vec<usize>,
    match_index: Vec<usize>,
    commit_index: usize,
    last_applied: usize,
    apply_ch: UnboundedSender<ApplyMsg>,
}

impl std::fmt::Display for Raft {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RAFT-{}[{}][{}]",
            self.me,
            self.role.load(Ordering::SeqCst),
            self.term.load(Ordering::SeqCst),
        )
    }
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
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let (event_tx, event_rx) = unbounded();
        let peers_len = peers.len();
        let mut rf = Raft {
            peers,
            persister,
            me,
            role: Arc::new(AtomicRaftRole::new(RaftRole::Follower)),
            term: Arc::new(AtomicU64::new(0)),
            voted_for: None,
            heartbeat_timeout: heartbeat_timeout_ms(),
            event_tx,
            event_rx,
            log: vec![
                // initially dummy entry
                LogEntry {
                    term: 0,
                    index: 0,
                    command: vec![],
                },
            ],
            next_index: vec![0; peers_len],
            match_index: vec![0; peers_len],
            commit_index: 0,
            last_applied: 0,
            apply_ch,
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
    ) -> oneshot::Receiver<Result<RequestVoteReply>> {
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
        let (tx, rx) = oneshot::channel();
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        peer.spawn(async move {
            let resp = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            if tx.send(resp).is_err() {
                error!("send RV: rx dropped?");
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
                error!("send AE: rx dropped?");
            }
        });
        rx
    }

    fn start(&mut self, command: Vec<u8>) -> Result<(u64, u64)> {
        // Your code here (2B).
        let is_leader = self.role.load(Ordering::SeqCst) == RaftRole::Leader;
        if !is_leader {
            return Err(Error::NotLeader);
        }

        // first is dummy(0), so new_entry.index == len(log_before_appended)
        let index = self.log.len();
        let term = self.term.load(Ordering::SeqCst);
        self.log.push(LogEntry {
            index: index as u64,
            term,
            command,
        });
        self.next_index[self.me] = index + 1;
        self.match_index[self.me] = index;
        Ok((index as u64, term))
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
        let _ = self.cond_install_snapshot(0, 0, &[]);
        self.snapshot(0, &[]);
        self.persist();
        let _ = &self.persister;
    }

    async fn run(mut self) {
        info!("{self} started");
        loop {
            match self.role.load(Ordering::SeqCst) {
                RaftRole::Killed => break,
                RaftRole::Candidate => self.run_cadidate().await,
                RaftRole::Follower => self.run_follower().await,
                RaftRole::Leader => self.run_leader().await,
            }
        }
        info!("{self} killed");
    }

    fn broadcast_append_entries(&self) {
        let term = self.term.load(Ordering::SeqCst);
        let me = self.me;
        for svr in 0..self.peers.len() {
            if svr == self.me {
                continue;
            }
            // 1. prev_log
            let (prev_log_index, prev_log_term) = self.prev_log_of(svr);
            let mut args = AppendEntriesArgs {
                term,
                leader: self.me as u64,
                entries: vec![], // heartbeat if empty
                leader_commit: self.commit_index as u64,
                prev_log_index,
                prev_log_term,
            };
            // 2. fill entries: log[next_index[svr] .. ]
            let next_index = self.next_index[svr];
            if self.last_log().index >= next_index as u64 {
                let slice = &self.log[next_index..];
                debug!(
                    "{self} send AE to RAFT-{svr}: leader_commit={}, logs[{:?}]",
                    self.commit_index,
                    next_index..(next_index + slice.len())
                );
                args.entries.extend_from_slice(slice);
            } else {
                debug!(
                    "{self} send AE(heartbeat) to RAFT-{svr}: leader_commit={}",
                    self.commit_index
                );
            }
            // 3. send rpc
            let last_index_on_success = args.prev_log_index as usize + args.entries.len();
            let rx = self.send_append_entries(svr, args);
            let event_tx = self.event_tx.clone();
            self.peers[me].spawn(async move {
                if let Ok(result) = rx.await {
                    let _ = event_tx.unbounded_send(RaftEvent::AppendEntriesReply {
                        svr,
                        result,
                        last_index_on_success,
                    });
                }
            });
        }
    }

    // -> transfer?
    fn on_append_entries_reply(
        &mut self,
        svr: usize,
        result: Result<AppendEntriesReply>,
        last_index: usize,
    ) -> bool {
        let r = match result {
            Ok(v) => v,
            Err(e) => {
                error!("{self} recv AE reply error from RAFT-{svr}: {e}");
                return false;
            }
        };

        debug!("{self} recv AE reply: {r:?}");
        let term = self.term.load(Ordering::SeqCst);
        let role = self.role.load(Ordering::SeqCst);
        // term fall behind
        if term < r.term {
            self.become_follower(r.term, None);
            return true;
        }
        // case: term >= r.term
        // not leader OR term not match
        if role != RaftRole::Leader || term != r.term {
            debug!("{self} ignore AE reply");
            return false;
        }
        // case: is_leader && term
        if r.success {
            let match_index = last_index;
            let next_index = last_index + 1;
            self.match_index[svr] = match_index;
            self.next_index[svr] = next_index;
            debug!(
                "{self} update(on match) next_index[{svr}]={next_index}, match_index[{svr}]={match_index}"
            );
            self.leader_try_commit_and_apply();
        } else {
            // follower has `prev_log_index` but `term` != `prev_log_term` =>
            // leader find the entry immediately beyond last of `term`: `term .. term term+1(this one)`
            // if `term not exist` => next_index = conflict_log_index
            if r.conflict_log_term > 0 {
                let next_index = match self.log.iter().position(|e| e.term == r.conflict_log_term) {
                    // term exist
                    Some(i) => match self.log[i..].iter().find(|e| e.term != r.conflict_log_term) {
                        Some(e) => e.index as usize,
                        _ => self.log.len(),
                    },
                    // term not exist
                    _ => r.conflict_log_index as usize,
                };
                if next_index > 0 {
                    debug!(
                        "{self} update(on term mismatch) next_index[{svr}]={next_index}, will retry AE"
                    );
                    self.next_index[svr] = next_index; // will try AE next round
                }
            } else if r.conflict_log_index > 0 {
                debug!(
                    "{self} update(on index mismatch) next_index[{svr}]={}, will retry AE",
                    r.conflict_log_index
                );
                self.next_index[svr] = r.conflict_log_index as usize; // will try AE next round
            }
        }
        false
    }

    fn leader_try_commit_and_apply(&mut self) {
        let term = self.term.load(Ordering::SeqCst);
        let majority_index = get_majority_same_index(self.match_index.clone());
        // if term is legal
        if self.log[majority_index].term == term && majority_index > self.commit_index {
            self.try_commit_to_and_apply(majority_index);
        }
    }

    fn leader_on_event(&mut self, evt: RaftEvent) -> bool {
        match evt {
            RaftEvent::RequestVote(args, tx) => {
                let (reply, to_follower) = self.on_request_vote(&args);
                if to_follower {
                    self.become_follower(args.term, Some(args.candidate as usize));
                    // resend RV
                    let _ = self
                        .event_tx
                        .unbounded_send(RaftEvent::RequestVote(args, tx));
                    return true;
                } else {
                    // stay
                    let _ = tx.send(reply);
                }
            }
            RaftEvent::AppendEntriesReply {
                svr,
                result,
                last_index_on_success,
            } => {
                if self.on_append_entries_reply(svr, result, last_index_on_success) {
                    return true;
                }
            }
            RaftEvent::StartCommand(buf, tx) => {
                let reply = self.start(buf);
                let _ = tx.send(reply);
            }
            RaftEvent::Kill => {
                debug!("{self} kill signal!");
                self.role.store(RaftRole::Killed, Ordering::SeqCst);
                return true;
            }
            unknown => {
                warn!("{self} unknown event: {unknown:?}");
            }
        }
        false
    }

    async fn run_leader(&mut self) {
        // push no-op log and broadcast immediately when enter leader,
        // to avoid https://www.cnblogs.com/xybaby/p/10124083.html#_label_10
        // but test framework don't have no-op command, and it'll break `n_committed` check even if it has
        // self.start(command);
        self.broadcast_append_entries();
        let mut heartbeat_timer = Delay::new(self.heartbeat_timeout).fuse();
        loop {
            futures::select! {
                _ = heartbeat_timer => {
                    self.broadcast_append_entries();
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
                        error!("{self} event channel closed?");
                        break;
                    }
                }
            }
        }
    }

    fn is_up_to_date(&self, last_log_term: u64, last_log_index: u64) -> bool {
        let last = self.last_log();
        last_log_term > last.term || (last_log_term == last.term && last_log_index >= last.index)
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
        match args.term.cmp(&curr_term) {
            cmp::Ordering::Less => {
                debug!("{self} reject vote(lower term): {args:?}");
                (rejected, false)
            }
            cmp::Ordering::Greater => {
                debug!("{self} will grant vote after become follower: {args:?}");
                (granted, true) // we don't reply `granted` immeidately, so it's not used here
            }
            cmp::Ordering::Equal => {
                if self.voted_for.is_none() || self.voted_for == Some(args.candidate as usize) {
                    if self.is_up_to_date(args.last_log_term, args.last_log_index) {
                        debug!("{self} grant vote (more up-to-date): {args:?}");
                        self.voted_for = Some(args.candidate as usize);
                        (granted, false)
                    } else {
                        debug!("{self} reject vote (less up-to-date): {args:?}");
                        self.voted_for = None;
                        (rejected, false)
                    }
                } else {
                    debug!(
                        "{self} reject vote (already vote to {:?}): {args:?}",
                        self.voted_for
                    );
                    (rejected, false)
                }
            }
        }
    }

    // (reply, transfer)
    fn follower_on_append_entries(
        &mut self,
        args: &AppendEntriesArgs,
    ) -> (AppendEntriesReply, bool) {
        let curr_term = self.term.load(Ordering::SeqCst);
        let mut judging = AppendEntriesReply {
            term: args.term,
            success: true,
            conflict_log_index: 0,
            conflict_log_term: 0,
        };
        let rejected = AppendEntriesReply {
            term: curr_term,
            success: false,
            conflict_log_index: 0,
            conflict_log_term: 0,
        };
        match args.term.cmp(&curr_term) {
            cmp::Ordering::Greater => {
                self.voted_for = None; // admit this leader, todo: do we need to remember leader_id?
                let _not_used = judging;
                (_not_used, true) // reenter with higher term (actually goto branch `Ordering::Equal`)
            }
            cmp::Ordering::Less => (rejected, false), // reject by term
            _ => {
                match self.log.get(args.prev_log_index as usize) {
                    // 1. if log has no `prev_log_index`
                    None => {
                        judging.success = false;
                        judging.conflict_log_term = 0;
                        judging.conflict_log_index = self.log.len() as u64;
                    }
                    Some(e) => {
                        // 2. log has `prev_log_index` but not match `prev_log_term`
                        // find 1st log of `log[prev_log_index].term`
                        if e.term != args.prev_log_term {
                            match self.log.iter().find(|fst| fst.term == e.term) {
                                Some(fst) => {
                                    judging.success = false;
                                    judging.conflict_log_term = fst.term;
                                    judging.conflict_log_index = fst.index;
                                }
                                _ => {
                                    // impossible, at least `e` is the 1st log
                                    warn!("{self} impossible: no log with term {}?", e.term);
                                    judging.success = false;
                                    judging.conflict_log_term = e.term;
                                    judging.conflict_log_index = e.index;
                                }
                            }
                        } else {
                            // 3. prev_log_* match, replicate logs
                            judging.success = true;
                            let start_index = args.prev_log_index as usize + 1;
                            if !args.entries.is_empty() {
                                debug!(
                                    "{self} copy to logs[{:?}](while self.len={})",
                                    start_index..(start_index + args.entries.len()),
                                    self.log.len(),
                                );
                                // keep [..start_index)
                                self.log.truncate(start_index);
                                // copy, todo: avoid clone by `extend_from_slice`
                                self.log.extend_from_slice(&args.entries[..]);
                                // always hold because log extented
                                assert!(self.last_log().index as usize > self.commit_index);
                            } else {
                                // heartbeat
                                // heartbeat rpc has also prev_log_index/term, to ensure & track followers' progress
                                // and also follower's last_log may > prev_log_index(from new leader),
                                // this is because this follower used to be leader, and recv `start` command from upper
                                // layer, so its logs advanced
                                // but now, new leader is coming to power, the `start` command before should be revert
                                if start_index <= self.log.len() {
                                    debug!(
                                        "{self} reply heartbeat with logs[{:?}] truncated",
                                        start_index..self.log.len()
                                    );
                                } else {
                                    debug!("{self} reply heartbeat");
                                }
                                self.log.truncate(start_index);
                                // heartbeat may also contain latest leader commit_index, so still need try to apply
                            }

                            // try update commit index
                            if args.leader_commit as usize > self.commit_index {
                                // self.log is extend, so self.last_log().index is SURE > self.commit_index
                                // so we pick min(args.leader_commit, self.last_log().index)
                                let to = args.leader_commit.min(self.last_log().index) as usize;
                                self.try_commit_to_and_apply(to);
                            }
                        }
                    }
                }
                (judging, false)
            }
        }
    }

    fn follower_on_event(&mut self, evt: RaftEvent, timer: &mut Fuse<Delay>) -> bool {
        match evt {
            RaftEvent::RequestVote(args, tx) => {
                let (reply, reenter) = self.on_request_vote(&args);
                if reenter {
                    self.become_follower(args.term, Some(args.candidate as usize));
                    // resend RV, handle it when I reenter
                    let _ = self
                        .event_tx
                        .unbounded_send(RaftEvent::RequestVote(args, tx));
                    return true;
                } else {
                    // stay
                    let _ = tx.send(reply);
                }
            }
            RaftEvent::AppendEntries(args, tx) => {
                let (reply, reenter) = self.follower_on_append_entries(&args);
                if reenter {
                    self.become_follower(args.term, None);
                    // resend AE, handle it when I enenter follower
                    let _ = self
                        .event_tx
                        .unbounded_send(RaftEvent::AppendEntries(args, tx));
                    return true;
                } else {
                    let _ = tx.send(reply);
                }
                // reset timer if it's legal AE from current leader
                // todo: refactor this
                if self.term.load(Ordering::SeqCst) == args.term {
                    let ms = election_timeout_ms();
                    debug!("{self} reset election_timeout={}ms", ms.as_millis());
                    *timer = Delay::new(ms).fuse();
                }
            }
            RaftEvent::StartCommand(buf, tx) => {
                let reply = self.start(buf);
                let _ = tx.send(reply);
            }
            RaftEvent::Kill => {
                debug!("{self} kill signal!");
                self.role.store(RaftRole::Killed, Ordering::SeqCst);
                return true;
            }
            unknown => {
                warn!("{self} unknown event: {unknown:?}");
            }
        }
        false
    }

    async fn run_follower(&mut self) {
        let ms = election_timeout_ms();
        debug!(
            "{self} run_follower with election_timeout={}ms",
            ms.as_millis()
        );
        let mut election_timer = Delay::new(ms).fuse();
        loop {
            futures::select! {
                opt = self.event_rx.next() => match opt {
                    Some(evt) => {
                        if self.follower_on_event(evt, &mut election_timer) {
                            break;
                        }
                    },
                    _ => {
                        error!("{self} event channel closed?");
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
        let (last_log_index, last_log_term) = {
            let last = self.last_log();
            (last.index, last.term)
        };
        let args = RequestVoteArgs {
            term,
            candidate: self.me as u64,
            last_log_index,
            last_log_term,
        };

        let me = self.me;
        for svr in 0..self.peers.len() {
            if svr == self.me {
                continue;
            }
            debug!("{self} send RV to {svr}: {args:?}");
            let rx = self.send_request_vote(svr, args.clone());
            let event_tx = self.event_tx.clone();
            self.peers[me].spawn(async move {
                if let Ok(result) = rx.await {
                    let _ = event_tx.unbounded_send(RaftEvent::RequestVoteReply { svr, result });
                }
            });
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
            // we're going to follower, but also need to check log consistency
            // it means we do as:
            // 1. to follower
            // 2. check log consistency
            // 3. reply as role of follower
            // so we just put placeholders here
            conflict_log_index: 0, // to-be-updated
            conflict_log_term: 0,  // to-to-updated
        };
        let rejected = AppendEntriesReply {
            term: curr_term,
            success: false,
            conflict_log_index: 0, // unused
            conflict_log_term: 0,  // unused
        };
        if args.term < curr_term {
            debug!("{self} reject AE: {args:?}");
            (rejected, false)
        } else {
            debug!("{self} judging AE: {args:?}");
            (accepted, true)
        }
    }

    fn candidate_on_event(&mut self, evt: RaftEvent, votes: &mut usize) -> bool {
        match evt {
            RaftEvent::RequestVote(args, tx) => {
                let (reply, to_follower) = self.on_request_vote(&args);
                if to_follower {
                    self.become_follower(args.term, Some(args.candidate as usize));
                    // resend RV, handle it when I become follower
                    let _ = self
                        .event_tx
                        .unbounded_send(RaftEvent::RequestVote(args, tx));
                    return true;
                } else {
                    // stay
                    let _ = tx.send(reply);
                }
            }
            RaftEvent::RequestVoteReply { svr, result } => {
                let term = self.term.load(Ordering::SeqCst);
                let tot = self.peers.len();
                match result {
                    Ok(r) => match r.term.cmp(&term) {
                        cmp::Ordering::Less => {
                            debug!("{self} ignore RV reply (lower term): {} < {}", r.term, term);
                            return false;
                        }
                        cmp::Ordering::Greater => {
                            debug!(
                                "{self} drop RV reply, to follower (higher term): {} > {}",
                                r.term, term
                            );
                            self.become_follower(r.term, None);
                            return true;
                        }
                        cmp::Ordering::Equal => {
                            if r.vote_granted {
                                *votes += 1;
                            }
                            debug!("{self} recv RV reply from RAFT-{svr}: {r:?}, votes = {votes}/{tot}");
                        }
                    },
                    Err(e) => {
                        error!("{self} recv RV reply error from RAFT-{svr}: {e}")
                    }
                }
                // once quorum is set, to leader immediately
                if *votes > tot / 2 {
                    self.become_leader();
                    return true;
                }
            }
            RaftEvent::AppendEntries(args, tx) => {
                let (reply, to_follower) = self.candidate_on_append_entries(&args);
                if to_follower {
                    self.become_follower(args.term, None);
                    // resend AE, handle it when I become follower
                    let _ = self
                        .event_tx
                        .unbounded_send(RaftEvent::AppendEntries(args, tx));
                    return true;
                } else {
                    // stay
                    let _ = tx.send(reply);
                }
            }
            RaftEvent::StartCommand(buf, tx) => {
                let reply = self.start(buf);
                let _ = tx.send(reply);
            }
            RaftEvent::Kill => {
                debug!("{self} kill signal!");
                self.role.store(RaftRole::Killed, Ordering::SeqCst);
                return true;
            }
            unknown => {
                warn!("{self} unknown event: {unknown:?}");
            }
        }
        false
    }

    async fn run_cadidate(&mut self) {
        let ms = election_timeout_ms();
        debug!(
            "{self} run_candidate with election_timeout={}ms",
            ms.as_millis()
        );
        let mut election_timer = Delay::new(ms).fuse();
        let mut votes = 1; // self +1
        self.broadcast_request_vote();
        loop {
            futures::select! {
                _ = election_timer => {
                    // new election when timeout
                    self.become_candidate();
                    break;
                }
                opt = self.event_rx.next() => match opt {
                    Some(evt) => {
                        if self.candidate_on_event(evt, &mut votes) {
                            break;
                        }
                    },
                    _ => {
                        error!("{self} event channel closed?");
                        break;
                    }
                }
            }
        }
    }

    fn become_candidate(&mut self) {
        let term = self.term.load(Ordering::SeqCst);
        info!("{self} => CAN[{}]", term + 1);
        self.term.fetch_add(1, Ordering::SeqCst);
        self.role.store(RaftRole::Candidate, Ordering::SeqCst);
        self.voted_for = Some(self.me);
    }

    fn become_follower(&mut self, term: u64, voted_for: Option<usize>) {
        info!("{self} => FLR[{term}]");
        self.term.store(term, Ordering::SeqCst);
        self.role.store(RaftRole::Follower, Ordering::SeqCst);
        self.voted_for = voted_for;
    }

    fn become_leader(&mut self) {
        let term = self.term.load(Ordering::SeqCst);
        info!("{self} => LDR[{term}]");
        self.role.store(RaftRole::Leader, Ordering::SeqCst);
        self.voted_for = None;
        let last_log_index = self.last_log().index as usize;
        // init index when becoming leader, me index also changed
        for svr in 0..self.peers.len() {
            self.next_index[svr] = last_log_index + 1;
            self.match_index[svr] = 0;
        }
        self.match_index[self.me] = last_log_index; // correct me match_index
    }

    fn last_log(&self) -> &LogEntry {
        assert!(!self.log.is_empty()); // always true coz dummy
        self.log.last().unwrap()
    }

    fn prev_log_of(&self, svr: usize) -> (u64, u64) {
        assert!(svr < self.peers.len());
        let prev_log = self.next_index[svr] - 1; // todo: what if prev_index == 0?
        let prev_log_index = self.log[prev_log].index;
        assert_eq!(prev_log as u64, prev_log_index); // always true coz dummy.index = 0
        let prev_log_term = self.log[prev_log].term;
        (prev_log_index, prev_log_term)
    }

    // follower need to APPLY too (as well as call `applyCh`, test framework checks applyed fsm of all servers)
    // NOTE: follower apply not beyond `args.leader_commit`, and `leader_commit` ensure all logs < it have been
    // safely replicated and will never rollback, so follower apply to (at most up-to) `leader_commit` is safe
    fn try_commit_to_and_apply(&mut self, to: usize) {
        let me = self.me;
        debug!(
            "{self} commit_index to {to}, last_applied = {}",
            self.last_applied
        );
        self.commit_index = to;

        if self.commit_index <= self.last_applied {
            return;
        }

        assert!(self.last_applied < self.log.len());
        let start = self.last_applied + 1;
        let apply_msgs = self
            .log
            .iter()
            .skip(start)
            .map(|e| ApplyMsg::Command {
                data: e.command.clone(),
                index: e.index,
            })
            .collect::<Vec<_>>();
        debug!("{self} apply logs[{:?}]", start..(start + apply_msgs.len()),);

        // todo: `unbound_send` or async `send_all`?
        let mut apply_ch = self.apply_ch.clone();
        self.peers[me].spawn(async move {
            let mut stream = futures::stream::iter(apply_msgs).map(Ok);
            let _ = apply_ch.send_all(&mut stream).await;
        });
        self.last_applied = self.commit_index;
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
    // me never change
    me: usize,
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
        let me = raft.me;
        let role = raft.role.clone();
        let term = raft.term.clone();
        let event_tx = raft.event_tx.clone();

        let pool = ThreadPool::new().unwrap();
        pool.spawn_ok(raft.run());

        Self {
            me,
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
        // `Node` is for simulate, actually here we send command encoded as bytes
        // and then deoced to pb message at raft side
        let (tx, rx) = mpsc::sync_channel(1); // 1 == oneshot
        let mut encoded = vec![];
        labcodec::encode(command, &mut encoded).map_err(Error::Encode)?;
        if let Err(e) = self
            .event_tx
            .unbounded_send(RaftEvent::StartCommand(encoded, tx))
        {
            error!("RAFT-{} start failed: {e}", self.me);
        }
        info!("NODE-{} start command: {command:?}", self.me);
        let resp = rx.recv().expect("sync sender dropped with no msg!");
        info!("NODE-{} start command reply: {resp:?}", self.me);
        resp
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
            error!("node-{} kill failed: {e}", self.me);
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
    // I leave notes about how to react to `reqeust_vote` rpc here:
    // case by role:
    //
    // LEADER, FOLLOWER
    // if sender.term > self.term:
    //   to_follower(term = sender.term, voted_for = sender)
    //   deal_with_it_as_follower again, which means goto branch: sender.term == self.term, voted_for = sender to check if up-to-date
    // elif sender.term < sel.term: reject
    // else:
    //  if can_vote_sender (voted_for is None | Some(sender)):
    //      if sender is up-to-date: ok, voted_for = sender if voted_for is None
    //      else: reject
    //          if voted_for is None: keep it as
    //          else: sender fall behind? I think should change it None
    //  else (!=None & !=sender, which means voted other before): reject
    //
    // CANDIDATE
    // begin with voted_for=self|other, never None
    // if sender.term > self.term:
    //   to_follower(term = sender.term, voted_for = sender)
    //   deal_with_it_as_follower again ..
    // elif sender.term < sel.term: reject
    // else:
    //  voted_for cannot be None, coz set to self already when entering candidate
    //  voted_for cannot be other, same as above
    //  so it's always self, and when candidate recv request_vote, it has only one case:
    //  voted_for == Some(self), so only to reject
    //
    // Q: why there's condition (as follower) voted_for == None? or when could it be None?
    // A: when follower recv AE from leader, set voted_for = None; and when it's follower,
    //    it may recv request_vote too, then it can vote sender
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

// after got replies of AE from followers, we should calculate quorum of match_index
// a quick way is to sort match_index, if over 1/2 of them are greater-or-equal than
// a value, it's safe to commit to that, it's eqv to:
// sort match_index reversely, i.e., [4, 3, 2, 1], quorum needs 3, so 2 is at-least
// we can commit to, for odd case, [5, 4, 3, 2, 1], it's 3, both at index = len/2
fn get_majority_same_index(mut match_index: Vec<usize>) -> usize {
    let len = match_index.len();
    match_index.sort_unstable_by_key(|k| std::cmp::Reverse(*k));
    match_index[len / 2]
}

// election refs:
// https://stackoverflow.com/questions/77381650/leader-election-and-appendentries-rejection
// https://stackoverflow.com/questions/57868272/should-raft-follower-update-term-on-receiving-vote-request-with-higher-term
// https://groups.google.com/g/raft-dev/c/KIozjYuq5m0
