use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{
    entry::Entry,
    membership::Membership,
    types::{DatabaseId, LogIndex, NodeId, RequestId, Term, Timestamp},
};

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq)]
pub struct Output {
    pub next_tick: Option<Timestamp>,
    pub persistent_state: PersistentState,
    pub messages: Vec<Message>,
    pub failed_requests: Vec<FailedRequest>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FailedRequest {
    pub request_id: RequestId,
    pub error: RequestError,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum RequestError {
    NotLeader,
    Busy,
    Bootstrap(BootstrapError),
    SetMembers(SetMembersError),
    SetLearners(SetLearnersError),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum BootstrapError {
    ClusterAlreadyInitialized,
    ThisNodeMustBeVoter,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SetMembersError {
    AlreadyChanging,
    InvalidMembers,
    InsufficientFaultTolerance {
        proposed_ids: Option<BTreeSet<NodeId>>,
    },
    TooManyLaggingMembers {
        lagging_ids: BTreeSet<NodeId>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SetLearnersError {
    AlreadyChanging,
    ExistingMembers { member_ids: BTreeSet<NodeId> },
}

impl Output {
    pub fn schedule_tick(&mut self, timestamp: Timestamp) {
        if let Some(next_tick) = &mut self.next_tick {
            *next_tick = (*next_tick).min(timestamp);
        } else {
            self.next_tick = Some(timestamp);
        }
    }
    pub fn add_message(&mut self, message: Message) {
        self.messages.push(message);
    }
    pub fn add_failed_request(&mut self, failed_request: FailedRequest) {
        self.failed_requests.push(failed_request)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Input {
    pub timestamp: Timestamp,
    pub persistent_state: PersistentState,
    pub event: Event,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PersistentState {
    pub database_id: DatabaseId,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub current_membership: Membership,
    /// The index of the last log applied to the state machine.
    pub last_log_applied: LogIndex,
    /// The term of the last log applied to the state machine.
    pub last_term_applied: Term,
    /// The last membership change applied to the state machine
    pub last_membership_applied: Membership,
    /// The terms for log entries which have not yet been applied
    /// to the state machine.
    pub unapplied_log_terms: VecDeque<Term>,
    /// Log changes which have yet to be persisted
    pub pending_log_changes: Vec<LogChange>,
    /// Loaded log entries
    pub cached_log_entries: BTreeMap<LogIndex, Arc<Entry>>,
    /// Desired log entries
    pub desired_log_entries: BTreeSet<LogIndex>,
}

impl PersistentState {
    pub fn add_log_change(&mut self, log_change: LogChange) {
        self.pending_log_changes.push(log_change);
    }

    pub fn last_log_index(&self) -> LogIndex {
        self.last_log_applied + self.unapplied_log_terms.len() as u64
    }

    pub fn last_log_term(&self) -> Term {
        self.unapplied_log_terms
            .back()
            .copied()
            .unwrap_or(self.last_term_applied)
    }

    pub fn can_vote_for(&self, term: Term, candidate_id: NodeId) -> bool {
        term == self.current_term
            && (self.voted_for.is_none() || self.voted_for == Some(candidate_id))
    }

    pub fn is_up_to_date(&self, last_log_term: Term, last_log_index: LogIndex) -> bool {
        last_log_term > self.last_log_term()
            || (last_log_term == self.last_log_term() && last_log_index >= self.last_log_index())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum LogChange {
    Replicate(LogRange),
    Apply(LogIndex),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Event {
    Tick,
    StateChanged,
    Message(Message),
    ClientRequest(ClientRequest),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Message {
    pub from_id: NodeId,
    pub to_id: NodeId,
    pub payload: MessagePayload,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum MessagePayload {
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    PreVoteRequest(PreVoteRequest),
    PreVoteResponse(PreVoteResponse),
    InstallSnapshotRequest(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AppendEntriesRequest {
    pub database_id: DatabaseId,
    pub term: Term,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<Arc<Entry>>,
    pub leader_commit: LogIndex,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub match_index: Option<LogIndex>,
    pub conflict_opt: Option<ConflictOpt>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EntryFromClient {
    pub request_id: Option<RequestId>,
    pub entry: Arc<Entry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LogRange {
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<EntryFromClient>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LogState {
    /// The index of the last entry.
    pub last_log_index: LogIndex,
    /// The index of the last log applied to the state machine.
    pub last_log_applied: LogIndex,
    /// The last membership change applied to the state machine
    pub last_membership_applied: Membership,
}

/// A struct used to implement the _conflicting term_ optimization outlined in §5.3 for log replication.
///
/// This value will only be present, and should only be considered, when an `AppendEntriesResponse`
/// object has a `success` value of `false`.
///
/// This implementation of Raft uses this value to more quickly synchronize a leader with its
/// followers which may be some distance behind in replication, may have conflicting entries, or
/// which may be new to the cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConflictOpt {
    /// The index of the most recent entry which does not conflict with the received request.
    pub index: LogIndex,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VoteRequest {
    /// The unique database ID
    pub database_id: DatabaseId,
    /// The candidate's current term.
    pub term: Term,
    /// The candidate's ID.
    pub candidate_id: NodeId,
    /// The index of the candidate’s last log entry (§5.4).
    pub last_log_index: LogIndex,
    /// The term of the candidate’s last log entry (§5.4).
    pub last_log_term: Term,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VoteResponse {
    /// The current term of the responding node, for the candidate to update itself.
    pub term: Term,
    /// Will be true if the candidate received a vote from the responder.
    pub vote_granted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PreVoteRequest {
    /// The unique database ID
    pub database_id: DatabaseId,
    /// The term which we will enter if the pre-vote succeeds.
    pub next_term: Term,
    /// The candidate's ID.
    pub candidate_id: NodeId,
    /// The index of the candidate’s last log entry (§5.4).
    pub last_log_index: LogIndex,
    /// The term of the candidate’s last log entry (§5.4).
    pub last_log_term: Term,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PreVoteResponse {
    /// The current term of the responding node, for the candidate to update itself.
    pub term: Term,
    /// Will be true if the candidate received a vote from the responder.
    pub vote_granted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InstallSnapshotRequest {
    /// The unique database ID
    pub database_id: DatabaseId,
    /// The leader's current term.
    pub term: Term,
    /// The leader's ID. Useful in redirecting clients.
    pub leader_id: NodeId,
    /// The snapshot replaces all log entries up through and including this index.
    pub last_included_index: LogIndex,
    /// The term of the `last_included_index`.
    pub last_included_term: Term,
    /// The byte offset where chunk is positioned in the snapshot file.
    pub offset: u64,
    /// The raw Vec<u8> of the snapshot chunk, starting at `offset`.
    pub data: Vec<u8>,
    /// Will be `true` if this is the last chunk in the snapshot.
    pub done: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InstallSnapshotResponse {
    /// The receiving node's current term, for leader to update itself.
    pub term: Term,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClientRequest {
    pub request_id: Option<RequestId>,
    pub payload: ClientRequestPayload,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ClientRequestPayload {
    Bootstrap(BootstrapRequest),
    Application,
    SetMembers(SetMembersRequest),
    SetLearners(SetLearnersRequest),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BootstrapRequest {
    pub database_id: DatabaseId,
    pub voter_ids: BTreeSet<NodeId>,
    pub learner_ids: BTreeSet<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SetMembersRequest {
    pub fault_tolerance: u64,
    pub member_ids: BTreeSet<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SetLearnersRequest {
    pub learner_ids: BTreeSet<NodeId>,
}
