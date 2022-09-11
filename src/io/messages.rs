use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{entry::Entry, DatabaseId, LogIndex, NodeId, Term};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Message<D> {
    pub from_id: NodeId,
    pub to_id: NodeId,
    pub payload: MessagePayload<D>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum MessagePayload<D> {
    AppendEntriesRequest(AppendEntriesRequest<D>),
    AppendEntriesResponse(AppendEntriesResponse),
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    PreVoteRequest(PreVoteRequest),
    PreVoteResponse(PreVoteResponse),
    InstallSnapshotRequest(DownloadSnapshotRequest),
    InstallSnapshotResponse(DownloadSnapshotResponse),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AppendEntriesRequest<D> {
    pub database_id: DatabaseId,
    pub term: Term,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<Arc<Entry<D>>>,
    pub leader_commit: LogIndex,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub match_index: Option<LogIndex>,
    pub conflict_opt: Option<ConflictOpt>,
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
pub struct DownloadSnapshotRequest {
    pub database_id: DatabaseId,
    pub term: Term,
    pub last_log_index: LogIndex,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DownloadSnapshotResponse {
    /// The receiving node's current term, for leader to update itself.
    pub term: Term,
    pub match_index: Option<LogIndex>,
}
