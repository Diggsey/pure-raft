use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};

use crate::{
    entry::EntryFromRequest, membership::Membership, DatabaseId, LogIndex, NodeId, StateError, Term,
};

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq)]
pub struct InitialState {
    /// These fields should be loaded directly from storage
    pub hard_state: HardState,
    /// The terms from all stored log entries. Log entries which are
    /// part of the initial snapshot should not be included.
    pub log_terms: VecDeque<Term>,
    /// All membership changes from the log (not including those
    /// part of the initial snapshot).
    pub membership_changes: BTreeMap<LogIndex, Membership>,
    /// The initial snapshot if present.
    pub initial_snapshot: Option<Snapshot>,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq)]
pub struct HardState {
    pub database_id: DatabaseId,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

impl HardState {
    pub(crate) fn can_vote_for(&self, term: Term, candidate_id: NodeId) -> bool {
        term == self.current_term
            && (self.voted_for.is_none() || self.voted_for == Some(candidate_id))
    }
    pub(crate) fn acknowledge_database_id(
        &mut self,
        database_id: DatabaseId,
    ) -> Result<(), StateError> {
        if !self.database_id.is_set() {
            self.database_id = database_id;
        }
        if self.database_id == database_id {
            Ok(())
        } else {
            Err(StateError::DatabaseMismatch)
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Snapshot {
    pub database_id: DatabaseId,
    /// The index of the last log entry included in the snapshot.
    pub last_log_index: LogIndex,
    /// The term of the last log entry included in the snapshot.
    pub last_log_term: Term,
    /// The last membership change included in the snapshot, or default.
    pub last_membership: Membership,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum LogChange<D> {
    Replicate(LogRange<D>),
    Apply(LogIndex),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LogRange<D> {
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<EntryFromRequest<D>>,
}
