use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{
    entry::{Entry, EntryFromRequest},
    membership::Membership,
    state::Error,
    DatabaseId, LogIndex, NodeId, Term,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PersistentState<D> {
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
    pub pending_log_changes: Vec<LogChange<D>>,
    /// Loaded log entries
    pub cached_log_entries: BTreeMap<LogIndex, Arc<Entry<D>>>,
    /// Desired log entries
    pub desired_log_entries: BTreeSet<LogIndex>,
}

impl<D> Default for PersistentState<D> {
    fn default() -> Self {
        Self {
            database_id: Default::default(),
            current_term: Default::default(),
            voted_for: Default::default(),
            current_membership: Default::default(),
            last_log_applied: Default::default(),
            last_term_applied: Default::default(),
            last_membership_applied: Default::default(),
            unapplied_log_terms: Default::default(),
            pending_log_changes: Default::default(),
            cached_log_entries: Default::default(),
            desired_log_entries: Default::default(),
        }
    }
}

impl<D> PersistentState<D> {
    pub fn add_log_change(&mut self, log_change: LogChange<D>) {
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
    pub fn acknowledge_database_id(&mut self, database_id: DatabaseId) -> Result<(), Error> {
        if !self.database_id.is_set() {
            self.database_id = database_id;
        }
        if self.database_id == database_id {
            Ok(())
        } else {
            Err(Error::DatabaseMismatch)
        }
    }
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
