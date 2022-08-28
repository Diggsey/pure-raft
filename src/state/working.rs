use std::collections::BTreeSet;

use crate::{
    EntryFromRequest, Event, Input, LogIndex, Message, PersistentState, State, Term, Timestamp,
};

struct WorkingState<'a, D> {
    pub timestamp: Timestamp,
    pub persistent_state: PersistentState<D>,
    pub event: Event<D>,
    pub state: &'a mut State<D>,
    /// Desired log entries
    pub desired_log_entries: BTreeSet<LogIndex>,
    pub apply_log_entries_up_to: LogIndex,
    pub truncate_log_to: LogIndex,
    pub append_log_entries: Vec<EntryFromRequest<D>>,
}

impl<'a, D> WorkingState<'a, D> {
    pub fn last_log_index(&self) -> LogIndex {
        self.truncate_log_to + self.append_log_entries.len() as u64
    }

    pub fn last_log_term(&self) -> Term {
        if let Some(item) = self.append_log_entries.last() {
            item.entry.term
        } else if self.truncate_log_to <= self.persistent_state.last_log_applied {
            assert_eq!(self.truncate_log_to, self.persistent_state.last_log_applied);
            self.persistent_state.last_term_applied
        } else {
            self.persistent_state.unapplied_log_terms
                [(self.truncate_log_to - self.persistent_state.last_log_applied) as usize - 1]
        }
    }
}
