use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    mem,
    sync::Arc,
};

use crate::{
    io::{
        initial_state::{HardState, InitialState},
        BeginDownloadSnapshotAction,
    },
    Entry, LogIndex, Membership, NodeId, Term, Timestamp,
};

pub struct CommonState<D> {
    pub this_id: NodeId,
    pub hard_state: HardState,
    pub last_applied_log_index: LogIndex,
    pub last_applied_log_term: Term,
    pub last_applied_membership: Membership,
    pub unapplied_log_terms: VecDeque<Term>,
    pub unapplied_membership_changes: BTreeMap<LogIndex, Membership>,

    // Best guess at current leader, may not be accurate...
    pub leader_id: Option<NodeId>,
    pub election_timeout: Option<Timestamp>,

    pub requested_log_entries: BTreeSet<LogIndex>,
    pub loaded_log_entries: BTreeMap<LogIndex, Arc<Entry<D>>>,
    pub base_log_index: LogIndex,
    pub base_log_term: Term,
    pub base_membership: Membership,
    pub downloading_snapshot: Option<BeginDownloadSnapshotAction>,
}

impl<D> CommonState<D> {
    pub fn new(this_id: NodeId, initial_state: InitialState) -> Self {
        let (base_log_index, base_log_term, base_membership) =
            if let Some(snapshot) = initial_state.initial_snapshot {
                (
                    snapshot.last_log_index,
                    snapshot.last_log_term,
                    snapshot.last_membership,
                )
            } else {
                Default::default()
            };
        Self {
            this_id,
            hard_state: initial_state.hard_state,
            last_applied_log_index: base_log_index,
            last_applied_log_term: base_log_term,
            last_applied_membership: base_membership.clone(),
            unapplied_log_terms: initial_state.log_terms,
            unapplied_membership_changes: initial_state.membership_changes,
            leader_id: None,
            election_timeout: None,
            requested_log_entries: BTreeSet::new(),
            loaded_log_entries: BTreeMap::new(),
            base_log_index,
            base_log_term,
            base_membership,
            downloading_snapshot: None,
        }
    }
    pub fn mark_not_leader(&mut self) {
        if self.leader_id == Some(self.this_id) {
            self.leader_id = None;
        }
    }
    pub fn last_log_index(&self) -> LogIndex {
        self.last_applied_log_index + (self.unapplied_log_terms.len() as u64)
    }

    pub fn last_log_term(&self) -> Term {
        self.unapplied_log_terms
            .back()
            .copied()
            .unwrap_or(self.last_applied_log_term)
    }

    pub fn current_membership(&self) -> &Membership {
        self.unapplied_membership_changes
            .values()
            .next_back()
            .unwrap_or(&self.last_applied_membership)
    }

    pub fn is_up_to_date(&self, last_log_term: Term, last_log_index: LogIndex) -> bool {
        last_log_term > self.last_log_term()
            || (last_log_term == self.last_log_term() && last_log_index >= self.last_log_index())
    }

    pub fn apply_up_to(&mut self, apply_up_to: LogIndex) {
        let count = (apply_up_to - self.last_applied_log_index) as usize;
        if count == 0 {
            return;
        }

        // First advance our applied index
        self.last_applied_log_index = apply_up_to;

        // Next, chop off the "unapplied log terms" which have now been applied, and use
        // the last one as our new "last applied log term".
        self.last_applied_log_term = self
            .unapplied_log_terms
            .drain(0..count)
            .next_back()
            .expect("Iterator to not be empty");

        // Finally, chop off the "unapplied membership changes" which have now been applied,
        // and use the last one as our new "last applied membership".
        let mut tmp = self
            .unapplied_membership_changes
            .split_off(&(self.last_applied_log_index + 1));
        mem::swap(&mut tmp, &mut self.unapplied_membership_changes);
        if let Some((_, m)) = tmp.into_iter().next_back() {
            self.last_applied_membership = m;
        }
    }
}
