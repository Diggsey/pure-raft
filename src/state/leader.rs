use std::{cmp, collections::BTreeMap};

use crate::{membership::MembershipType, LogIndex, NodeId};

use super::{overlay::OverlayState, replication::ReplicationState};

#[derive(Default)]
pub struct LeaderState {
    pub replication_state: BTreeMap<NodeId, ReplicationState>,
}

impl LeaderState {
    pub fn is_up_to_date<D>(&self, overlay: &OverlayState<D>, node_id: NodeId) -> bool {
        if overlay.common.this_id == node_id {
            true
        } else if let Some(replication_state) = self.replication_state.get(&node_id) {
            replication_state.match_index + overlay.config.batch_size
                >= overlay.common.last_log_index()
        } else {
            false
        }
    }
    fn compute_partial_median_match_index<D>(
        &self,
        overlay: &OverlayState<D>,
        next: bool,
    ) -> LogIndex {
        let membership = overlay.common.current_membership();
        let filter_fn = |v: &MembershipType| {
            if next {
                v.is_voter_next
            } else {
                v.is_voter_prev
            }
        };

        let mut match_indices: Vec<_> = membership
            .nodes
            .iter()
            .filter(|(_, v)| filter_fn(v))
            .map(|(k, _)| {
                if let Some(replication_state) = self.replication_state.get(k) {
                    replication_state.match_index
                } else {
                    LogIndex::ZERO
                }
            })
            .collect();

        // The replication state map does not include the leader node itself, so check if the leader
        // is part of the voting group, and incorporate the last log index if so.
        if let Some(mt) = membership.nodes.get(&overlay.common.this_id) {
            if filter_fn(mt) {
                match_indices.push(overlay.common.last_log_index());
            }
        }

        match_indices.sort();

        match_indices[match_indices.len() / 2]
    }
    pub fn compute_median_match_index<D>(&self, overlay: &OverlayState<D>) -> LogIndex {
        cmp::min(
            self.compute_partial_median_match_index(overlay, false),
            self.compute_partial_median_match_index(overlay, true),
        )
    }
}
