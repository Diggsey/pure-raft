use std::collections::BTreeMap;

use crate::NodeId;

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
}
