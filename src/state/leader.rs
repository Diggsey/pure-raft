use std::collections::BTreeMap;

use crate::{io::Output, NodeId};

use super::{common::CommonState, replication::ReplicationState};

#[derive(Default)]
pub struct LeaderState {
    pub replication_state: BTreeMap<NodeId, ReplicationState>,
}

impl LeaderState {
    pub fn is_up_to_date<D>(
        &self,
        common: &CommonState,
        node_id: NodeId,
        output: &Output<D>,
    ) -> bool {
        if common.this_id == node_id {
            true
        } else if let Some(replication_state) = self.replication_state.get(&node_id) {
            replication_state.match_index + common.config.batch_size
                >= output.persistent_state.last_log_index()
        } else {
            false
        }
    }
}
