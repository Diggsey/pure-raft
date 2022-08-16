use rand::{thread_rng, Rng};

use crate::{
    io::{messages::MessagePayload, Output},
    Duration, LogIndex, Message, NodeId, Timestamp,
};

use super::config::Config;

pub struct CommonState {
    pub this_id: NodeId,
    pub config: Config,
    pub leader_commit_index: LogIndex,
    pub committed_index: LogIndex,
    // Best guess at current leader, may not be accurate...
    pub leader_id: Option<NodeId>,
    pub election_timeout: Option<Timestamp>,
}

impl CommonState {
    pub fn new(this_id: NodeId, config: Config) -> Self {
        Self {
            this_id,
            config,
            leader_commit_index: LogIndex::ZERO,
            committed_index: LogIndex::ZERO,
            leader_id: None,
            election_timeout: None,
        }
    }
    pub fn mark_not_leader(&mut self) {
        if self.leader_id == Some(self.this_id) {
            self.leader_id = None;
        }
    }
    pub fn schedule_election_timeout<D>(&mut self, timestamp: Timestamp, output: &mut Output<D>) {
        let election_timeout = timestamp
            + Duration(thread_rng().gen_range(
                self.config.min_election_timeout.0..=self.config.max_election_timeout.0,
            ));
        self.election_timeout = Some(election_timeout);
        output.schedule_tick(election_timeout);
    }
    pub fn election_timeout_elapsed(&self, timestamp: Timestamp) -> bool {
        if let Some(election_timeout) = self.election_timeout {
            election_timeout <= timestamp
        } else {
            false
        }
    }

    pub fn send_message<D>(
        &self,
        to_id: NodeId,
        output: &mut Output<D>,
        payload: MessagePayload<D>,
    ) {
        output.add_message(Message {
            from_id: self.this_id,
            to_id,
            payload,
        });
    }
}
