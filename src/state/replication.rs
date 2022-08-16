use crate::{LogIndex, Timestamp};

pub struct ReplicationState {
    pub send_after_index: LogIndex,
    pub match_index: LogIndex,
    pub retry_at: Option<Timestamp>,
    pub in_flight_request: bool,
    pub waiting_on_storage: bool,
}

impl ReplicationState {
    pub fn should_retry(&self, timestamp: Timestamp) -> bool {
        if let Some(retry_at) = self.retry_at {
            timestamp >= retry_at
        } else {
            false
        }
    }
}
