use std::collections::BTreeSet;

use rand::{thread_rng, Rng};

use crate::{
    io::{errors::RequestError, FailedRequest},
    Config, Duration, EntryFromRequest, EntryPayload, LogIndex, Message, MessagePayload, NodeId,
    Timestamp,
};

use super::common::CommonState;

pub struct OverlayState<'a, D> {
    // Semi-persistent state
    pub config: &'a Config,
    pub common: &'a mut CommonState<D>,

    // The current timestamp
    pub timestamp: Timestamp,

    // Transient state
    pub desired_log_entries: BTreeSet<LogIndex>,
    pub truncate_log_to: LogIndex,
    pub append_log_entries: Vec<EntryFromRequest<D>>,
    pub committed_index: LogIndex,
    pub messages: Vec<Message<D>>,
    pub failed_requests: Vec<FailedRequest>,
    pub changed_match_index: bool,
}

impl<'a, D> OverlayState<'a, D> {
    pub fn election_timeout_elapsed(&self) -> bool {
        if let Some(election_timeout) = self.common.election_timeout {
            election_timeout <= self.timestamp
        } else {
            false
        }
    }

    pub fn send_message(&mut self, to_id: NodeId, payload: MessagePayload<D>) {
        self.messages.push(Message {
            from_id: self.common.this_id,
            to_id,
            payload,
        });
    }

    pub fn schedule_election_timeout(&mut self) {
        let election_timeout = self.timestamp
            + Duration(thread_rng().gen_range(
                self.config.min_election_timeout.0..=self.config.max_election_timeout.0,
            ));
        self.common.election_timeout = Some(election_timeout);
    }

    fn truncate_log_inner(&mut self, size: usize) {
        self.failed_requests.extend(
            self.append_log_entries
                .drain(size..)
                .filter_map(|entry| entry.request_id)
                .map(|request_id| FailedRequest {
                    request_id,
                    error: RequestError::Conflict,
                }),
        );
    }

    pub fn truncate_log_to(&mut self, truncate_log_to: LogIndex) {
        if truncate_log_to > self.truncate_log_to {
            self.truncate_log_inner((truncate_log_to - self.truncate_log_to) as usize);
        } else {
            self.truncate_log_inner(0);
            self.truncate_log_to = truncate_log_to;
        }
        let new_unapplied_log_size = self.truncate_log_to - self.common.last_applied_log_index;
        self.common
            .unapplied_log_terms
            .truncate(new_unapplied_log_size as usize);
        self.common
            .unapplied_membership_changes
            .split_off(&(self.truncate_log_to + 1));

        // Force truncated entries to be unloaded
        self.common
            .requested_log_entries
            .split_off(&(truncate_log_to + 1));
        self.common
            .loaded_log_entries
            .split_off(&(truncate_log_to + 1));
    }

    pub fn extend_log(&mut self, entries: impl IntoIterator<Item = EntryFromRequest<D>>) {
        let offset = self.append_log_entries.len();
        let from_index = self.common.last_log_index();
        self.append_log_entries.extend(entries);
        let count = self.append_log_entries.len() - offset;
        self.common.unapplied_log_terms.extend(
            self.append_log_entries[offset..]
                .iter()
                .map(|entry| entry.entry.term),
        );
        self.common.unapplied_membership_changes.extend(
            self.append_log_entries[offset..]
                .iter()
                .enumerate()
                .filter_map(|(idx, entry)| {
                    if let EntryPayload::MembershipChange(m) = &entry.entry.payload {
                        Some((from_index + idx as u64 + 1, m.clone()))
                    } else {
                        None
                    }
                }),
        );

        // Preload new entries, on the basis that we'll likely be sending them to peers
        self.common
            .requested_log_entries
            .extend((0..count).map(|idx| from_index + idx as u64 + 1));
        self.common.loaded_log_entries.extend(
            self.append_log_entries[offset..]
                .iter()
                .enumerate()
                .map(|(idx, entry)| (from_index + idx as u64 + 1, entry.entry.clone())),
        );

        // If we are the leader, we might be able to immediately commit these entries
        self.changed_match_index = true;
    }
}
