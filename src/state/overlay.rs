use std::{cmp, collections::BTreeSet};

use crate::{
    io::{errors::RequestError, FailedRequest},
    AppendEntriesRequest, Config, DownloadSnapshotRequest, EntryFromRequest, EntryPayload,
    LogIndex, Message, MessagePayload, NodeId, StateError, Timestamp,
};

use super::{common::CommonState, replication::ReplicationState};

pub struct OverlayState<'a, D> {
    // Semi-persistent state
    pub config: &'a mut Config,
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
    pub errors: Vec<StateError>,
    pub reset_to_snapshot: bool,
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
            + self.config.random_sampler.sample_duration(
                self.config.min_election_timeout..=self.config.max_election_timeout,
            );
        self.common.election_timeout = Some(election_timeout);
    }

    fn truncate_pending_log(&mut self, size: usize) {
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
            self.truncate_pending_log((truncate_log_to - self.truncate_log_to) as usize);
        } else {
            self.truncate_pending_log(0);
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

    pub fn reset_to_snapshot(&mut self) {
        self.reset_to_snapshot = true;
        self.committed_index = self.common.base_log_index;
        self.truncate_log_to = self.common.base_log_index;
        self.truncate_pending_log(0);
        self.common.last_applied_log_index = self.common.base_log_index;
        self.common.last_applied_log_term = self.common.base_log_term;
        self.common.last_applied_membership = self.common.base_membership.clone();
        self.common.unapplied_log_terms.clear();
        self.common.unapplied_membership_changes.clear();
    }

    pub fn update_replication_state(
        &mut self,
        node_id: NodeId,
        replication_state: &mut ReplicationState,
    ) {
        replication_state.waiting_on_storage = false;
        let prev_log_index = replication_state.send_after_index;

        // Calculate number of log entries we still need to send to this node
        let unsent_entries = self.common.last_log_index() - prev_log_index;
        let needs_snapshot = prev_log_index < self.common.base_log_index;
        let should_wait =
            unsent_entries == 0 || needs_snapshot || replication_state.in_flight_request;

        // If the hearbeat timeout is up, or if we have unsent log entries and we don't have an
        // in-flight request, then we need to send one.
        if !should_wait || replication_state.should_retry(self.timestamp) {
            // If the follower is behind our base log index
            if needs_snapshot {
                // Then we need to send a snapshot
                self.send_message(
                    node_id,
                    MessagePayload::InstallSnapshotRequest(DownloadSnapshotRequest {
                        database_id: self.common.hard_state.database_id,
                        term: self.common.hard_state.current_term,
                        last_log_index: self.common.base_log_index,
                    }),
                );
                replication_state.in_flight_request = true;
                replication_state.retry_at = Some(self.timestamp + self.config.heartbeat_interval);
            } else {
                // Otherwise, we can send individual log entries
                let num_to_send = cmp::min(unsent_entries, self.config.batch_size);

                // Try to get the term of the previous log entry
                let mut loaded_prev_log_term = false;
                let maybe_prev_log_term =
                        // First check if the log entry has not yet been applied - in that case, we can get
                        // the log term from our "last applied term" or "unapplied log terms" array.
                        if prev_log_index >= self.common.last_applied_log_index {
                            Some(
                                if prev_log_index == self.common.last_applied_log_index {
                                    self.common.last_applied_log_term
                                } else {
                                    self.common.unapplied_log_terms[(prev_log_index
                                        - self.common.last_applied_log_index
                                        - 1)
                                        as usize]
                                },
                            )
                        // Next, check if the log entry is the base entry we were initialized with.
                        // If that's the case, we can directly return the base log term. We want to
                        // avoid trying to load the entry in this case because it will not be stored.
                        } else if prev_log_index == self.common.base_log_index {
                            Some(self.common.base_log_term)
                        // Finally, check if the entry is already loaded.
                        } else if let Some(entry) =
                            self.common.loaded_log_entries.get(&prev_log_index)
                        {
                            loaded_prev_log_term = true;
                            Some(entry.term)
                        // Otherwise, we need to wait for it to be loaded.
                        } else {
                            replication_state.waiting_on_storage = true;
                            None
                        };

                // Try to satisfy the request using log entries already in the cache
                let mut entries = Vec::new();
                for i in 0..num_to_send {
                    let log_index = prev_log_index + i + 1;
                    if let Some(entry) = self.common.loaded_log_entries.get(&log_index) {
                        entries.push(entry.clone());
                    } else {
                        replication_state.waiting_on_storage = true;
                        break;
                    }
                }

                if !replication_state.waiting_on_storage {
                    // Must not indicate a leader commit index ahead of the last entry in
                    // the request payload.
                    let leader_commit = self
                        .committed_index
                        .min(prev_log_index + (entries.len() as u64));
                    // If all the log entries we needed were present, then send the request
                    self.send_message(
                        node_id,
                        MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                            database_id: self.common.hard_state.database_id,
                            term: self.common.hard_state.current_term,
                            prev_log_index,
                            prev_log_term: maybe_prev_log_term
                                .expect("To be set if not waiting on storage"),
                            entries,
                            leader_commit,
                        }),
                    );
                    replication_state.in_flight_request = true;
                    replication_state.retry_at =
                        Some(self.timestamp + self.config.heartbeat_interval);
                } else {
                    // Otherwise, we were missing an entry, so populate our desired set of
                    // log entries.
                    if maybe_prev_log_term.is_none() || loaded_prev_log_term {
                        self.desired_log_entries.insert(prev_log_index);
                    }
                    for i in 0..num_to_send {
                        self.desired_log_entries
                            .insert(replication_state.send_after_index + i + 1);
                    }
                }
            }
        }
    }
}
