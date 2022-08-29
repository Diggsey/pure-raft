use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{
    state::{role::Role, working::WorkingState},
    types::{RequestId, Timestamp},
    Entry, EntryFromRequest, HardState, LogIndex,
};

use self::{client_requests::ClientRequest, errors::RequestError, messages::Message};

pub mod client_requests;
pub mod errors;
pub mod messages;
pub mod persistent_state;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Input<D> {
    pub timestamp: Timestamp,
    pub event: Event<D>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Output<D> {
    pub next_tick: Option<Timestamp>,
    pub actions: Vec<Action<D>>,
}

impl<D> From<WorkingState<'_, D>> for Output<D> {
    fn from(working: WorkingState<'_, D>) -> Self {
        let mut actions = Vec::new();

        // Detect log truncation
        if working.overlay.truncate_log_to < working.original.last_log_index {
            actions.push(Action::TruncateLog(TruncateLogAction {
                last_log_index: working.overlay.truncate_log_to,
            }));
        }

        // Detect log extension
        if !working.overlay.append_log_entries.is_empty() {
            actions.push(Action::ExtendLog(ExtendLogAction {
                entries: working.overlay.append_log_entries,
            }));
        }

        // Detect hard state change
        if working.overlay.common.hard_state != working.original.hard_state {
            actions.push(Action::SaveState(working.overlay.common.hard_state.clone()));
        }

        // Detect sent messages
        if !working.overlay.messages.is_empty() {
            actions.extend(
                working
                    .overlay
                    .messages
                    .into_iter()
                    .map(Action::SendMessage),
            );
        }

        // Detect failed requests
        if !working.overlay.failed_requests.is_empty() {
            actions.extend(
                working
                    .overlay
                    .failed_requests
                    .into_iter()
                    .map(Action::FailedRequest),
            );
        }

        // Detect log entries to be applied
        if working.overlay.committed_index > working.overlay.common.last_applied_log_index {
            working
                .overlay
                .common
                .apply_up_to(working.overlay.committed_index);
            actions.push(Action::ApplyLog(ApplyLogAction {
                up_to_log_index: working.overlay.common.last_applied_log_index,
            }));
        }

        // Detect log entries to be loaded
        let log_entries_to_request: BTreeSet<_> = working
            .overlay
            .desired_log_entries
            .difference(&working.overlay.common.requested_log_entries)
            .copied()
            .collect();
        if !log_entries_to_request.is_empty() {
            working
                .overlay
                .common
                .requested_log_entries
                .extend(log_entries_to_request.iter().copied());
            actions.push(Action::LoadLog(LoadLogAction {
                desired_entries: log_entries_to_request,
            }));
        }

        // Free no-longer-required log entries
        let log_entries_to_free: BTreeSet<_> = working
            .overlay
            .common
            .requested_log_entries
            .difference(&working.overlay.desired_log_entries)
            .copied()
            .collect();
        if !log_entries_to_free.is_empty() {
            for log_index in log_entries_to_free {
                working
                    .overlay
                    .common
                    .requested_log_entries
                    .remove(&log_index);
                working.overlay.common.loaded_log_entries.remove(&log_index);
            }
        }

        // Determine when the state machine should be ticked if no other events happen
        let mut next_tick = None;
        let mut schedule_tick = |maybe_timestamp| {
            if let Some(timestamp) = maybe_timestamp {
                if let Some(prev_timestamp) = next_tick {
                    if timestamp < prev_timestamp {
                        next_tick = Some(timestamp);
                    }
                } else {
                    next_tick = Some(timestamp);
                }
            }
        };

        schedule_tick(working.overlay.common.election_timeout);
        if let Role::Leader(leader_state) = working.role {
            for replication_state in leader_state.replication_state.values() {
                if !replication_state.waiting_on_storage {
                    schedule_tick(replication_state.retry_at);
                }
            }
        }

        Self { actions, next_tick }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Event<D> {
    Tick,
    LoadedLog(LoadedLogEvent<D>),
    ReceivedMessage(Message<D>),
    ClientRequest(ClientRequest<D>),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LoadedLogEvent<D> {
    pub entries: BTreeMap<LogIndex, Arc<Entry<D>>>,
}

// Actions will always be returned in this order
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Action<D> {
    TruncateLog(TruncateLogAction),
    ExtendLog(ExtendLogAction<D>),
    SaveState(HardState),
    SendMessage(Message<D>),
    FailedRequest(FailedRequest),
    ApplyLog(ApplyLogAction),
    LoadLog(LoadLogAction),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TruncateLogAction {
    pub last_log_index: LogIndex,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExtendLogAction<D> {
    pub entries: Vec<EntryFromRequest<D>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ApplyLogAction {
    pub up_to_log_index: LogIndex,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LoadLogAction {
    pub desired_entries: BTreeSet<LogIndex>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FailedRequest {
    pub request_id: RequestId,
    pub error: RequestError,
}
