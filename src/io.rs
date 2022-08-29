use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{
    types::{RequestId, Timestamp},
    Entry, EntryFromRequest, HardState, LogIndex,
};

use self::{client_requests::ClientRequest, errors::RequestError, messages::Message};

pub mod client_requests;
pub mod errors;
pub mod initial_state;
pub mod messages;

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
