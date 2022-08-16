use serde::{Deserialize, Serialize};

use crate::types::{RequestId, Timestamp};

use self::{
    client_requests::ClientRequest, errors::RequestError, messages::Message,
    persistent_state::PersistentState,
};

pub mod client_requests;
pub mod errors;
pub mod messages;
pub mod persistent_state;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Input<D> {
    pub timestamp: Timestamp,
    pub persistent_state: PersistentState<D>,
    pub event: Event<D>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Output<D> {
    pub next_tick: Option<Timestamp>,
    pub persistent_state: PersistentState<D>,
    pub messages: Vec<Message<D>>,
    pub failed_requests: Vec<FailedRequest>,
}

impl<D> Default for Output<D> {
    fn default() -> Self {
        Self {
            next_tick: Default::default(),
            persistent_state: Default::default(),
            messages: Default::default(),
            failed_requests: Default::default(),
        }
    }
}

impl<D> Output<D> {
    pub fn schedule_tick(&mut self, timestamp: Timestamp) {
        if let Some(next_tick) = &mut self.next_tick {
            *next_tick = (*next_tick).min(timestamp);
        } else {
            self.next_tick = Some(timestamp);
        }
    }
    pub fn add_message(&mut self, message: Message<D>) {
        self.messages.push(message);
    }
    pub fn add_failed_request(&mut self, failed_request: FailedRequest) {
        self.failed_requests.push(failed_request)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FailedRequest {
    pub request_id: RequestId,
    pub error: RequestError,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Event<D> {
    Tick,
    StateChanged,
    Message(Message<D>),
    ClientRequest(ClientRequest),
}
