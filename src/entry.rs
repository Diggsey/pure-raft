use std::{fmt::Debug, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{membership::Membership, types::Term, RequestId};

pub trait AppData: Clone + Debug {}

/// A Raft log entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry<D> {
    /// This entry's term.
    pub term: Term,
    /// This entry's payload.
    pub payload: EntryPayload<D>,
}

impl<D> Entry<D> {
    pub fn is_membership_change(&self) -> bool {
        if let EntryPayload::MembershipChange(_) = self.payload {
            true
        } else {
            false
        }
    }
}

/// Log entry payload variants.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum EntryPayload<D> {
    /// An empty payload committed by a new cluster leader.
    Blank,
    /// A normal log entry.
    Application(D),
    /// A membership change log entry.
    MembershipChange(Membership),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EntryFromRequest<D> {
    pub request_id: Option<RequestId>,
    pub entry: Arc<Entry<D>>,
}
