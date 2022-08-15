use serde::{Deserialize, Serialize};

use crate::{membership::Membership, types::Term};

/// A Raft log entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    /// This entry's term.
    pub term: Term,
    /// This entry's payload.
    pub payload: EntryPayload,
}

impl Entry {
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
pub enum EntryPayload {
    /// An empty payload committed by a new cluster leader.
    Blank,
    /// A normal log entry.
    Application(EntryNormal),
    /// A membership change log entry.
    MembershipChange(Membership),
}

/// A normal log entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EntryNormal {
    /// The contents of this entry.
    pub data: (),
}
