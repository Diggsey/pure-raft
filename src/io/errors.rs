use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::NodeId;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StateError {
    DatabaseMismatch,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum RequestError {
    NotLeader,
    Busy,
    Conflict,
    Bootstrap(BootstrapError),
    SetMembers(SetMembersError),
    SetLearners(SetLearnersError),
    State(StateError),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum BootstrapError {
    ClusterAlreadyInitialized,
    ThisNodeMustBeVoter,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum SetMembersError {
    AlreadyChanging,
    InvalidMembers,
    InsufficientFaultTolerance {
        proposed_ids: Option<BTreeSet<NodeId>>,
    },
    TooManyLaggingMembers {
        lagging_ids: BTreeSet<NodeId>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum SetLearnersError {
    AlreadyChanging,
    ExistingMembers { member_ids: BTreeSet<NodeId> },
}
