use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::NodeId;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum RequestError {
    NotLeader,
    Busy,
    Bootstrap(BootstrapError),
    SetMembers(SetMembersError),
    SetLearners(SetLearnersError),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum BootstrapError {
    ClusterAlreadyInitialized,
    ThisNodeMustBeVoter,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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
pub enum SetLearnersError {
    AlreadyChanging,
    ExistingMembers { member_ids: BTreeSet<NodeId> },
}
