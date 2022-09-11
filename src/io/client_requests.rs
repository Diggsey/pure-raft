use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{DatabaseId, NodeId, RequestId, Snapshot};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClientRequest<D> {
    pub request_id: Option<RequestId>,
    pub payload: ClientRequestPayload<D>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ClientRequestPayload<D> {
    Bootstrap(BootstrapRequest),
    Application(D),
    SetMembers(SetMembersRequest),
    SetLearners(SetLearnersRequest),
    InstallSnapshot(InstallSnapshotRequest),
    FailedToDownloadSnapshot,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BootstrapRequest {
    pub database_id: DatabaseId,
    pub voter_ids: BTreeSet<NodeId>,
    pub learner_ids: BTreeSet<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SetMembersRequest {
    pub fault_tolerance: u64,
    pub member_ids: BTreeSet<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SetLearnersRequest {
    pub learner_ids: BTreeSet<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InstallSnapshotRequest {
    pub snapshot: Snapshot,
    /// True if this completes a previously requested snapshot
    /// download.
    pub was_downloaded: bool,
}
