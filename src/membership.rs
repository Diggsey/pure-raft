use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::types::NodeId;

#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct MembershipType {
    pub is_voter_prev: bool,
    pub is_voter_next: bool,
}

impl MembershipType {
    pub const LEARNER: Self = Self {
        is_voter_prev: false,
        is_voter_next: false,
    };
    pub const VOTER: Self = Self {
        is_voter_prev: true,
        is_voter_next: true,
    };
    pub fn is_changing(&self) -> bool {
        self.is_voter_prev != self.is_voter_next
    }
    pub(crate) fn begin_change(&mut self, is_voter: bool) {
        assert!(!self.is_changing(), "Change has already begun");
        self.is_voter_next = is_voter;
    }
    pub(crate) fn complete_change(&mut self) {
        self.is_voter_prev = self.is_voter_next;
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub struct Membership {
    pub nodes: BTreeMap<NodeId, MembershipType>,
}

impl Membership {
    pub(crate) fn empty() -> Self {
        Self::default()
    }
    pub(crate) fn bootstrap(
        voter_ids: impl IntoIterator<Item = NodeId>,
        learner_ids: impl IntoIterator<Item = NodeId>,
    ) -> Self {
        let mut res = Self::empty();
        res.nodes.extend(
            learner_ids
                .into_iter()
                .map(|node_id| (node_id, MembershipType::LEARNER)),
        );
        res.nodes.extend(
            voter_ids
                .into_iter()
                .map(|node_id| (node_id, MembershipType::VOTER)),
        );
        res
    }
    pub fn is_changing(&self) -> bool {
        self.nodes.values().any(MembershipType::is_changing)
    }
    /// Should only be called when in joint consensus
    pub(crate) fn complete_change(&mut self) {
        self.nodes
            .values_mut()
            .for_each(MembershipType::complete_change);
    }
    /// Should only be called when not in joint consensus
    pub(crate) fn begin_change(&mut self, new_members: BTreeSet<NodeId>) {
        for (k, v) in &mut self.nodes {
            v.begin_change(new_members.contains(k));
        }
    }
    pub(crate) fn set_learners(&mut self, learners: BTreeSet<NodeId>) {
        self.nodes
            .retain(|_, membership_type| *membership_type != MembershipType::LEARNER);
        self.nodes.extend(
            learners
                .into_iter()
                .map(|node_id| (node_id, MembershipType::LEARNER)),
        );
    }
    pub(crate) fn is_learner_or_unknown(&self, node_id: NodeId) -> bool {
        self.nodes.get(&node_id).copied().unwrap_or_default() == MembershipType::LEARNER
    }
}
