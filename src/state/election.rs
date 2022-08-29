use std::collections::HashSet;

use crate::membership::{Membership, MembershipType};
use crate::types::NodeId;

pub struct ElectionState {
    membership: Membership,
    votes_received: HashSet<NodeId>,
    pub(crate) has_majority: bool,
}

impl ElectionState {
    fn check_majority(&self, is_voter_fn: impl Fn(&MembershipType) -> bool) -> bool {
        let mut total_count = 0;
        let mut vote_count = 0;
        for (node_id, type_) in &self.membership.nodes {
            if is_voter_fn(type_) {
                total_count += 1;
                if self.votes_received.contains(node_id) {
                    vote_count += 1;
                }
            }
        }
        vote_count > total_count / 2
    }
    // Returns true if we gained a majority
    pub(crate) fn add_vote(&mut self, from: NodeId) {
        if self.votes_received.insert(from) && !self.has_majority {
            self.has_majority = self.check_majority(|mt| mt.is_voter_prev)
                && self.check_majority(|mt| mt.is_voter_next);
        }
    }
    pub(crate) fn new(membership: Membership) -> Self {
        Self {
            membership,
            votes_received: HashSet::new(),
            has_majority: false,
        }
    }
}
