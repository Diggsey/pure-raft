use crate::election_state::ElectionState;

use super::leader::LeaderState;

pub enum Role {
    Learner,
    Follower,
    Applicant(ElectionState),
    Candidate(ElectionState),
    Leader(LeaderState),
}

impl Role {
    pub fn is_learner(&self) -> bool {
        matches!(self, Self::Learner)
    }
    pub fn is_leader(&self) -> bool {
        matches!(self, Self::Leader(_))
    }
}
