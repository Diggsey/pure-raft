use crate::{
    io::{initial_state::InitialState, Input, Output},
    types::NodeId,
    Config, LogIndex, Membership, Term,
};

use self::{common::CommonState, role::Role, working::WorkingState};

mod common;
mod election;
mod leader;
mod overlay;
mod replication;
mod role;
mod working;

pub struct State<D> {
    config: Config,
    common: CommonState<D>,
    role: Role,
}

impl<D> State<D> {
    pub fn new(this_id: NodeId, initial_state: InitialState, config: Config) -> Self {
        Self {
            config,
            common: CommonState::new(this_id, initial_state),
            role: Role::Learner,
        }
    }
    pub fn handle(&mut self, input: Input<D>) -> Output<D> {
        let mut working_state = WorkingState::new(self, input.timestamp);
        working_state.handle(input.event);

        working_state.into()
    }
    pub fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader(_))
    }
    pub fn last_applied_log_index(&self) -> LogIndex {
        self.common.last_applied_log_index
    }
    pub fn last_applied_log_term(&self) -> Term {
        self.common.last_applied_log_term
    }
    pub fn last_applied_membership(&self) -> &Membership {
        &self.common.last_applied_membership
    }
}
