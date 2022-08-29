use crate::{
    io::{persistent_state::InitialState, Input, Output},
    types::NodeId,
};

use self::{common::CommonState, config::Config, role::Role, working::WorkingState};

mod common;
pub mod config;
mod election;
mod leader;
pub mod overlay;
mod replication;
pub mod role;
pub mod working;

pub struct State<D> {
    pub config: Config,
    pub common: CommonState<D>,
    pub role: Role,
}

#[derive(Debug)]
pub enum Error {
    DatabaseMismatch,
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
}
