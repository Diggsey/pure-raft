use maplit::btreeset;
use pretty_assertions::assert_eq;
use pure_raft::{
    Action, Event, InitialState, Input, LoadLogAction, LogIndex, NodeId, Output, State, Timestamp,
};

use crate::{default_config, two_node_bootstrap};

#[test]
fn two_node_append_timeout() {
    let mut state = State::<()>::new(NodeId(1), InitialState::default(), default_config());
    state.handle(two_node_bootstrap());
    let actual_output = state.handle(Input {
        timestamp: Timestamp(1000),
        event: Event::Tick,
    });

    let expected_output = Output {
        next_tick: None,
        actions: vec![Action::LoadLog(LoadLogAction {
            desired_entries: btreeset![LogIndex(1), LogIndex(2)],
        })],
    };

    assert_eq!(actual_output, expected_output);
}
