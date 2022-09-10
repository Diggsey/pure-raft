use maplit::btreemap;
use pretty_assertions::assert_eq;
use pure_raft::{
    Action, AppendEntriesRequest, Event, InitialState, Input, LoadedLogEvent, LogIndex, Message,
    MessagePayload, NodeId, Output, State, Term, Timestamp,
};

use crate::{default_config, two_node_bootstrap, two_node_entries, DATABASE_ID};

#[test]
fn two_node_append_timeout() {
    let mut state = State::<()>::new(NodeId(1), InitialState::default(), default_config());
    state.handle(two_node_bootstrap());
    state.handle(Input {
        timestamp: Timestamp(1000),
        event: Event::Tick,
    });

    let expected_log_entries = two_node_entries();

    let actual_output = state.handle(Input {
        timestamp: Timestamp(1010),
        event: Event::LoadedLog(LoadedLogEvent {
            entries: btreemap! {
                LogIndex(1) => expected_log_entries[0].clone(),
                LogIndex(2) => expected_log_entries[1].clone(),
            },
        }),
    });

    let expected_output = Output {
        next_tick: Some(Timestamp(2010)),
        actions: vec![Action::SendMessage(Message {
            from_id: NodeId(1),
            to_id: NodeId(2),
            payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                database_id: DATABASE_ID,
                term: Term(0),
                prev_log_index: LogIndex(0),
                prev_log_term: Term(0),
                entries: expected_log_entries,
                leader_commit: LogIndex(0),
            }),
        })],
        errors: Vec::new(),
    };

    assert_eq!(actual_output, expected_output);
}
