use maplit::btreeset;
use pretty_assertions::assert_eq;
use pure_raft::{
    Action, Event, InitialState, Input, LoadLogAction, LogIndex, Message, MessagePayload, NodeId,
    Output, PreVoteRequest, State, Term, Timestamp,
};

use crate::{
    adopted, default_config, three_node_entries, two_node_bootstrap, two_node_entries, DATABASE_ID,
};

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
        errors: Vec::new(),
    };

    assert_eq!(actual_output, expected_output);
}

#[test]
fn two_node_leader_timeout() {
    let mut state = State::<()>::new(NodeId(2), InitialState::default(), default_config());
    let output = state.handle(adopted(Term(0), LogIndex(0), &two_node_entries()));
    let actual_output = state.handle(Input {
        timestamp: output.next_tick.unwrap(),
        event: Event::Tick,
    });

    let expected_actions = vec![Action::SendMessage(Message {
        from_id: NodeId(2),
        to_id: NodeId(1),
        payload: MessagePayload::PreVoteRequest(PreVoteRequest {
            database_id: DATABASE_ID,
            next_term: Term(1),
            candidate_id: NodeId(2),
            last_log_index: LogIndex(2),
            last_log_term: Term(0),
        }),
    })];

    assert_eq!(actual_output.actions, expected_actions);
}

#[test]
fn three_node_leader_timeout() {
    let config = default_config();
    let mut state = State::<()>::new(NodeId(2), InitialState::default(), config.clone());
    let output = state.handle(adopted(Term(0), LogIndex(0), &three_node_entries()));
    let next_tick = output.next_tick.unwrap();

    let actual_output = state.handle(Input {
        timestamp: next_tick,
        event: Event::Tick,
    });

    let expected_actions = vec![
        Action::SendMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(1),
            payload: MessagePayload::PreVoteRequest(PreVoteRequest {
                database_id: DATABASE_ID,
                next_term: Term(1),
                candidate_id: NodeId(2),
                last_log_index: LogIndex(2),
                last_log_term: Term(0),
            }),
        }),
        Action::SendMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(3),
            payload: MessagePayload::PreVoteRequest(PreVoteRequest {
                database_id: DATABASE_ID,
                next_term: Term(1),
                candidate_id: NodeId(2),
                last_log_index: LogIndex(2),
                last_log_term: Term(0),
            }),
        }),
    ];

    assert!(actual_output.next_tick >= Some(next_tick + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(next_tick + config.max_election_timeout));

    assert_eq!(actual_output.actions, expected_actions);
}
