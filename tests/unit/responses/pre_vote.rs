use pretty_assertions::assert_eq;
use pure_raft::{
    Action, Event, HardState, InitialState, Input, LogIndex, Message, MessagePayload, NodeId,
    PreVoteResponse, State, Term, VoteRequest,
};

use crate::{adopted, default_config, three_node_entries, DATABASE_ID};

#[test]
fn three_node_leader_timeout_denied() {
    let config = default_config();
    let mut state = State::<()>::new(NodeId(2), InitialState::default(), config.clone());
    let output = state.handle(adopted(Term(0), LogIndex(0), &three_node_entries()));
    let next_tick = output.next_tick.unwrap();
    state.handle(Input {
        timestamp: next_tick,
        event: Event::Tick,
    });
    let actual_output = state.handle(Input {
        timestamp: next_tick,
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(3),
            to_id: NodeId(2),
            payload: MessagePayload::PreVoteResponse(PreVoteResponse {
                term: Term(0),
                vote_granted: false,
            }),
        }),
    });

    assert!(actual_output.next_tick >= Some(next_tick + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(next_tick + config.max_election_timeout));
    assert_eq!(actual_output.actions, vec![]);
}

#[test]
fn three_node_leader_timeout_granted() {
    let config = default_config();
    let mut state = State::<()>::new(NodeId(2), InitialState::default(), config.clone());
    let output = state.handle(adopted(Term(0), LogIndex(0), &three_node_entries()));
    let next_tick = output.next_tick.unwrap();
    state.handle(Input {
        timestamp: next_tick,
        event: Event::Tick,
    });
    let actual_output = state.handle(Input {
        timestamp: next_tick,
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(3),
            to_id: NodeId(2),
            payload: MessagePayload::PreVoteResponse(PreVoteResponse {
                term: Term(0),
                vote_granted: true,
            }),
        }),
    });

    assert!(actual_output.next_tick >= Some(next_tick + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(next_tick + config.max_election_timeout));

    let expected_actions = vec![
        Action::SaveState(HardState {
            database_id: DATABASE_ID,
            current_term: Term(1),
            voted_for: Some(NodeId(2)),
        }),
        Action::SendMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(1),
            payload: MessagePayload::VoteRequest(VoteRequest {
                database_id: DATABASE_ID,
                term: Term(1),
                candidate_id: NodeId(2),
                last_log_index: LogIndex(2),
                last_log_term: Term(0),
            }),
        }),
        Action::SendMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(3),
            payload: MessagePayload::VoteRequest(VoteRequest {
                database_id: DATABASE_ID,
                term: Term(1),
                candidate_id: NodeId(2),
                last_log_index: LogIndex(2),
                last_log_term: Term(0),
            }),
        }),
    ];

    assert_eq!(actual_output.actions, expected_actions);
}
