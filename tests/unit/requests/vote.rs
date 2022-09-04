use pretty_assertions::assert_eq;
use pure_raft::{
    Action, Event, HardState, InitialState, Input, LogIndex, Message, MessagePayload, NodeId,
    PreVoteRequest, State, Term, Timestamp, VoteRequest, VoteResponse,
};

use crate::{adopted, default_config, three_node_entries, DATABASE_ID};

#[test]
fn three_node_leader_timeout() {
    let config = default_config();
    let mut state = State::<()>::new(NodeId(2), InitialState::default(), config.clone());
    let output = state.handle(adopted(Term(0), LogIndex(0), &three_node_entries()));
    let next_tick = output.next_tick.unwrap();
    state.handle(Input {
        timestamp: next_tick,
        event: Event::Tick,
    });
    let timestamp = Timestamp(0) + config.max_election_timeout;
    state.handle(Input {
        timestamp,
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(3),
            to_id: NodeId(2),
            payload: MessagePayload::PreVoteRequest(PreVoteRequest {
                database_id: DATABASE_ID,
                next_term: Term(1),
                candidate_id: NodeId(3),
                last_log_index: LogIndex(2),
                last_log_term: Term(0),
            }),
        }),
    });
    let actual_output = state.handle(Input {
        timestamp,
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(3),
            to_id: NodeId(2),
            payload: MessagePayload::VoteRequest(VoteRequest {
                database_id: DATABASE_ID,
                term: Term(1),
                candidate_id: NodeId(3),
                last_log_index: LogIndex(2),
                last_log_term: Term(0),
            }),
        }),
    });

    assert!(actual_output.next_tick >= Some(timestamp + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(timestamp + config.max_election_timeout));

    let expected_actions = vec![
        Action::SaveState(HardState {
            database_id: DATABASE_ID,
            current_term: Term(1),
            voted_for: Some(NodeId(3)),
        }),
        Action::SendMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(3),
            payload: MessagePayload::VoteResponse(VoteResponse {
                term: Term(1),
                vote_granted: true,
            }),
        }),
    ];

    assert_eq!(actual_output.actions, expected_actions);
}
