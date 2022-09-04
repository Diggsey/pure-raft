use std::sync::Arc;

use pretty_assertions::assert_eq;
use pure_raft::{
    Action, AppendEntriesRequest, Entry, EntryFromRequest, EntryPayload, Event, ExtendLogAction,
    InitialState, Input, LogIndex, Message, MessagePayload, NodeId, PreVoteResponse, State, Term,
    VoteResponse,
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
    state.handle(Input {
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
    let actual_output = state.handle(Input {
        timestamp: next_tick,
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(3),
            to_id: NodeId(2),
            payload: MessagePayload::VoteResponse(VoteResponse {
                term: Term(1),
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
    state.handle(Input {
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
    let actual_output = state.handle(Input {
        timestamp: next_tick,
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(3),
            to_id: NodeId(2),
            payload: MessagePayload::VoteResponse(VoteResponse {
                term: Term(1),
                vote_granted: true,
            }),
        }),
    });

    assert_eq!(
        actual_output.next_tick,
        Some(next_tick + config.heartbeat_interval)
    );

    let log_entries = vec![Arc::new(Entry {
        term: Term(1),
        payload: EntryPayload::Blank,
    })];

    let expected_actions = vec![
        Action::ExtendLog(ExtendLogAction {
            entries: vec![EntryFromRequest {
                request_id: None,
                entry: log_entries[0].clone(),
            }],
        }),
        Action::SendMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(1),
            payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                database_id: DATABASE_ID,
                term: Term(1),
                prev_log_index: LogIndex(2),
                prev_log_term: Term(0),
                entries: log_entries.clone(),
                leader_commit: LogIndex(0),
            }),
        }),
        Action::SendMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(3),
            payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                database_id: DATABASE_ID,
                term: Term(1),
                prev_log_index: LogIndex(2),
                prev_log_term: Term(0),
                entries: log_entries.clone(),
                leader_commit: LogIndex(0),
            }),
        }),
    ];

    assert_eq!(actual_output.actions, expected_actions);
}
