use std::sync::Arc;

use maplit::btreemap;
use pretty_assertions::assert_eq;
use pure_raft::{
    Action, AppendEntriesRequest, AppendEntriesResponse, ApplyLogAction, DatabaseId, Entry,
    EntryFromRequest, EntryPayload, Event, ExtendLogAction, HardState, InitialState, Input,
    LogIndex, Membership, MembershipType, Message, MessagePayload, NodeId, State, Term, Timestamp,
};

use crate::default_config;

const DATABASE_ID: DatabaseId = DatabaseId(1);

#[test]
fn two_node() {
    const BASE_TIMESTAMP: Timestamp = Timestamp(10);

    let config = default_config();
    let log_entries = vec![
        Arc::new(Entry {
            term: Term(0),
            payload: EntryPayload::Blank,
        }),
        Arc::new(Entry {
            term: Term(0),
            payload: EntryPayload::MembershipChange(Membership {
                nodes: btreemap! {
                    NodeId(1) => MembershipType::VOTER,
                    NodeId(2) => MembershipType::VOTER,
                },
            }),
        }),
    ];

    let mut state = State::<()>::new(NodeId(2), InitialState::default(), config.clone());
    let actual_output = state.handle(Input {
        timestamp: BASE_TIMESTAMP,
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(1),
            to_id: NodeId(2),
            payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                database_id: DATABASE_ID,
                term: Term(0),
                prev_log_index: LogIndex(0),
                prev_log_term: Term(0),
                entries: log_entries.clone(),
                leader_commit: LogIndex(0),
            }),
        }),
    });

    assert!(actual_output.next_tick >= Some(BASE_TIMESTAMP + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(BASE_TIMESTAMP + config.max_election_timeout));

    let expected_actions = vec![
        Action::ExtendLog(ExtendLogAction {
            entries: vec![
                EntryFromRequest {
                    request_id: None,
                    entry: log_entries[0].clone(),
                },
                EntryFromRequest {
                    request_id: None,
                    entry: log_entries[1].clone(),
                },
            ],
        }),
        Action::SaveState(HardState {
            database_id: DATABASE_ID,
            current_term: Term(0),
            voted_for: None,
        }),
        Action::SendMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(1),
            payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(0),
                match_index: Some(LogIndex(2)),
                conflict_opt: None,
            }),
        }),
    ];

    assert_eq!(actual_output.actions, expected_actions);
}

#[test]
fn single_node_with_learner() {
    const BASE_TIMESTAMP: Timestamp = Timestamp(10);

    let config = default_config();
    let log_entries = vec![
        Arc::new(Entry {
            term: Term(0),
            payload: EntryPayload::Blank,
        }),
        Arc::new(Entry {
            term: Term(0),
            payload: EntryPayload::MembershipChange(Membership {
                nodes: btreemap! {
                    NodeId(1) => MembershipType::VOTER,
                    NodeId(2) => MembershipType::LEARNER,
                },
            }),
        }),
    ];

    let mut state = State::<()>::new(NodeId(2), InitialState::default(), config.clone());
    let actual_output = state.handle(Input {
        timestamp: BASE_TIMESTAMP,
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(1),
            to_id: NodeId(2),
            payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                database_id: DATABASE_ID,
                term: Term(0),
                prev_log_index: LogIndex(0),
                prev_log_term: Term(0),
                entries: log_entries.clone(),
                leader_commit: LogIndex(2),
            }),
        }),
    });

    assert_eq!(actual_output.next_tick, None);

    let expected_actions = vec![
        Action::ExtendLog(ExtendLogAction {
            entries: vec![
                EntryFromRequest {
                    request_id: None,
                    entry: log_entries[0].clone(),
                },
                EntryFromRequest {
                    request_id: None,
                    entry: log_entries[1].clone(),
                },
            ],
        }),
        Action::SaveState(HardState {
            database_id: DATABASE_ID,
            current_term: Term(0),
            voted_for: None,
        }),
        Action::SendMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(1),
            payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(0),
                match_index: Some(LogIndex(2)),
                conflict_opt: None,
            }),
        }),
        Action::ApplyLog(ApplyLogAction {
            up_to_log_index: LogIndex(2),
        }),
    ];

    assert_eq!(actual_output.actions, expected_actions);
}
