use std::sync::Arc;

use maplit::{btreemap, btreeset};
use pretty_assertions::assert_eq;
use pure_raft::{
    Action, AppendEntriesRequest, ApplyLogAction, BootstrapRequest, ClientRequest,
    ClientRequestPayload, DatabaseId, Entry, EntryFromRequest, EntryPayload, Event,
    ExtendLogAction, HardState, InitialState, Input, LogIndex, Membership, MembershipType, Message,
    MessagePayload, NodeId, Output, RequestId, State, Term, Timestamp,
};

use crate::default_config;

#[derive(Debug, Copy, Clone)]
struct Data(u32);

const DATABASE_ID: DatabaseId = DatabaseId(1);

#[test]
fn single_node() {
    let mut state = State::<()>::new(NodeId(1), InitialState::default(), default_config());
    let actual_output = state.handle(Input {
        timestamp: Timestamp(0),
        event: Event::ClientRequest(ClientRequest {
            request_id: Some(RequestId(1)),
            payload: ClientRequestPayload::Bootstrap(BootstrapRequest {
                database_id: DATABASE_ID,
                voter_ids: btreeset![NodeId(1)],
                learner_ids: btreeset![],
            }),
        }),
    });

    let expected_output = Output {
        next_tick: None,
        actions: vec![
            Action::ExtendLog(ExtendLogAction {
                entries: vec![
                    EntryFromRequest {
                        request_id: None,
                        entry: Arc::new(Entry {
                            term: Term(0),
                            payload: EntryPayload::Blank,
                        }),
                    },
                    EntryFromRequest {
                        request_id: Some(RequestId(1)),
                        entry: Arc::new(Entry {
                            term: Term(0),
                            payload: EntryPayload::MembershipChange(Membership {
                                nodes: btreemap! {
                                    NodeId(1) => MembershipType::VOTER,
                                },
                            }),
                        }),
                    },
                ],
            }),
            Action::SaveState(HardState {
                database_id: DATABASE_ID,
                current_term: Term(0),
                voted_for: Some(NodeId(1)),
            }),
            Action::ApplyLog(ApplyLogAction {
                up_to_log_index: LogIndex(2),
            }),
        ],
    };

    assert_eq!(actual_output, expected_output);
}

#[test]
fn two_node() {
    let mut state = State::<()>::new(NodeId(1), InitialState::default(), default_config());
    let actual_output = state.handle(Input {
        timestamp: Timestamp(0),
        event: Event::ClientRequest(ClientRequest {
            request_id: Some(RequestId(1)),
            payload: ClientRequestPayload::Bootstrap(BootstrapRequest {
                database_id: DATABASE_ID,
                voter_ids: btreeset![NodeId(1), NodeId(2)],
                learner_ids: btreeset![],
            }),
        }),
    });

    let expected_log_entries = vec![
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

    let expected_output = Output {
        next_tick: Some(Timestamp(1000)),
        actions: vec![
            Action::ExtendLog(ExtendLogAction {
                entries: vec![
                    EntryFromRequest {
                        request_id: None,
                        entry: expected_log_entries[0].clone(),
                    },
                    EntryFromRequest {
                        request_id: Some(RequestId(1)),
                        entry: expected_log_entries[1].clone(),
                    },
                ],
            }),
            Action::SaveState(HardState {
                database_id: DATABASE_ID,
                current_term: Term(0),
                voted_for: Some(NodeId(1)),
            }),
            Action::SendMessage(Message {
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
            }),
        ],
    };

    assert_eq!(actual_output, expected_output);
}

#[test]
fn single_node_with_learner() {
    let mut state = State::<()>::new(NodeId(1), InitialState::default(), default_config());
    let actual_output = state.handle(Input {
        timestamp: Timestamp(0),
        event: Event::ClientRequest(ClientRequest {
            request_id: Some(RequestId(1)),
            payload: ClientRequestPayload::Bootstrap(BootstrapRequest {
                database_id: DATABASE_ID,
                voter_ids: btreeset![NodeId(1)],
                learner_ids: btreeset![NodeId(2)],
            }),
        }),
    });

    let expected_log_entries = vec![
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

    let expected_output = Output {
        next_tick: Some(Timestamp(1000)),
        actions: vec![
            Action::ExtendLog(ExtendLogAction {
                entries: vec![
                    EntryFromRequest {
                        request_id: None,
                        entry: expected_log_entries[0].clone(),
                    },
                    EntryFromRequest {
                        request_id: Some(RequestId(1)),
                        entry: expected_log_entries[1].clone(),
                    },
                ],
            }),
            Action::SaveState(HardState {
                database_id: DATABASE_ID,
                current_term: Term(0),
                voted_for: Some(NodeId(1)),
            }),
            Action::SendMessage(Message {
                from_id: NodeId(1),
                to_id: NodeId(2),
                payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                    database_id: DATABASE_ID,
                    term: Term(0),
                    prev_log_index: LogIndex(0),
                    prev_log_term: Term(0),
                    entries: expected_log_entries,
                    leader_commit: LogIndex(2),
                }),
            }),
            Action::ApplyLog(ApplyLogAction {
                up_to_log_index: LogIndex(2),
            }),
        ],
    };

    assert_eq!(actual_output, expected_output);
}

#[test]
fn three_node() {
    let mut state = State::<()>::new(NodeId(1), InitialState::default(), default_config());
    let actual_output = state.handle(Input {
        timestamp: Timestamp(0),
        event: Event::ClientRequest(ClientRequest {
            request_id: Some(RequestId(1)),
            payload: ClientRequestPayload::Bootstrap(BootstrapRequest {
                database_id: DATABASE_ID,
                voter_ids: btreeset![NodeId(1), NodeId(2), NodeId(3)],
                learner_ids: btreeset![],
            }),
        }),
    });

    let expected_log_entries = vec![
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
                    NodeId(3) => MembershipType::VOTER,
                },
            }),
        }),
    ];

    let expected_output = Output {
        next_tick: Some(Timestamp(1000)),
        actions: vec![
            Action::ExtendLog(ExtendLogAction {
                entries: vec![
                    EntryFromRequest {
                        request_id: None,
                        entry: expected_log_entries[0].clone(),
                    },
                    EntryFromRequest {
                        request_id: Some(RequestId(1)),
                        entry: expected_log_entries[1].clone(),
                    },
                ],
            }),
            Action::SaveState(HardState {
                database_id: DATABASE_ID,
                current_term: Term(0),
                voted_for: Some(NodeId(1)),
            }),
            Action::SendMessage(Message {
                from_id: NodeId(1),
                to_id: NodeId(2),
                payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                    database_id: DATABASE_ID,
                    term: Term(0),
                    prev_log_index: LogIndex(0),
                    prev_log_term: Term(0),
                    entries: expected_log_entries.clone(),
                    leader_commit: LogIndex(0),
                }),
            }),
            Action::SendMessage(Message {
                from_id: NodeId(1),
                to_id: NodeId(3),
                payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                    database_id: DATABASE_ID,
                    term: Term(0),
                    prev_log_index: LogIndex(0),
                    prev_log_term: Term(0),
                    entries: expected_log_entries,
                    leader_commit: LogIndex(0),
                }),
            }),
        ],
    };

    assert_eq!(actual_output, expected_output);
}
