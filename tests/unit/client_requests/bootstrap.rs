use std::sync::Arc;

use maplit::{btreemap, btreeset};
use pretty_assertions::assert_eq;
use pure_raft::{
    Action, AppendEntriesRequest, ApplyLogAction, BootstrapError, BootstrapRequest, ClientRequest,
    ClientRequestPayload, Entry, EntryFromRequest, EntryPayload, Event, ExtendLogAction,
    FailedRequest, HardState, InitialState, Input, LogIndex, Membership, MembershipType, Message,
    MessagePayload, NodeId, Output, RequestError, RequestId, State, Term, Timestamp,
};

use crate::{
    default_config, single_node_bootstrap, single_node_with_learner_bootstrap,
    three_node_bootstrap, two_node_bootstrap, DATABASE_ID,
};

#[test]
fn single_node() {
    let mut state = State::<()>::new(NodeId(1), InitialState::default(), default_config());
    let actual_output = state.handle(single_node_bootstrap());

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
    let actual_output = state.handle(two_node_bootstrap());

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
    let actual_output = state.handle(single_node_with_learner_bootstrap());

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
    let actual_output = state.handle(three_node_bootstrap());

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

#[test]
fn bad_non_voter() {
    let mut state = State::<()>::new(NodeId(1), InitialState::default(), default_config());
    let actual_output = state.handle(Input {
        timestamp: Timestamp(0),
        event: Event::ClientRequest(ClientRequest {
            request_id: Some(RequestId(1)),
            payload: ClientRequestPayload::Bootstrap(BootstrapRequest {
                database_id: DATABASE_ID,
                voter_ids: btreeset![NodeId(2)],
                learner_ids: btreeset![NodeId(1)],
            }),
        }),
    });

    let expected_output = Output {
        next_tick: None,
        actions: vec![Action::FailedRequest(FailedRequest {
            request_id: RequestId(1),
            error: RequestError::Bootstrap(BootstrapError::ThisNodeMustBeVoter),
        })],
    };

    assert_eq!(actual_output, expected_output);
}

#[test]
fn bad_double_bootstrap() {
    let mut state = State::<()>::new(NodeId(1), InitialState::default(), default_config());
    state.handle(single_node_bootstrap());
    let actual_output = state.handle(Input {
        timestamp: Timestamp(0),
        event: Event::ClientRequest(ClientRequest {
            request_id: Some(RequestId(2)),
            payload: ClientRequestPayload::Bootstrap(BootstrapRequest {
                database_id: DATABASE_ID,
                voter_ids: btreeset![NodeId(1)],
                learner_ids: btreeset![],
            }),
        }),
    });

    let expected_output = Output {
        next_tick: None,
        actions: vec![Action::FailedRequest(FailedRequest {
            request_id: RequestId(2),
            error: RequestError::Bootstrap(BootstrapError::ClusterAlreadyInitialized),
        })],
    };

    assert_eq!(actual_output, expected_output);
}
