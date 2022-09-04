use std::sync::Arc;

use pretty_assertions::assert_eq;
use pure_raft::{
    Action, AppendEntriesRequest, AppendEntriesResponse, ApplyLogAction, ConflictOpt, Entry,
    EntryFromRequest, EntryPayload, Event, ExtendLogAction, HardState, InitialState, Input,
    LogIndex, Message, MessagePayload, NodeId, State, Term, Timestamp, TruncateLogAction,
};

use crate::{
    adopted, default_config, single_node_with_learner_entries, three_node_entries,
    two_node_entries, DATABASE_ID,
};

#[test]
fn two_node() {
    let config = default_config();
    let log_entries = two_node_entries();

    let mut state = State::<()>::new(NodeId(2), InitialState::default(), config.clone());
    let actual_output = state.handle(adopted(Term(0), LogIndex(0), &log_entries));

    assert!(actual_output.next_tick >= Some(Timestamp(0) + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(Timestamp(0) + config.max_election_timeout));

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
    let config = default_config();
    let log_entries = single_node_with_learner_entries();

    let mut state = State::<()>::new(NodeId(2), InitialState::default(), config.clone());
    let actual_output = state.handle(adopted(Term(0), LogIndex(2), &log_entries));

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

#[test]
fn three_node_old_term() {
    // In this test, the cluster was bootstrapped on node 3, whilst
    // our node (node 2) was disconnected. The bootstrapping was
    // received by node 1 and committed by both nodes. Node 3 failed
    // temporarily, and node 1 took its place as leader. Node 2 receives
    // an "append entries" request from both node 1 and node 3, with the
    // request from the new leader arriving first. We expect the other
    // request to be rejected as it's from an older term.
    let config = default_config();
    let mut log_entries = three_node_entries();
    log_entries.push(Arc::new(Entry {
        term: Term(1),
        payload: EntryPayload::Blank,
    }));

    let mut state = State::<()>::new(NodeId(2), InitialState::default(), config.clone());
    let actual_output = state.handle(adopted(Term(1), LogIndex(1), &log_entries));

    assert!(actual_output.next_tick >= Some(Timestamp(0) + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(Timestamp(0) + config.max_election_timeout));

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
                EntryFromRequest {
                    request_id: None,
                    entry: log_entries[2].clone(),
                },
            ],
        }),
        Action::SaveState(HardState {
            database_id: DATABASE_ID,
            current_term: Term(1),
            voted_for: None,
        }),
        Action::SendMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(1),
            payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(1),
                match_index: Some(LogIndex(3)),
                conflict_opt: None,
            }),
        }),
        Action::ApplyLog(ApplyLogAction {
            up_to_log_index: LogIndex(1),
        }),
    ];

    assert_eq!(actual_output.actions, expected_actions);

    let actual_output = state.handle(Input {
        timestamp: Timestamp(0),
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(3),
            to_id: NodeId(2),
            payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                database_id: DATABASE_ID,
                term: Term(0),
                prev_log_index: LogIndex(0),
                prev_log_term: Term(0),
                entries: log_entries[0..2].to_vec(),
                leader_commit: LogIndex(1),
            }),
        }),
    });

    assert!(actual_output.next_tick >= Some(Timestamp(0) + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(Timestamp(0) + config.max_election_timeout));

    let expected_actions = vec![Action::SendMessage(Message {
        from_id: NodeId(2),
        to_id: NodeId(3),
        payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
            term: Term(1),
            match_index: None,
            conflict_opt: None,
        }),
    })];

    assert_eq!(actual_output.actions, expected_actions);
}

#[test]
fn three_node_overlap() {
    // This test is the same as the one above, but the messages from
    // nodes (1) and (3) arrived in the opposite order.
    let config = default_config();
    let mut log_entries = three_node_entries();
    log_entries.push(Arc::new(Entry {
        term: Term(1),
        payload: EntryPayload::Blank,
    }));

    let mut state = State::<()>::new(NodeId(2), InitialState::default(), config.clone());
    let actual_output = state.handle(adopted(Term(0), LogIndex(1), &log_entries[0..2]));

    assert!(actual_output.next_tick >= Some(Timestamp(0) + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(Timestamp(0) + config.max_election_timeout));

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
            up_to_log_index: LogIndex(1),
        }),
    ];

    assert_eq!(actual_output.actions, expected_actions);

    let actual_output = state.handle(Input {
        timestamp: Timestamp(0),
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(3),
            to_id: NodeId(2),
            payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                database_id: DATABASE_ID,
                term: Term(1),
                prev_log_index: LogIndex(0),
                prev_log_term: Term(0),
                entries: log_entries.clone(),
                leader_commit: LogIndex(1),
            }),
        }),
    });

    assert!(actual_output.next_tick >= Some(Timestamp(0) + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(Timestamp(0) + config.max_election_timeout));

    let expected_actions = vec![
        Action::ExtendLog(ExtendLogAction {
            entries: vec![EntryFromRequest {
                request_id: None,
                entry: log_entries[2].clone(),
            }],
        }),
        Action::SaveState(HardState {
            database_id: DATABASE_ID,
            current_term: Term(1),
            voted_for: None,
        }),
        Action::SendMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(3),
            payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(1),
                match_index: Some(LogIndex(3)),
                conflict_opt: None,
            }),
        }),
    ];

    assert_eq!(actual_output.actions, expected_actions);
}

#[test]
fn three_node_conflict() {
    // This test is the same as the one above, but one of the
    // entries conflicts.
    let config = default_config();
    let mut log_entries = three_node_entries();
    log_entries.push(Arc::new(Entry {
        term: Term(1),
        payload: EntryPayload::Blank,
    }));

    let mut state = State::<()>::new(NodeId(2), InitialState::default(), config.clone());
    let actual_output = state.handle(adopted(
        Term(0),
        LogIndex(1),
        log_entries[0..2].iter().chain([&Arc::new(Entry {
            term: Term(0),
            payload: EntryPayload::Blank,
        })]),
    ));

    assert!(actual_output.next_tick >= Some(Timestamp(0) + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(Timestamp(0) + config.max_election_timeout));

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
                EntryFromRequest {
                    request_id: None,
                    entry: Arc::new(Entry {
                        term: Term(0),
                        payload: EntryPayload::Blank,
                    }),
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
                match_index: Some(LogIndex(3)),
                conflict_opt: None,
            }),
        }),
        Action::ApplyLog(ApplyLogAction {
            up_to_log_index: LogIndex(1),
        }),
    ];

    assert_eq!(actual_output.actions, expected_actions);

    let actual_output = state.handle(Input {
        timestamp: Timestamp(0),
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(3),
            to_id: NodeId(2),
            payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                database_id: DATABASE_ID,
                term: Term(1),
                prev_log_index: LogIndex(0),
                prev_log_term: Term(0),
                entries: log_entries.clone(),
                leader_commit: LogIndex(1),
            }),
        }),
    });

    assert!(actual_output.next_tick >= Some(Timestamp(0) + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(Timestamp(0) + config.max_election_timeout));

    let expected_actions = vec![
        Action::TruncateLog(TruncateLogAction {
            last_log_index: LogIndex(2),
        }),
        Action::ExtendLog(ExtendLogAction {
            entries: vec![EntryFromRequest {
                request_id: None,
                entry: log_entries[2].clone(),
            }],
        }),
        Action::SaveState(HardState {
            database_id: DATABASE_ID,
            current_term: Term(1),
            voted_for: None,
        }),
        Action::SendMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(3),
            payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(1),
                match_index: Some(LogIndex(3)),
                conflict_opt: None,
            }),
        }),
    ];

    assert_eq!(actual_output.actions, expected_actions);
}

#[test]
fn single_node_with_learner_partial_resend() {
    let config = default_config();
    let log_entries = single_node_with_learner_entries();

    let mut state = State::<()>::new(NodeId(2), InitialState::default(), config.clone());
    state.handle(adopted(Term(0), LogIndex(2), &log_entries));
    let actual_output = state.handle(Input {
        timestamp: Timestamp(0),
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(1),
            to_id: NodeId(2),
            payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                database_id: DATABASE_ID,
                term: Term(0),
                prev_log_index: LogIndex(0),
                prev_log_term: Term(0),
                entries: log_entries[0..1].to_vec(),
                leader_commit: LogIndex(2),
            }),
        }),
    });

    assert_eq!(actual_output.next_tick, None);

    let expected_actions = vec![Action::SendMessage(Message {
        from_id: NodeId(2),
        to_id: NodeId(1),
        payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
            term: Term(0),
            match_index: Some(LogIndex(1)),
            conflict_opt: None,
        }),
    })];

    assert_eq!(actual_output.actions, expected_actions);
}

#[test]
fn two_nodes() {
    let config = default_config();
    let mut log_entries = two_node_entries();
    log_entries.push(Arc::new(Entry {
        term: Term(0),
        payload: EntryPayload::Application(()),
    }));

    let mut state = State::<()>::new(NodeId(2), InitialState::default(), config.clone());
    let actual_output = state.handle(adopted(Term(0), LogIndex(0), &log_entries[0..2]));

    assert!(actual_output.next_tick >= Some(Timestamp(0) + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(Timestamp(0) + config.max_election_timeout));

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

    let actual_output = state.handle(Input {
        timestamp: Timestamp(10),
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(1),
            to_id: NodeId(2),
            payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                database_id: DATABASE_ID,
                term: Term(0),
                prev_log_index: LogIndex(2),
                prev_log_term: Term(0),
                entries: log_entries[2..3].to_vec(),
                leader_commit: LogIndex(2),
            }),
        }),
    });

    assert!(actual_output.next_tick >= Some(Timestamp(10) + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(Timestamp(10) + config.max_election_timeout));

    let expected_actions = vec![
        Action::ExtendLog(ExtendLogAction {
            entries: vec![EntryFromRequest {
                request_id: None,
                entry: log_entries[2].clone(),
            }],
        }),
        Action::SendMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(1),
            payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(0),
                match_index: Some(LogIndex(3)),
                conflict_opt: None,
            }),
        }),
        Action::ApplyLog(ApplyLogAction {
            up_to_log_index: LogIndex(2),
        }),
    ];

    assert_eq!(actual_output.actions, expected_actions);
}

#[test]
fn two_nodes_overlap() {
    let config = default_config();
    let mut log_entries = two_node_entries();
    log_entries.push(Arc::new(Entry {
        term: Term(0),
        payload: EntryPayload::Application(()),
    }));

    let mut state = State::<()>::new(NodeId(2), InitialState::default(), config.clone());
    let actual_output = state.handle(adopted(Term(0), LogIndex(0), &log_entries[0..2]));

    assert!(actual_output.next_tick >= Some(Timestamp(0) + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(Timestamp(0) + config.max_election_timeout));

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

    let actual_output = state.handle(Input {
        timestamp: Timestamp(10),
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(1),
            to_id: NodeId(2),
            payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                database_id: DATABASE_ID,
                term: Term(0),
                prev_log_index: LogIndex(1),
                prev_log_term: Term(0),
                entries: log_entries[1..3].to_vec(),
                leader_commit: LogIndex(2),
            }),
        }),
    });

    assert!(actual_output.next_tick >= Some(Timestamp(10) + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(Timestamp(10) + config.max_election_timeout));

    let expected_actions = vec![
        Action::ExtendLog(ExtendLogAction {
            entries: vec![EntryFromRequest {
                request_id: None,
                entry: log_entries[2].clone(),
            }],
        }),
        Action::SendMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(1),
            payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(0),
                match_index: Some(LogIndex(3)),
                conflict_opt: None,
            }),
        }),
        Action::ApplyLog(ApplyLogAction {
            up_to_log_index: LogIndex(2),
        }),
    ];

    assert_eq!(actual_output.actions, expected_actions);
}

#[test]
fn two_nodes_conflict() {
    let config = default_config();
    let mut log_entries = two_node_entries();
    log_entries.push(Arc::new(Entry {
        term: Term(0),
        payload: EntryPayload::Application(()),
    }));

    let mut state = State::<()>::new(NodeId(2), InitialState::default(), config.clone());
    let actual_output = state.handle(adopted(Term(0), LogIndex(0), &log_entries));

    assert!(actual_output.next_tick >= Some(Timestamp(0) + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(Timestamp(0) + config.max_election_timeout));

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
                EntryFromRequest {
                    request_id: None,
                    entry: log_entries[2].clone(),
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
                match_index: Some(LogIndex(3)),
                conflict_opt: None,
            }),
        }),
    ];

    assert_eq!(actual_output.actions, expected_actions);

    let actual_output = state.handle(Input {
        timestamp: Timestamp(10),
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(1),
            to_id: NodeId(2),
            payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                database_id: DATABASE_ID,
                term: Term(0),
                prev_log_index: LogIndex(3),
                prev_log_term: Term(1),
                entries: vec![Arc::new(Entry {
                    term: Term(1),
                    payload: EntryPayload::Application(()),
                })],
                leader_commit: LogIndex(2),
            }),
        }),
    });

    assert!(actual_output.next_tick >= Some(Timestamp(10) + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(Timestamp(10) + config.max_election_timeout));

    let expected_actions = vec![Action::SendMessage(Message {
        from_id: NodeId(2),
        to_id: NodeId(1),
        payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
            term: Term(0),
            match_index: None,
            conflict_opt: Some(ConflictOpt { index: LogIndex(0) }),
        }),
    })];

    assert_eq!(actual_output.actions, expected_actions);
}

#[test]
fn two_nodes_future() {
    let config = default_config();
    let log_entries = two_node_entries();

    let mut state = State::<()>::new(NodeId(2), InitialState::default(), config.clone());
    let actual_output = state.handle(adopted(Term(0), LogIndex(0), &log_entries));

    assert!(actual_output.next_tick >= Some(Timestamp(0) + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(Timestamp(0) + config.max_election_timeout));

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

    let actual_output = state.handle(Input {
        timestamp: Timestamp(10),
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(1),
            to_id: NodeId(2),
            payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                database_id: DATABASE_ID,
                term: Term(0),
                prev_log_index: LogIndex(3),
                prev_log_term: Term(1),
                entries: vec![Arc::new(Entry {
                    term: Term(1),
                    payload: EntryPayload::Application(()),
                })],
                leader_commit: LogIndex(2),
            }),
        }),
    });

    assert!(actual_output.next_tick >= Some(Timestamp(10) + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(Timestamp(10) + config.max_election_timeout));

    let expected_actions = vec![Action::SendMessage(Message {
        from_id: NodeId(2),
        to_id: NodeId(1),
        payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
            term: Term(0),
            match_index: None,
            conflict_opt: Some(ConflictOpt { index: LogIndex(2) }),
        }),
    })];

    assert_eq!(actual_output.actions, expected_actions);
}
