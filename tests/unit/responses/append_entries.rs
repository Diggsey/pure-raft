use maplit::btreeset;
use pretty_assertions::assert_eq;
use pure_raft::{
    Action, AppendEntriesResponse, ApplyLogAction, ClientRequest, ClientRequestPayload,
    ConflictOpt, Event, HardState, InitialState, Input, LoadLogAction, LogIndex, Message,
    MessagePayload, NodeId, Output, RequestId, State, Term, Timestamp,
};

use crate::{default_config, two_node_bootstrap, DATABASE_ID};

#[test]
fn two_node() {
    let mut state = State::<()>::new(NodeId(1), InitialState::default(), default_config());
    state.handle(two_node_bootstrap());
    let actual_output = state.handle(Input {
        timestamp: Timestamp(100),
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(1),
            payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(0),
                match_index: Some(LogIndex(2)),
                conflict_opt: None,
            }),
        }),
    });

    let expected_output = Output {
        next_tick: Some(Timestamp(1000)),
        actions: vec![Action::ApplyLog(ApplyLogAction {
            up_to_log_index: LogIndex(2),
        })],
        errors: Vec::new(),
    };

    assert_eq!(actual_output, expected_output);
}

#[test]
fn two_node_conflict() {
    let mut state = State::<()>::new(NodeId(1), InitialState::default(), default_config());
    state.handle(two_node_bootstrap());
    state.handle(Input {
        timestamp: Timestamp(100),
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(1),
            payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(0),
                match_index: Some(LogIndex(2)),
                conflict_opt: None,
            }),
        }),
    });
    state.handle(Input {
        timestamp: Timestamp(200),
        event: Event::ClientRequest(ClientRequest {
            request_id: Some(RequestId(2)),
            payload: ClientRequestPayload::Application(()),
        }),
    });
    let actual_output = state.handle(Input {
        timestamp: Timestamp(300),
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(1),
            payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(0),
                match_index: None,
                conflict_opt: Some(ConflictOpt { index: LogIndex(1) }),
            }),
        }),
    });

    let expected_output = Output {
        next_tick: None,
        actions: vec![Action::LoadLog(LoadLogAction {
            desired_entries: btreeset![LogIndex(1), LogIndex(2), LogIndex(3)],
        })],
        errors: Vec::new(),
    };

    assert_eq!(actual_output, expected_output);
}

#[test]
fn two_node_ignored() {
    let mut state = State::<()>::new(NodeId(1), InitialState::default(), default_config());
    state.handle(two_node_bootstrap());
    state.handle(Input {
        timestamp: Timestamp(100),
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(1),
            payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(0),
                match_index: Some(LogIndex(2)),
                conflict_opt: None,
            }),
        }),
    });
    state.handle(Input {
        timestamp: Timestamp(200),
        event: Event::ClientRequest(ClientRequest {
            request_id: Some(RequestId(2)),
            payload: ClientRequestPayload::Application(()),
        }),
    });
    let actual_output = state.handle(Input {
        timestamp: Timestamp(300),
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(1),
            payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(0),
                match_index: None,
                conflict_opt: None,
            }),
        }),
    });

    let expected_output = Output {
        next_tick: None,
        actions: vec![Action::LoadLog(LoadLogAction {
            desired_entries: btreeset![LogIndex(3)],
        })],
        errors: Vec::new(),
    };

    assert_eq!(actual_output, expected_output);
}

#[test]
fn two_node_wrong_term() {
    let config = default_config();
    let mut state = State::<()>::new(NodeId(1), InitialState::default(), config.clone());
    state.handle(two_node_bootstrap());
    state.handle(Input {
        timestamp: Timestamp(100),
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(1),
            payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(0),
                match_index: Some(LogIndex(2)),
                conflict_opt: None,
            }),
        }),
    });
    state.handle(Input {
        timestamp: Timestamp(200),
        event: Event::ClientRequest(ClientRequest {
            request_id: Some(RequestId(2)),
            payload: ClientRequestPayload::Application(()),
        }),
    });
    let actual_output = state.handle(Input {
        timestamp: Timestamp(300),
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(2),
            to_id: NodeId(1),
            payload: MessagePayload::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(1),
                match_index: None,
                conflict_opt: None,
            }),
        }),
    });

    let expected_actions = vec![Action::SaveState(HardState {
        database_id: DATABASE_ID,
        current_term: Term(1),
        voted_for: None,
    })];

    assert!(actual_output.next_tick >= Some(Timestamp(300) + config.min_election_timeout));
    assert!(actual_output.next_tick <= Some(Timestamp(300) + config.max_election_timeout));

    assert_eq!(actual_output.actions, expected_actions);
}
