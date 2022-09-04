use std::sync::Arc;

use pretty_assertions::assert_eq;
use pure_raft::{
    Action, AppendEntriesRequest, AppendEntriesResponse, ClientRequest, ClientRequestPayload,
    Entry, EntryFromRequest, EntryPayload, Event, ExtendLogAction, InitialState, Input, LogIndex,
    Message, MessagePayload, NodeId, Output, RequestId, State, Term, Timestamp,
};

use crate::{default_config, two_node_bootstrap, DATABASE_ID};

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
    let actual_output = state.handle(Input {
        timestamp: Timestamp(200),
        event: Event::ClientRequest(ClientRequest {
            request_id: Some(RequestId(2)),
            payload: ClientRequestPayload::Application(()),
        }),
    });

    let log_entry = Arc::new(Entry {
        term: Term(0),
        payload: EntryPayload::Application(()),
    });

    let expected_output = Output {
        next_tick: Some(Timestamp(1200)),
        actions: vec![
            Action::ExtendLog(ExtendLogAction {
                entries: vec![EntryFromRequest {
                    request_id: Some(RequestId(2)),
                    entry: log_entry.clone(),
                }],
            }),
            Action::SendMessage(Message {
                from_id: NodeId(1),
                to_id: NodeId(2),
                payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                    database_id: DATABASE_ID,
                    term: Term(0),
                    prev_log_index: LogIndex(2),
                    prev_log_term: Term(0),
                    entries: vec![log_entry],
                    leader_commit: LogIndex(2),
                }),
            }),
        ],
    };

    assert_eq!(actual_output, expected_output);
}
