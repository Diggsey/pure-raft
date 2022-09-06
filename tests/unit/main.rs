use std::sync::Arc;

use maplit::{btreemap, btreeset};
use pure_raft::{
    AppendEntriesRequest, BootstrapRequest, ClientRequest, ClientRequestPayload, Config,
    DatabaseId, Entry, EntryPayload, Event, Input, LogIndex, Membership, MembershipType, Message,
    MessagePayload, NodeId, RequestId, Term, Timestamp,
};

fn default_config() -> Config {
    Config::default()
}

const DATABASE_ID: DatabaseId = DatabaseId(1);

fn single_node_bootstrap<D>() -> Input<D> {
    Input {
        timestamp: Timestamp(0),
        event: Event::ClientRequest(ClientRequest {
            request_id: Some(RequestId(1)),
            payload: ClientRequestPayload::Bootstrap(BootstrapRequest {
                database_id: DATABASE_ID,
                voter_ids: btreeset![NodeId(1)],
                learner_ids: btreeset![],
            }),
        }),
    }
}

fn two_node_bootstrap<D>() -> Input<D> {
    Input {
        timestamp: Timestamp(0),
        event: Event::ClientRequest(ClientRequest {
            request_id: Some(RequestId(1)),
            payload: ClientRequestPayload::Bootstrap(BootstrapRequest {
                database_id: DATABASE_ID,
                voter_ids: btreeset![NodeId(1), NodeId(2)],
                learner_ids: btreeset![],
            }),
        }),
    }
}

fn single_node_with_learner_bootstrap<D>() -> Input<D> {
    Input {
        timestamp: Timestamp(0),
        event: Event::ClientRequest(ClientRequest {
            request_id: Some(RequestId(1)),
            payload: ClientRequestPayload::Bootstrap(BootstrapRequest {
                database_id: DATABASE_ID,
                voter_ids: btreeset![NodeId(1)],
                learner_ids: btreeset![NodeId(2)],
            }),
        }),
    }
}

fn three_node_bootstrap<D>() -> Input<D> {
    Input {
        timestamp: Timestamp(0),
        event: Event::ClientRequest(ClientRequest {
            request_id: Some(RequestId(1)),
            payload: ClientRequestPayload::Bootstrap(BootstrapRequest {
                database_id: DATABASE_ID,
                voter_ids: btreeset![NodeId(1), NodeId(2), NodeId(3)],
                learner_ids: btreeset![],
            }),
        }),
    }
}

fn two_node_entries<D>() -> Vec<Arc<Entry<D>>> {
    vec![
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
    ]
}

fn single_node_with_learner_entries<D>() -> Vec<Arc<Entry<D>>> {
    vec![
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
    ]
}

fn three_node_entries<D>() -> Vec<Arc<Entry<D>>> {
    vec![
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
    ]
}

fn adopted<'a, D: 'a>(
    term: Term,
    leader_commit: LogIndex,
    entries: impl IntoIterator<Item = &'a Arc<Entry<D>>>,
) -> Input<D> {
    Input {
        timestamp: Timestamp(0),
        event: Event::ReceivedMessage(Message {
            from_id: NodeId(1),
            to_id: NodeId(2),
            payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                database_id: DATABASE_ID,
                term,
                prev_log_index: LogIndex(0),
                prev_log_term: Term(0),
                entries: entries.into_iter().cloned().collect(),
                leader_commit,
            }),
        }),
    }
}

pub mod client_requests;
pub mod loaded_log;
pub mod requests;
pub mod responses;
pub mod ticks;
