use maplit::btreeset;
use pure_raft::{
    BootstrapRequest, ClientRequest, ClientRequestPayload, Config, DatabaseId, Duration, Event,
    Input, MembershipChangeCondition, NodeId, RequestId, Timestamp,
};

fn default_config() -> Config {
    Config {
        pre_vote: true,
        leader_stickiness: true,
        heartbeat_interval: Duration(1000),
        min_election_timeout: Duration(2000),
        max_election_timeout: Duration(3000),
        batch_size: 10,
        max_unapplied_entries: 20,
        membership_change_condition: MembershipChangeCondition::NewUpToDate,
    }
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

pub mod client_requests;
pub mod loaded_log;
pub mod requests;
pub mod responses;
pub mod ticks;
