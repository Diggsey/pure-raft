use maplit::btreeset;
use pure_raft::{
    BootstrapRequest, ClientRequest, ClientRequestPayload, Config, DatabaseId, Duration, Event,
    InitialState, Input, MembershipChangeCondition, NodeId, RequestId, State, Timestamp,
};

#[derive(Debug, Copy, Clone)]
struct Data(u32);

const DATABASE_ID: DatabaseId = DatabaseId(1);

#[test]
fn smoke() {
    let config = Config {
        pre_vote: true,
        leader_stickiness: true,
        heartbeat_interval: Duration(1000),
        min_election_timeout: Duration(2000),
        max_election_timeout: Duration(3000),
        batch_size: 10,
        max_unapplied_entries: 20,
        membership_change_condition: MembershipChangeCondition::NewUpToDate,
    };
    let initial_state = InitialState::default();

    let mut state = State::<()>::new(NodeId(1), initial_state, config);
    let output = state.handle(Input {
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
    dbg!(output);
    panic!();
}
