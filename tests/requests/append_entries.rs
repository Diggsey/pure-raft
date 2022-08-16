use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

use pure_raft::{
    AppendEntriesRequest, Config, DatabaseId, Duration, Entry, EntryPayload, Event, Input,
    LogIndex, Membership, MembershipChangeCondition, Message, MessagePayload, NodeId,
    PersistentState, State, Term, Timestamp,
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
    let membership = Membership::bootstrap([NodeId(0), NodeId(1), NodeId(2)], []);
    let input = Input::<Data> {
        timestamp: Timestamp(0),
        persistent_state: PersistentState {
            database_id: DATABASE_ID,
            current_term: Term(1),
            voted_for: None,
            current_membership: membership.clone(),
            last_log_applied: LogIndex(1),
            last_term_applied: Term(1),
            last_membership_applied: membership.clone(),
            unapplied_log_terms: VecDeque::new(),
            pending_log_changes: vec![],
            cached_log_entries: BTreeMap::new(),
            desired_log_entries: BTreeSet::new(),
        },
        event: Event::Message(Message {
            from_id: NodeId(0),
            to_id: NodeId(1),
            payload: MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                database_id: DATABASE_ID,
                term: Term(0),
                prev_log_index: LogIndex(0),
                prev_log_term: Term(0),
                entries: vec![Arc::new(Entry {
                    term: Term(0),
                    payload: EntryPayload::Blank,
                })],
                leader_commit: LogIndex(0),
            }),
        }),
    };
    let mut state = State::new(NodeId(1), config);
    let output = state.handle(input);
    dbg!(output);
    panic!();
}
