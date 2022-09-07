#![no_main]

use std::collections::{btree_map, hash_map::DefaultHasher, BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::Arc;

use arbitrary::{Error, Result, Unstructured};
use libfuzzer_sys::fuzz_target;
use scoped_tls_hkt::scoped_thread_local;

use pure_raft::*;

#[derive(Debug, Hash, PartialEq, Eq)]
struct Data;

struct NodeState {
    state: State<Data>,
    next_tick: Option<Timestamp>,
    log_entries: Vec<Arc<Entry<Data>>>,
    hard_state: HardState,
    applied_up_to: LogIndex,
}

impl NodeState {
    fn reset(&mut self, node_id: NodeId, config: Config) {
        self.state = State::new(
            node_id,
            InitialState {
                hard_state: self.hard_state.clone(),
                initial_snapshot: None,
                log_terms: self.log_entries.iter().map(|e| e.term).collect(),
                membership_changes: self
                    .log_entries
                    .iter()
                    .enumerate()
                    .filter_map(|(i, e)| {
                        if let EntryPayload::MembershipChange(m) = &e.payload {
                            Some((LogIndex(i as u64 + 1), m.clone()))
                        } else {
                            None
                        }
                    })
                    .collect(),
            },
            config,
        );
        self.applied_up_to = LogIndex(0);
        self.next_tick = Some(Timestamp(0));
    }
    fn is_bootstrapped(&self) -> bool {
        !self.log_entries.is_empty()
    }
}

enum InFlightOp {
    Message(Message<Data>),
    Storage(StorageOp),
}

struct StorageOp {
    node_id: NodeId,
    entries: BTreeMap<LogIndex, Arc<Entry<Data>>>,
}

struct MultiNodeState {
    config: Config,
    nodes: BTreeMap<NodeId, NodeState>,
    in_flight_ops: BTreeMap<Timestamp, Vec<InFlightOp>>,
    timestamp: Timestamp,
    initial_count: u64,
    allow_failures: bool,
    leaders: BTreeMap<Term, NodeId>,
    first_committed: BTreeMap<LogIndex, (Arc<Entry<Data>>, Term)>,
}

const MAX_NODES: u64 = 19;
const MAX_LATENCY: u64 = 5000;

scoped_thread_local!(static mut INPUT: Unstructured<'static>);

fn log_event(timestamp: Timestamp, node_id: NodeId, name: &str, data: &dyn Debug) {
    if log::log_enabled!(log::Level::Debug) {
        eprintln!(
            "Time {}, Node {}, {}: {:?}",
            timestamp.0, node_id.0, name, data
        );
    }
}

impl MultiNodeState {
    fn new() -> Self {
        let mut config = Config::default();
        config.random_sampler = Box::new(RandomSamplerFn(|range| {
            INPUT.with(|input| {
                Duration(input.int_in_range(range.start().0..=range.end().0).unwrap())
            })
        }));
        let nodes = (0..MAX_NODES)
            .map(|i| {
                (
                    NodeId(i),
                    NodeState {
                        state: State::new(NodeId(i), InitialState::default(), config.clone()),
                        log_entries: vec![],
                        hard_state: HardState::default(),
                        applied_up_to: LogIndex(0),
                        next_tick: None,
                    },
                )
            })
            .collect();

        Self {
            config,
            nodes,
            in_flight_ops: BTreeMap::new(),
            timestamp: Timestamp(0),
            initial_count: 0,
            allow_failures: false,
            leaders: BTreeMap::new(),
            first_committed: BTreeMap::new(),
        }
    }

    fn handle(&mut self, node_id: NodeId, event: Event<Data>) -> Result<()> {
        log_event(self.timestamp, node_id, "Handle", &event);
        let in_flight_ops = {
            let mut node = self.nodes.get_mut(&node_id).unwrap();
            let output = node.state.handle(Input {
                timestamp: self.timestamp,
                event,
            });
            node.next_tick = output.next_tick;
            let mut in_flight_ops = Vec::new();

            for action in output.actions {
                if self.allow_failures && INPUT.with(|input| input.ratio(1, 256))? {
                    node.reset(node_id, self.config.clone());
                    log_event(self.timestamp, node_id, "Reset", &());
                    break;
                }
                log_event(self.timestamp, node_id, "Action", &action);
                match action {
                    Action::TruncateLog(x) => {
                        assert!(x.last_log_index >= node.applied_up_to);
                        // Check "leader append-only" safety invariant
                        assert!(!node.state.is_leader());
                        node.log_entries.truncate(x.last_log_index.0 as usize)
                    }
                    Action::ExtendLog(x) => node
                        .log_entries
                        .extend(x.entries.into_iter().map(|e| e.entry)),
                    Action::SaveState(hard_state) => {
                        node.hard_state = hard_state;
                    }
                    Action::SendMessage(message) => {
                        let delay = INPUT.with(|input| input.int_in_range(0..=MAX_LATENCY))?;
                        if !self.allow_failures || delay != MAX_LATENCY {
                            in_flight_ops.push((
                                self.timestamp + Duration(delay),
                                InFlightOp::Message(message),
                            ));
                        } else {
                            log_event(self.timestamp, node_id, "Drop", &message);
                        }
                    }
                    Action::FailedRequest(_) => {}
                    Action::ApplyLog(x) => {
                        assert!(x.up_to_log_index > node.applied_up_to);

                        for log_index in node.applied_up_to.0..x.up_to_log_index.0 {
                            match self.first_committed.entry(LogIndex(log_index) + 1) {
                                btree_map::Entry::Occupied(x) => {
                                    let x = x.into_mut();
                                    // Check "state machine safety" invariant
                                    assert_eq!(x.0, node.log_entries[log_index as usize]);
                                    if node.hard_state.current_term < x.1 {
                                        x.1 = node.hard_state.current_term;
                                    }
                                }
                                btree_map::Entry::Vacant(x) => {
                                    x.insert((
                                        node.log_entries[log_index as usize].clone(),
                                        node.hard_state.current_term,
                                    ));
                                }
                            }
                        }

                        node.applied_up_to = x.up_to_log_index;
                    }
                    Action::LoadLog(x) => {
                        let delay = INPUT.with(|input| input.int_in_range(0..=MAX_LATENCY))?;
                        in_flight_ops.push((
                            self.timestamp + Duration(delay),
                            InFlightOp::Storage(StorageOp {
                                node_id,
                                entries: x
                                    .desired_entries
                                    .into_iter()
                                    .map(|log_index| {
                                        (
                                            log_index,
                                            node.log_entries[log_index.0 as usize - 1].clone(),
                                        )
                                    })
                                    .collect(),
                            }),
                        ));
                    }
                }
            }
            in_flight_ops
        };
        for (ts, op) in in_flight_ops {
            self.in_flight_ops.entry(ts).or_default().push(op);
        }
        Ok(())
    }

    fn is_majority_bootstrapped(&self) -> bool {
        self.nodes
            .values()
            .filter(|node| node.is_bootstrapped())
            .count() as u64
            > self.initial_count / 2
    }

    fn check_election_safety(&mut self) {
        for (k, v) in &self.nodes {
            if v.state.is_leader() {
                let prev = self.leaders.insert(v.hard_state.current_term, *k);
                assert!(prev.is_none() || prev == Some(*k));
            }
        }
    }

    fn check_log_matching(&self) {
        let mut hashes = BTreeMap::new();
        for node in self.nodes.values() {
            let mut hasher = DefaultHasher::new();
            for (i, v) in node.log_entries.iter().enumerate() {
                v.hash(&mut hasher);
                let hash = hasher.finish();
                let existing_hash = *hashes.entry((v.term, i)).or_insert(hash);
                assert_eq!(hash, existing_hash);
            }
        }
    }

    fn check_leader_completeness(&self) {
        for (log_index, (entry, term)) in &self.first_committed {
            let leader_ids: BTreeSet<NodeId> =
                self.leaders.range(term..).map(|(_, v)| *v).collect();
            for leader_id in &leader_ids {
                assert_eq!(
                    &self.nodes[leader_id].log_entries[log_index.0 as usize - 1],
                    entry
                );
            }
        }
    }

    fn advance(&mut self) -> Result<()> {
        self.check_election_safety();
        self.check_log_matching();
        self.check_leader_completeness();

        if !self.allow_failures {
            self.allow_failures = self.is_majority_bootstrapped();
        }
        let mut next_timestamp = Timestamp(u64::MAX);
        for (node_id, node) in &self.nodes {
            if let Some(next_tick) = node.next_tick {
                if next_tick <= self.timestamp {
                    return self.handle(*node_id, Event::Tick);
                } else {
                    next_timestamp = next_timestamp.min(next_tick);
                }
            }
        }
        if let Some(next_op) = self.in_flight_ops.keys().copied().next() {
            if next_op <= self.timestamp {
                let ops = self.in_flight_ops.get_mut(&next_op).unwrap();
                if let Some(op) = ops.pop() {
                    return match op {
                        InFlightOp::Message(m) => self.handle(m.to_id, Event::ReceivedMessage(m)),
                        InFlightOp::Storage(s) => self.handle(
                            s.node_id,
                            Event::LoadedLog(LoadedLogEvent { entries: s.entries }),
                        ),
                    };
                } else {
                    self.in_flight_ops.remove(&next_op);
                    return Ok(());
                }
            } else {
                next_timestamp = next_timestamp.min(next_op);
            }
        }

        if next_timestamp == Timestamp(u64::MAX) {
            return Err(Error::NotEnoughData);
        }

        assert!(next_timestamp > self.timestamp);
        if INPUT.with(|input| input.ratio(1, 256))? {
            self.timestamp = Timestamp(
                INPUT.with(|input| input.int_in_range(self.timestamp.0..=next_timestamp.0))?,
            );
            let node_id = NodeId(INPUT.with(|input| input.int_in_range(0..=MAX_NODES - 1))?);
            return self.handle(
                node_id,
                Event::ClientRequest(ClientRequest {
                    request_id: None,
                    payload: ClientRequestPayload::Application(Data),
                }),
            );
        }

        self.timestamp = next_timestamp;
        Ok(())
    }
}

fuzz_target!(|data: &[u8]| {
    let _ = pretty_env_logger::try_init();

    let mut data = Unstructured::new(unsafe {
        // Safety: we are erasing the lifetime of `data` here, replacing
        // it with `'static`. This is sound because we will ensure all
        // references are dropped before returning from the function.
        mem::transmute(data)
    });
    let mut system = MultiNodeState::new();
    let _ = INPUT.set(&mut data, || -> Result<()> {
        let initial_node_count = INPUT.with(|input| input.int_in_range(1..=MAX_NODES))?;
        system.initial_count = initial_node_count;
        system.handle(
            NodeId(0),
            Event::ClientRequest(ClientRequest {
                request_id: None,
                payload: ClientRequestPayload::Bootstrap(BootstrapRequest {
                    database_id: DatabaseId(1),
                    learner_ids: BTreeSet::new(),
                    voter_ids: (0..initial_node_count).map(NodeId).collect(),
                }),
            }),
        )?;

        while INPUT.with(|input| !input.is_empty()) {
            system.advance()?;
        }
        Ok(())
    });
});
