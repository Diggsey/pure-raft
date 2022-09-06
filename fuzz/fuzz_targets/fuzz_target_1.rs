#![no_main]

use std::collections::{BTreeMap, BTreeSet};
use std::mem;
use std::sync::Arc;

use arbitrary::{Error, Result, Unstructured};
use libfuzzer_sys::fuzz_target;
use scoped_tls_hkt::scoped_thread_local;

use pure_raft::*;

#[derive(Debug)]
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
}

const MAX_NODES: u64 = 19;
const MAX_LATENCY: u64 = 5000;

scoped_thread_local!(static mut INPUT: Unstructured<'static>);

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
        }
    }

    fn handle(&mut self, node_id: NodeId, event: Event<Data>) -> Result<()> {
        let in_flight_ops = {
            let mut node = self.nodes.get_mut(&node_id).unwrap();
            let output = node.state.handle(Input {
                timestamp: self.timestamp,
                event,
            });
            node.next_tick = output.next_tick;
            let mut in_flight_ops = Vec::new();

            for action in output.actions {
                if INPUT.with(|input| input.ratio(1, 256))? {
                    node.reset(node_id, self.config.clone());
                    break;
                }
                match action {
                    Action::TruncateLog(x) => {
                        assert!(x.last_log_index >= node.applied_up_to);
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
                        if delay != MAX_LATENCY {
                            in_flight_ops.push((
                                self.timestamp + Duration(delay),
                                InFlightOp::Message(message),
                            ));
                        }
                    }
                    Action::FailedRequest(_) => {}
                    Action::ApplyLog(x) => {
                        assert!(x.up_to_log_index > node.applied_up_to);
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

    fn advance(&mut self) -> Result<()> {
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
    let mut data = Unstructured::new(unsafe {
        // Safety: we are erasing the lifetime of `data` here, replacing
        // it with `'static`. This is sound because we will ensure all
        // references are dropped before returning from the function.
        mem::transmute(data)
    });
    let mut system = MultiNodeState::new();
    let _ = INPUT.set(&mut data, || -> Result<()> {
        let initial_node_count = INPUT.with(|input| input.int_in_range(1..=MAX_NODES))?;
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
