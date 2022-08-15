use std::collections::VecDeque;

use pure_raft::{Config, Duration, Message, NodeId, State};

struct Node {
    state: State,
}

impl Node {
    fn new(index: u64, config: &Config) -> Self {
        Self {
            state: State::new(NodeId(index), config.clone()),
        }
    }
}

struct Simulation {
    nodes: Vec<Node>,
    in_flight_messages: VecDeque<Message>,
}

fn main() {
    let config = Config {
        pre_vote: true,
        leader_stickiness: true,
        heartbeat_interval: Duration(1000),
        min_election_timeout: Duration(2000),
        max_election_timeout: Duration(3000),
        batch_size: 10,
    };
    let mut simulation = Simulation {
        nodes: vec![
            Node::new(0, &config),
            Node::new(1, &config),
            Node::new(2, &config),
        ],
        in_flight_messages: VecDeque::new(),
    };
    println!("Hello, world!");
}
