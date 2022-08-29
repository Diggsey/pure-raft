use pure_raft::{Config, Duration, MembershipChangeCondition};

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

pub mod append_entries;
pub mod bootstrap;
