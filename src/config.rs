use crate::Duration;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum MembershipChangeCondition {
    /// Sufficient nodes from the target configuration must be
    /// up-to-date such that the requested fault tolerance can be
    /// respected.
    MinimumUpToDate,
    /// In addition to the minimum requirements, all new nodes must
    /// be up-to-date.
    NewUpToDate,
    /// All nodes in the target configuration must be up-to-date.
    AllUpToDate,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub pre_vote: bool,
    pub leader_stickiness: bool,
    pub heartbeat_interval: Duration,
    pub min_election_timeout: Duration,
    pub max_election_timeout: Duration,
    pub batch_size: u64,
    pub max_unapplied_entries: u64,
    pub membership_change_condition: MembershipChangeCondition,
}
