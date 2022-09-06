use std::{fmt::Debug, ops::RangeInclusive};

use rand::{thread_rng, Rng};

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

pub trait RandomSampler: Debug {
    fn sample_duration(&mut self, range: RangeInclusive<Duration>) -> Duration;
    fn boxed_clone(&self) -> Box<dyn RandomSampler>;
}

impl Clone for Box<dyn RandomSampler> {
    fn clone(&self) -> Self {
        self.boxed_clone()
    }
}

impl<T: Rng + Clone + Debug + 'static> RandomSampler for T {
    fn sample_duration(&mut self, range: RangeInclusive<Duration>) -> Duration {
        Duration(self.gen_range(range.start().0..=range.end().0))
    }

    fn boxed_clone(&self) -> Box<dyn RandomSampler> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Copy)]
pub struct RandomSamplerFn<F: FnMut(RangeInclusive<Duration>) -> Duration>(pub F);

impl<F: FnMut(RangeInclusive<Duration>) -> Duration> Debug for RandomSamplerFn<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RandomSamplerFn").finish()
    }
}

impl<F: Clone + FnMut(RangeInclusive<Duration>) -> Duration + 'static> RandomSampler
    for RandomSamplerFn<F>
{
    fn sample_duration(&mut self, range: RangeInclusive<Duration>) -> Duration {
        (self.0)(range)
    }

    fn boxed_clone(&self) -> Box<dyn RandomSampler> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub random_sampler: Box<dyn RandomSampler>,
    pub pre_vote: bool,
    pub leader_stickiness: bool,
    pub heartbeat_interval: Duration,
    pub min_election_timeout: Duration,
    pub max_election_timeout: Duration,
    pub batch_size: u64,
    pub max_unapplied_entries: u64,
    pub membership_change_condition: MembershipChangeCondition,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            random_sampler: Box::new(thread_rng()),
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
}
