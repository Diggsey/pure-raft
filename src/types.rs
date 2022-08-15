use std::{
    num::NonZeroU64,
    ops::{Add, AddAssign, Sub, SubAssign},
};

use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]

pub struct DatabaseId(pub u64);

impl DatabaseId {
    pub const UNSET: Self = Self(0);

    pub fn new(id: NonZeroU64) -> Self {
        Self(id.get())
    }

    pub fn is_set(self) -> bool {
        self != Self::UNSET
    }
}

#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Term(pub u64);

impl Term {
    pub fn inc(&mut self) {
        self.0 += 1;
    }
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RequestId(pub u64);

#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct LogIndex(pub u64);

impl LogIndex {
    pub const ZERO: LogIndex = LogIndex(0);
}

impl Add<u64> for LogIndex {
    type Output = LogIndex;
    fn add(mut self, other: u64) -> LogIndex {
        self += other;
        self
    }
}

impl AddAssign<u64> for LogIndex {
    fn add_assign(&mut self, other: u64) {
        self.0 += other;
    }
}

impl Sub<LogIndex> for LogIndex {
    type Output = u64;
    fn sub(self, other: LogIndex) -> u64 {
        self.0 - other.0
    }
}

impl Sub<u64> for LogIndex {
    type Output = LogIndex;
    fn sub(mut self, other: u64) -> LogIndex {
        self -= other;
        self
    }
}

impl SubAssign<u64> for LogIndex {
    fn sub_assign(&mut self, other: u64) {
        assert!(self.0 >= other);
        self.0 -= other;
    }
}

#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Timestamp(pub u64);

#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Duration(pub u64);

impl Add<Duration> for Timestamp {
    type Output = Timestamp;
    fn add(mut self, other: Duration) -> Timestamp {
        self += other;
        self
    }
}

impl AddAssign<Duration> for Timestamp {
    fn add_assign(&mut self, other: Duration) {
        self.0 += other.0;
    }
}

impl Sub<Timestamp> for Timestamp {
    type Output = Duration;
    fn sub(self, other: Timestamp) -> Duration {
        Duration(self.0 - other.0)
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Timestamp;
    fn sub(mut self, other: Duration) -> Timestamp {
        self -= other;
        self
    }
}

impl SubAssign<Duration> for Timestamp {
    fn sub_assign(&mut self, other: Duration) {
        assert!(self.0 >= other.0);
        self.0 -= other.0;
    }
}

impl Add<Duration> for Duration {
    type Output = Duration;
    fn add(mut self, other: Duration) -> Duration {
        self += other;
        self
    }
}

impl AddAssign<Duration> for Duration {
    fn add_assign(&mut self, other: Duration) {
        self.0 += other.0;
    }
}
