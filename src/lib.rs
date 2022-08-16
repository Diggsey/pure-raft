mod election_state;
mod entry;
mod io;
mod membership;
mod state;
mod types;

pub use entry::{Entry, EntryFromRequest, EntryPayload};
pub use io::{
    messages::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, Message, MessagePayload, PreVoteRequest, PreVoteResponse,
        VoteRequest, VoteResponse,
    },
    persistent_state::PersistentState,
    Event, Input, Output,
};
pub use membership::Membership;
pub use state::{
    config::{Config, MembershipChangeCondition},
    State,
};
pub use types::{DatabaseId, Duration, LogIndex, NodeId, RequestId, Term, Timestamp};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
