mod config;
mod entry;
mod io;
mod membership;
mod state;
mod types;

pub use config::{Config, MembershipChangeCondition};
pub use entry::{Entry, EntryFromRequest, EntryPayload};
pub use io::{
    client_requests::{BootstrapRequest, ClientRequest, ClientRequestPayload},
    initial_state::{HardState, InitialSnapshot, InitialState},
    messages::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, Message, MessagePayload, PreVoteRequest, PreVoteResponse,
        VoteRequest, VoteResponse,
    },
    Action, ApplyLogAction, Event, ExtendLogAction, FailedRequest, Input, LoadLogAction,
    LoadedLogEvent, Output, TruncateLogAction,
};
pub use membership::{Membership, MembershipType};
pub use state::State;
pub use types::{DatabaseId, Duration, LogIndex, NodeId, RequestId, Term, Timestamp};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
