mod election_state;
mod entry;
mod io;
mod membership;
mod state;
mod types;

pub use io::Message;
pub use state::{Config, State};
pub use types::{DatabaseId, Duration, LogIndex, NodeId, RequestId, Term, Timestamp};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
