use std::{
    cmp,
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use rand::{thread_rng, Rng};

use crate::{
    election_state::ElectionState,
    entry::{Entry, EntryPayload},
    io::{
        AppendEntriesRequest, AppendEntriesResponse, BootstrapError, BootstrapRequest,
        ClientRequest, ClientRequestPayload, ConflictOpt, EntryFromClient, Event, FailedRequest,
        Input, LogChange, LogRange, Message, MessagePayload, Output, PreVoteRequest,
        PreVoteResponse, RequestError, SetLearnersError, SetLearnersRequest, SetMembersError,
        SetMembersRequest, VoteRequest, VoteResponse,
    },
    membership::Membership,
    types::{DatabaseId, Duration, LogIndex, NodeId, RequestId, Term, Timestamp},
};

enum Role {
    Learner,
    Follower,
    Applicant(ElectionState),
    Candidate(ElectionState),
    Leader(LeaderState),
}

impl Role {
    fn is_learner(&self) -> bool {
        matches!(self, Self::Learner)
    }
    fn is_leader(&self) -> bool {
        matches!(self, Self::Leader(_))
    }
}

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

#[derive(Default)]
struct LeaderState {
    replication_state: HashMap<NodeId, ReplicationState>,
}

impl LeaderState {
    fn is_up_to_date(&self, common: &CommonState, node_id: NodeId, output: &Output) -> bool {
        if common.this_id == node_id {
            true
        } else if let Some(replication_state) = self.replication_state.get(&node_id) {
            replication_state.match_index + common.config.batch_size
                >= output.persistent_state.last_log_index()
        } else {
            false
        }
    }
}

struct ReplicationState {
    send_after_index: LogIndex,
    match_index: LogIndex,
    retry_at: Option<Timestamp>,
    in_flight_request: bool,
    waiting_on_storage: bool,
}

impl ReplicationState {
    fn should_retry(&self, timestamp: Timestamp) -> bool {
        if let Some(retry_at) = self.retry_at {
            timestamp >= retry_at
        } else {
            false
        }
    }
}

struct CommonState {
    this_id: NodeId,
    config: Config,
    leader_commit_index: LogIndex,
    committed_index: LogIndex,
    // Best guess at current leader, may not be accurate...
    leader_id: Option<NodeId>,
    election_timeout: Option<Timestamp>,
}

impl CommonState {
    fn new(this_id: NodeId, config: Config) -> Self {
        Self {
            this_id,
            config,
            leader_commit_index: LogIndex::ZERO,
            committed_index: LogIndex::ZERO,
            leader_id: None,
            election_timeout: None,
        }
    }
    fn mark_not_leader(&mut self) {
        if self.leader_id == Some(self.this_id) {
            self.leader_id = None;
        }
    }
    fn schedule_election_timeout(&mut self, timestamp: Timestamp, output: &mut Output) {
        let election_timeout = timestamp
            + Duration(thread_rng().gen_range(
                self.config.min_election_timeout.0..=self.config.max_election_timeout.0,
            ));
        self.election_timeout = Some(election_timeout);
        output.schedule_tick(election_timeout);
    }
    fn election_timeout_elapsed(&self, timestamp: Timestamp) -> bool {
        if let Some(election_timeout) = self.election_timeout {
            election_timeout <= timestamp
        } else {
            false
        }
    }

    fn send_message(&self, to_id: NodeId, output: &mut Output, payload: MessagePayload) {
        output.add_message(Message {
            from_id: self.this_id,
            to_id,
            payload,
        });
    }
}

pub struct State {
    common: CommonState,
    role: Role,
}

#[derive(Debug)]
enum Error {
    DatabaseMismatch,
}

impl State {
    pub fn new(this_id: NodeId, config: Config) -> Self {
        Self {
            common: CommonState::new(this_id, config),
            role: Role::Learner,
        }
    }
    pub fn handle(&mut self, input: Input) -> Output {
        let mut output = Output::default();
        output.persistent_state = input.persistent_state;
        match input.event {
            Event::Tick | Event::StateChanged => {
                self.advance(input.timestamp, &mut output);
            }
            Event::Message(message) => {
                if let Err(e) = self.handle_message(message, input.timestamp, &mut output) {
                    eprintln!("{:?}", e);
                }
            }
            Event::ClientRequest(client_request) => {
                let request_id = client_request.request_id;
                if let Err(error) =
                    self.handle_client_request(client_request, input.timestamp, &mut output)
                {
                    if let Some(request_id) = request_id {
                        output.add_failed_request(FailedRequest { request_id, error });
                    }
                }
            }
        };
        output
    }
    fn acknowledge_database_id(
        &mut self,
        database_id: DatabaseId,
        output: &mut Output,
    ) -> Result<(), Error> {
        if !output.persistent_state.database_id.is_set() {
            output.persistent_state.database_id = database_id;
        }
        if output.persistent_state.database_id == database_id {
            Ok(())
        } else {
            Err(Error::DatabaseMismatch)
        }
    }
    fn acknowledge_term(
        &mut self,
        term: Term,
        is_from_leader: bool,
        timestamp: Timestamp,
        output: &mut Output,
    ) {
        if term > output.persistent_state.current_term {
            output.persistent_state.current_term = term;
            output.persistent_state.voted_for = None;
            match self.role {
                // If leader stickiness is enabled, we don't want to revert to follower unless
                // we actually receive an append entries request, otherwise we'll start rejecting
                // other vote requests on the basis that the leader is still healthy.
                Role::Candidate(_) if !is_from_leader && self.common.config.leader_stickiness => {}
                Role::Learner | Role::Applicant(_) | Role::Follower => {}
                Role::Leader(_) | Role::Candidate(_) => self.become_follower(timestamp, output),
            }
        }
    }
    fn acknowledge_leader(&mut self, leader_id: NodeId, timestamp: Timestamp, output: &mut Output) {
        self.common.leader_id = Some(leader_id);
        if !self.role.is_learner() {
            self.become_follower(timestamp, output);
        }
    }
    fn become_follower(&mut self, timestamp: Timestamp, output: &mut Output) {
        self.role = Role::Follower;
        self.common.mark_not_leader();
        self.common.schedule_election_timeout(timestamp, output);
    }
    fn become_learner(&mut self) {
        self.role = Role::Learner;
        self.common.mark_not_leader();
        self.common.election_timeout = None;
    }
    fn become_applicant(&mut self, timestamp: Timestamp, output: &mut Output) {
        let current_membership = output.persistent_state.current_membership.clone();
        let mut election_state = ElectionState::new(current_membership.clone());

        // Vote for ourselves
        election_state.add_vote(self.common.this_id);

        self.role = Role::Applicant(election_state);
        self.common.mark_not_leader();
        self.common.schedule_election_timeout(timestamp, output);

        let pre_vote_request = PreVoteRequest {
            database_id: output.persistent_state.database_id,
            next_term: output.persistent_state.current_term.next(),
            candidate_id: self.common.this_id,
            last_log_index: output.persistent_state.last_log_index(),
            last_log_term: output.persistent_state.last_log_term(),
        };

        // Request a pre-vote from all voting nodes
        for (&node_id, &membership_type) in &current_membership.nodes {
            // Don't bother sending vote requests to learners
            if membership_type.is_voter_next || membership_type.is_voter_prev {
                self.common.send_message(
                    node_id,
                    output,
                    MessagePayload::PreVoteRequest(pre_vote_request.clone()),
                )
            }
        }
        output.schedule_tick(timestamp);
    }
    fn become_candidate(&mut self, timestamp: Timestamp, output: &mut Output) {
        output.persistent_state.current_term.inc();
        output.persistent_state.voted_for = Some(self.common.this_id);

        let current_membership = output.persistent_state.current_membership.clone();
        let mut election_state = ElectionState::new(current_membership.clone());

        // Vote for ourselves
        election_state.add_vote(self.common.this_id);

        self.role = Role::Candidate(election_state);
        self.common.mark_not_leader();
        self.common.schedule_election_timeout(timestamp, output);

        let vote_request = VoteRequest {
            database_id: output.persistent_state.database_id,
            term: output.persistent_state.current_term,
            candidate_id: self.common.this_id,
            last_log_index: output.persistent_state.last_log_index(),
            last_log_term: output.persistent_state.last_log_term(),
        };

        // Request a pre-vote from all voting nodes
        for (&node_id, &membership_type) in &current_membership.nodes {
            // Don't bother sending vote requests to learners
            if membership_type.is_voter_next || membership_type.is_voter_prev {
                self.common.send_message(
                    node_id,
                    output,
                    MessagePayload::VoteRequest(vote_request.clone()),
                )
            }
        }
        output.schedule_tick(timestamp);
    }
    fn become_leader(&mut self, timestamp: Timestamp, output: &mut Output) {
        self.role = Role::Leader(LeaderState::default());
        self.common.leader_id = Some(self.common.this_id);
        self.common.leader_commit_index = self.common.committed_index;
        output.schedule_tick(timestamp);
    }
    fn handle_append_entries_request(
        &mut self,
        from_id: NodeId,
        mut payload: AppendEntriesRequest,
        timestamp: Timestamp,
        output: &mut Output,
    ) -> Result<(), Error> {
        self.acknowledge_database_id(payload.database_id, output)?;
        self.acknowledge_term(payload.term, true, timestamp, output);

        // Ignore requests from old terms
        let resp = if payload.term != output.persistent_state.current_term {
            AppendEntriesResponse {
                match_index: None,
                term: output.persistent_state.current_term,
                conflict_opt: None,
            }
        } else {
            self.acknowledge_leader(from_id, timestamp, output);

            // Remove any entries prior to our commit index, as we know these match
            if payload.prev_log_index < self.common.committed_index && !payload.entries.is_empty() {
                let num_to_remove = cmp::min(
                    self.common.committed_index - payload.prev_log_index,
                    payload.entries.len() as u64,
                );
                payload.prev_log_term = payload.entries[num_to_remove as usize - 1].term;
                payload.prev_log_index += num_to_remove;
                payload.entries.drain(0..num_to_remove as usize);
            }

            // Figure out whether the remaining entries can be applied cleanly
            let (success, conflict_opt) = if output.persistent_state.last_log_term()
                == payload.prev_log_term
                && output.persistent_state.last_log_index() == payload.prev_log_index
            {
                // Happy path, new entries can just be appended
                (true, None)
            } else if payload.prev_log_index < self.common.committed_index {
                // There were no entries more recent than our commit index, so we know they all match
                assert!(payload.entries.is_empty());
                (true, None)
            } else if payload.prev_log_index < output.persistent_state.last_log_index() {
                // Sanity check that we haven't applied any uncommitted log entries. This also guarantees
                // that the subtraction below won't underflow.
                assert!(self.common.committed_index >= output.persistent_state.last_log_applied);

                // We need to check that the entries match our uncommitted entries
                let unapplied_offset =
                    payload.prev_log_index - output.persistent_state.last_log_applied;

                let expected_log_term = if unapplied_offset == 0 {
                    output.persistent_state.last_term_applied
                } else {
                    output.persistent_state.unapplied_log_terms[unapplied_offset as usize - 1]
                };

                if expected_log_term == payload.prev_log_term {
                    let num_matching = payload
                        .entries
                        .iter()
                        .zip(
                            output
                                .persistent_state
                                .unapplied_log_terms
                                .iter()
                                .copied()
                                .skip(unapplied_offset as usize),
                        )
                        .position(|(a, b)| a.term != b)
                        .unwrap_or(cmp::min(
                            output.persistent_state.unapplied_log_terms.len()
                                - unapplied_offset as usize,
                            payload.entries.len(),
                        ));

                    // Remove matching entries from the incoming request
                    if num_matching > 0 {
                        payload.prev_log_term = payload.entries[num_matching - 1].term;
                        payload.prev_log_index += num_matching as u64;
                        payload.entries.drain(0..num_matching);
                    }

                    (true, None)
                } else {
                    // New entries would conflict
                    (
                        false,
                        // Jump back to the most recent committed entry
                        Some(ConflictOpt {
                            index: self.common.committed_index,
                        }),
                    )
                }
            } else {
                // New entries are from the future, we need to fill in the gap
                (
                    false,
                    Some(ConflictOpt {
                        index: output.persistent_state.last_log_index(),
                    }),
                )
            };

            let mut match_index = None;
            if success {
                match_index = Some(payload.prev_log_index + payload.entries.len() as u64);
                if !payload.entries.is_empty() {
                    output
                        .persistent_state
                        .add_log_change(LogChange::Replicate(LogRange {
                            prev_log_index: payload.prev_log_index,
                            prev_log_term: payload.prev_log_term,
                            entries: payload
                                .entries
                                .into_iter()
                                .map(|entry| EntryFromClient {
                                    request_id: None,
                                    entry,
                                })
                                .collect(),
                        }));
                }

                // Advance the leader commit index. This may go ahead of our log entries,
                // but we won't advance our own commit index until we have those log entries.
                if payload.leader_commit > self.common.leader_commit_index {
                    self.common.leader_commit_index = payload.leader_commit;
                    output.schedule_tick(timestamp);
                }
            }

            AppendEntriesResponse {
                match_index,
                term: output.persistent_state.current_term,
                conflict_opt,
            }
        };
        self.common
            .send_message(from_id, output, MessagePayload::AppendEntriesResponse(resp));
        Ok(())
    }
    fn handle_append_entries_response(
        &mut self,
        from_id: NodeId,
        payload: AppendEntriesResponse,
        timestamp: Timestamp,
        output: &mut Output,
    ) -> Result<(), Error> {
        self.acknowledge_term(payload.term, false, timestamp, output);

        if let Role::Leader(leader_state) = &mut self.role {
            let mut changed_match_index = false;

            if let Some(replication_state) = leader_state.replication_state.get_mut(&from_id) {
                replication_state.in_flight_request = false;
                if let Some(match_index) = payload.match_index {
                    changed_match_index = true;
                    replication_state.match_index = match_index;
                    replication_state.send_after_index = match_index;
                } else if let Some(conflict) = payload.conflict_opt {
                    replication_state.send_after_index = conflict.index;
                } else if replication_state.send_after_index > LogIndex::ZERO {
                    replication_state.send_after_index -= 1;
                } else {
                    // Can't go back any further, just retry
                }
            }

            if changed_match_index {
                let mut match_indexes: Vec<_> = leader_state
                    .replication_state
                    .values()
                    .map(|rs| rs.match_index)
                    .collect();
                match_indexes.push(output.persistent_state.last_log_index());
                match_indexes.sort();
                let new_commit_index = match_indexes[match_indexes.len() / 2];
                if new_commit_index > self.common.leader_commit_index {
                    self.common.leader_commit_index = new_commit_index;
                    output.schedule_tick(timestamp);
                }
            }
        }
        Ok(())
    }

    fn should_betray_leader(&self) -> bool {
        // If leader stickiness is enabled, reject pre-votes unless
        // we haven't heard from the leader in a while.
        if self.common.config.leader_stickiness {
            match &self.role {
                Role::Follower | Role::Leader(_) => false,
                Role::Applicant(_) | Role::Candidate(_) | Role::Learner => true,
            }
        } else {
            true
        }
    }

    fn handle_vote_request(
        &mut self,
        from_id: NodeId,
        payload: VoteRequest,
        timestamp: Timestamp,
        output: &mut Output,
    ) -> Result<(), Error> {
        self.acknowledge_database_id(payload.database_id, output)?;
        self.acknowledge_term(payload.term, false, timestamp, output);

        let vote_granted = output
            .persistent_state
            .can_vote_for(payload.term, payload.candidate_id)
            && output
                .persistent_state
                .is_up_to_date(payload.last_log_term, payload.last_log_index)
            && self.should_betray_leader();

        self.common.send_message(
            from_id,
            output,
            MessagePayload::VoteResponse(VoteResponse {
                term: output.persistent_state.current_term,
                vote_granted,
            }),
        );

        Ok(())
    }

    fn handle_vote_response(
        &mut self,
        from_id: NodeId,
        payload: VoteResponse,
        timestamp: Timestamp,
        output: &mut Output,
    ) -> Result<(), Error> {
        self.acknowledge_term(payload.term, false, timestamp, output);

        if payload.term == output.persistent_state.current_term && payload.vote_granted {
            if let Role::Candidate(candidate) = &mut self.role {
                candidate.add_vote(from_id);
                if candidate.has_majority {
                    self.become_leader(timestamp, output);
                }
            }
        }

        Ok(())
    }

    fn handle_pre_vote_request(
        &mut self,
        from_id: NodeId,
        payload: PreVoteRequest,
        output: &mut Output,
    ) -> Result<(), Error> {
        self.acknowledge_database_id(payload.database_id, output)?;

        let vote_granted = payload.next_term >= output.persistent_state.current_term
            && output
                .persistent_state
                .is_up_to_date(payload.last_log_term, payload.last_log_index)
            && self.should_betray_leader();

        self.common.send_message(
            from_id,
            output,
            MessagePayload::PreVoteResponse(PreVoteResponse {
                term: output.persistent_state.current_term,
                vote_granted,
            }),
        );

        Ok(())
    }

    fn handle_pre_vote_response(
        &mut self,
        from_id: NodeId,
        payload: PreVoteResponse,
        timestamp: Timestamp,
        output: &mut Output,
    ) -> Result<(), Error> {
        self.acknowledge_term(payload.term, false, timestamp, output);

        if payload.term == output.persistent_state.current_term && payload.vote_granted {
            if let Role::Applicant(applicant) = &mut self.role {
                applicant.add_vote(from_id);
                if applicant.has_majority {
                    self.become_candidate(timestamp, output);
                }
            }
        }

        Ok(())
    }

    fn handle_message(
        &mut self,
        message: Message,
        timestamp: Timestamp,
        output: &mut Output,
    ) -> Result<(), Error> {
        match message.payload {
            MessagePayload::AppendEntriesRequest(payload) => {
                self.handle_append_entries_request(message.from_id, payload, timestamp, output)
            }
            MessagePayload::AppendEntriesResponse(payload) => {
                self.handle_append_entries_response(message.from_id, payload, timestamp, output)
            }
            MessagePayload::VoteRequest(payload) => {
                self.handle_vote_request(message.from_id, payload, timestamp, output)
            }
            MessagePayload::VoteResponse(payload) => {
                self.handle_vote_response(message.from_id, payload, timestamp, output)
            }
            MessagePayload::PreVoteRequest(payload) => {
                self.handle_pre_vote_request(message.from_id, payload, output)
            }
            MessagePayload::PreVoteResponse(payload) => {
                self.handle_pre_vote_response(message.from_id, payload, timestamp, output)
            }
            MessagePayload::InstallSnapshotRequest(_) => todo!(),
            MessagePayload::InstallSnapshotResponse(_) => todo!(),
        }
    }
    fn handle_client_request(
        &mut self,
        client_request: ClientRequest,
        timestamp: Timestamp,
        output: &mut Output,
    ) -> Result<(), RequestError> {
        match client_request.payload {
            ClientRequestPayload::Bootstrap(payload) => {
                self.bootstrap_cluster(client_request.request_id, payload, timestamp, output)
            }
            ClientRequestPayload::Application => todo!(),
            ClientRequestPayload::SetMembers(payload) => {
                self.set_members(client_request.request_id, payload, output)
            }
            ClientRequestPayload::SetLearners(payload) => {
                self.set_learners(client_request.request_id, payload, output)
            }
        }
    }
    fn internal_request(
        &mut self,
        payload: EntryPayload,
        request_id: Option<RequestId>,
        rate_limited: bool,
        output: &mut Output,
    ) -> Result<(), RequestError> {
        if self.role.is_leader() {
            if !rate_limited
                || (output.persistent_state.unapplied_log_terms.len() as u64)
                    < self.common.config.max_unapplied_entries
            {
                output
                    .persistent_state
                    .add_log_change(LogChange::Replicate(LogRange {
                        prev_log_index: output.persistent_state.last_log_index(),
                        prev_log_term: output.persistent_state.last_log_term(),
                        entries: vec![EntryFromClient {
                            request_id,
                            entry: Arc::new(Entry {
                                term: output.persistent_state.current_term,
                                payload,
                            }),
                        }],
                    }));
                Ok(())
            } else {
                // Too many in-flight requests already
                Err(RequestError::Busy)
            }
        } else {
            // Only the leader can respond to client requests
            Err(RequestError::NotLeader)
        }
    }
    fn bootstrap_cluster(
        &mut self,
        request_id: Option<RequestId>,
        payload: BootstrapRequest,
        timestamp: Timestamp,
        output: &mut Output,
    ) -> Result<(), RequestError> {
        let cluster_initialized = output.persistent_state.database_id.is_set()
            || output.persistent_state.last_log_index() != LogIndex::ZERO
            || output.persistent_state.voted_for.is_some();
        if cluster_initialized {
            return Err(RequestError::Bootstrap(
                BootstrapError::ClusterAlreadyInitialized,
            ));
        }
        if !payload.voter_ids.contains(&self.common.this_id) {
            return Err(RequestError::Bootstrap(BootstrapError::ThisNodeMustBeVoter));
        }

        output.persistent_state.database_id = payload.database_id;
        output.persistent_state.voted_for = Some(self.common.this_id);

        self.become_leader(timestamp, output);
        self.internal_request(
            EntryPayload::MembershipChange(Membership::bootstrap(
                payload.voter_ids,
                payload.learner_ids,
            )),
            request_id,
            false,
            output,
        )
    }
    fn set_members(
        &mut self,
        request_id: Option<RequestId>,
        payload: SetMembersRequest,
        output: &mut Output,
    ) -> Result<(), RequestError> {
        // Check destination state for validity
        if payload.member_ids.is_empty() {
            // Empty cluster is not allowed...
            return Err(RequestError::SetMembers(SetMembersError::InvalidMembers));
        }

        // Check that we are sufficiently fault tolerant
        let original_ids: BTreeSet<NodeId> = output
            .persistent_state
            .current_membership
            .nodes
            .iter()
            .filter(|(_, membership_type)| membership_type.is_voter_next)
            .map(|(&node_id, _)| node_id)
            .collect();

        let old_fault_tolerance = (original_ids.len() as u64 - 1) / 2;
        if old_fault_tolerance < payload.fault_tolerance {
            // Current cluster is too small to provide desired fault tolerance
            return Err(RequestError::SetMembers(
                SetMembersError::InsufficientFaultTolerance { proposed_ids: None },
            ));
        }

        let new_fault_tolerance = (payload.member_ids.len() as u64 - 1) / 2;
        if new_fault_tolerance < payload.fault_tolerance {
            // Requested cluster is too small to provide desired fault tolerance
            return Err(RequestError::SetMembers(
                SetMembersError::InsufficientFaultTolerance { proposed_ids: None },
            ));
        }

        // At this point we know the old and new clusters each have at least 3 nodes
        let static_ids = &original_ids & &payload.member_ids;

        // Check if we need to make the change in multiple steps
        let excessive_changes = (payload.fault_tolerance as i64) + 1 - (static_ids.len() as i64);
        if excessive_changes > 0 {
            let added_ids = &payload.member_ids - &original_ids;
            let removed_ids = &original_ids - &payload.member_ids;

            let proposed_ids = static_ids
                .into_iter()
                .chain(removed_ids.into_iter().take(excessive_changes as usize))
                .chain(added_ids.into_iter().skip(excessive_changes as usize))
                .collect();

            // Requested member change must be done in multiple steps
            return Err(RequestError::SetMembers(
                SetMembersError::InsufficientFaultTolerance {
                    proposed_ids: Some(proposed_ids),
                },
            ));
        }

        // Don't allow member changes whilst members are already changing
        if output.persistent_state.current_membership.is_changing() {
            return Err(RequestError::SetMembers(SetMembersError::AlreadyChanging));
        }

        // Check that sufficiently many nodes are up-to-date
        if let Role::Leader(leader_state) = &mut self.role {
            let lagging_ids: BTreeSet<NodeId> = payload
                .member_ids
                .iter()
                .copied()
                .filter(|&node_id| !leader_state.is_up_to_date(&self.common, node_id, output))
                .collect();
            let num_up_to_date = payload.member_ids.len() - lagging_ids.len();
            let min_up_to_date = num_up_to_date > 0
                && (((num_up_to_date - 1) / 2) as u64) >= payload.fault_tolerance;

            let allowed_by_cond = match self.common.config.membership_change_condition {
                MembershipChangeCondition::MinimumUpToDate => min_up_to_date,
                MembershipChangeCondition::NewUpToDate => {
                    min_up_to_date
                        && !lagging_ids.iter().any(|&lagging_id| {
                            output
                                .persistent_state
                                .current_membership
                                .is_learner_or_unknown(lagging_id)
                        })
                }
                MembershipChangeCondition::AllUpToDate => lagging_ids.is_empty(),
            };
            if !allowed_by_cond {
                // Too many lagging members
                return Err(RequestError::SetMembers(
                    SetMembersError::TooManyLaggingMembers { lagging_ids },
                ));
            }
        }

        // Everything is good to go, build the new membership configuration!
        let mut membership = output.persistent_state.current_membership.clone();
        membership.begin_change(payload.member_ids);

        self.internal_request(
            EntryPayload::MembershipChange(membership),
            request_id,
            true,
            output,
        )
    }
    fn set_learners(
        &mut self,
        request_id: Option<RequestId>,
        payload: SetLearnersRequest,
        output: &mut Output,
    ) -> Result<(), RequestError> {
        // For simplicity, don't allow learner changes whilst members are changing
        if output.persistent_state.current_membership.is_changing() {
            return Err(RequestError::SetLearners(SetLearnersError::AlreadyChanging));
        }

        // Don't allow members to be added as learners
        let existing_members: BTreeSet<NodeId> = payload
            .learner_ids
            .iter()
            .copied()
            .filter(|&node_id| {
                !output
                    .persistent_state
                    .current_membership
                    .is_learner_or_unknown(node_id)
            })
            .collect();

        if !existing_members.is_empty() {
            return Err(RequestError::SetLearners(
                SetLearnersError::ExistingMembers {
                    member_ids: existing_members,
                },
            ));
        }

        // All good, make the change
        let mut membership = output.persistent_state.current_membership.clone();
        membership.set_learners(payload.learner_ids);

        self.internal_request(
            EntryPayload::MembershipChange(membership),
            request_id,
            true,
            output,
        )
    }
    fn update_role_from_membership(&mut self, timestamp: Timestamp, output: &mut Output) {
        let applied_learner = output
            .persistent_state
            .last_membership_applied
            .is_learner_or_unknown(self.common.this_id);
        let current_learner = output
            .persistent_state
            .current_membership
            .is_learner_or_unknown(self.common.this_id);

        match (&self.role, applied_learner, current_learner) {
            // The Leader -> Learner transition is special, and doesn't occur until the membership
            // change is applied.
            (Role::Leader(_), true, true) => self.become_learner(),

            // Other transitions occur immediately.
            (Role::Candidate(_), _, true)
            | (Role::Follower, _, true)
            | (Role::Applicant(_), _, true) => self.become_learner(),
            (Role::Learner, _, false) => self.become_follower(timestamp, output),

            // Do nothing.
            _ => {}
        }
    }
    fn advance(&mut self, timestamp: Timestamp, output: &mut Output) {
        output.persistent_state.desired_log_entries.clear();

        // First, try to advance commit index
        let committed_index = cmp::min(
            self.common.leader_commit_index,
            output.persistent_state.last_log_index(),
        );
        while committed_index > self.common.committed_index {
            self.common.committed_index += 1;
            output
                .persistent_state
                .add_log_change(LogChange::Apply(self.common.committed_index));
        }

        // Update role in case of membership changes
        self.update_role_from_membership(timestamp, output);

        // Check for election timeout
        if self.common.election_timeout_elapsed(timestamp) {
            match &self.role {
                Role::Learner | Role::Leader(_) => unreachable!(),
                Role::Follower | Role::Applicant(_) if self.common.config.pre_vote => {
                    self.become_applicant(timestamp, output);
                }
                Role::Follower | Role::Applicant(_) | Role::Candidate(_) => {
                    self.become_candidate(timestamp, output);
                }
            }
        }

        // Execute leader logic
        if let Role::Leader(leader_state) = &mut self.role {
            let membership = output.persistent_state.current_membership.clone();

            // Synchronize replication state with current membership
            leader_state.replication_state.retain(|node_id, _| {
                membership.nodes.contains_key(node_id) && *node_id != self.common.this_id
            });

            for &node_id in membership.nodes.keys() {
                if node_id != self.common.this_id {
                    leader_state
                        .replication_state
                        .entry(node_id)
                        .or_insert(ReplicationState {
                            send_after_index: output.persistent_state.last_log_index(),
                            match_index: LogIndex::ZERO,
                            retry_at: None,
                            in_flight_request: false,
                            waiting_on_storage: false,
                        });
                }
            }

            // Update followers
            for (&node_id, replication_state) in &mut leader_state.replication_state {
                replication_state.waiting_on_storage = false;

                // Calculate number of log entries we still need to send to this node
                let unsent_entries =
                    output.persistent_state.last_log_index() - replication_state.send_after_index;

                // If the hearbeat timeout is up, or if we have unsent log entries and we don't have an
                // in-flight request, then we need to send one.
                if replication_state.should_retry(timestamp)
                    || (unsent_entries > 0 && !replication_state.in_flight_request)
                {
                    let num_to_send = cmp::min(unsent_entries, self.common.config.batch_size);

                    // Try to satisfy the request using log entries already in the cache
                    let mut entries = Vec::new();
                    let mut prev_log_index = LogIndex::ZERO;
                    let mut prev_log_term = Term(0);
                    for i in 0..=num_to_send {
                        let log_index = replication_state.send_after_index + i;
                        if let Some(entry) =
                            output.persistent_state.cached_log_entries.get(&log_index)
                        {
                            if i == 0 {
                                prev_log_index = log_index;
                                prev_log_term = entry.term;
                            } else {
                                entries.push(entry.clone());
                            }
                        } else {
                            replication_state.waiting_on_storage = true;
                            break;
                        }
                    }

                    if !replication_state.waiting_on_storage {
                        // If all the log entries we needed were present, then send the request
                        self.common.send_message(
                            node_id,
                            output,
                            MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                                database_id: output.persistent_state.database_id,
                                term: output.persistent_state.current_term,
                                prev_log_index,
                                prev_log_term,
                                entries,
                                leader_commit: self.common.committed_index,
                            }),
                        );
                        replication_state.in_flight_request = true;
                        replication_state.retry_at =
                            Some(timestamp + self.common.config.heartbeat_interval);
                    } else {
                        // Otherwise, we were missing an entry, so populate our desired set of
                        // log entries.
                        for i in 0..=num_to_send {
                            output
                                .persistent_state
                                .desired_log_entries
                                .insert(replication_state.send_after_index + i);
                        }
                    }
                }
                if !replication_state.waiting_on_storage {
                    if let Some(retry_at) = replication_state.retry_at {
                        output.schedule_tick(retry_at);
                    }
                }
            }

            // If we applied the first half of a membership change
            if output
                .persistent_state
                .last_membership_applied
                .is_changing()
                && output.persistent_state.current_membership.is_changing()
            {
                // Then trigger the second half
                let mut membership = output.persistent_state.last_membership_applied.clone();
                membership.complete_change();
                let _ = self.internal_request(
                    EntryPayload::MembershipChange(membership),
                    None,
                    false,
                    output,
                );
            }
        }
    }
}
