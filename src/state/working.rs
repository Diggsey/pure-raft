use std::{
    cmp::{self, Ordering},
    collections::BTreeSet,
    sync::Arc,
};

use crate::{
    io::{
        client_requests::{
            BootstrapRequest, ClientRequest, ClientRequestPayload, SetLearnersRequest,
            SetMembersRequest,
        },
        errors::{BootstrapError, RequestError, SetLearnersError, SetMembersError},
        messages::ConflictOpt,
        Action, ApplyLogAction, ExtendLogAction, FailedRequest, LoadLogAction, TruncateLogAction,
    },
    AppendEntriesRequest, AppendEntriesResponse, Entry, EntryFromRequest, EntryPayload, Event,
    HardState, LogIndex, Membership, MembershipChangeCondition, Message, MessagePayload, NodeId,
    Output, PreVoteRequest, PreVoteResponse, RequestId, State, Term, Timestamp, VoteRequest,
    VoteResponse,
};

use super::{
    election::ElectionState, leader::LeaderState, overlay::OverlayState,
    replication::ReplicationState, role::Role, Error,
};

pub struct OriginalState {
    pub hard_state: HardState,
    pub last_log_index: LogIndex,
}

pub struct WorkingState<'a, D> {
    pub overlay: OverlayState<'a, D>,
    pub role: &'a mut Role,
    pub original: OriginalState,
}

impl<'a, D> WorkingState<'a, D> {
    pub fn new(state: &'a mut State<D>, timestamp: Timestamp) -> Self {
        let committed_index = state.common.last_applied_log_index;
        let hard_state = state.common.hard_state.clone();
        let last_log_index = state.common.last_log_index();
        Self {
            overlay: OverlayState {
                config: &state.config,
                common: &mut state.common,
                timestamp,
                desired_log_entries: Default::default(),
                truncate_log_to: last_log_index,
                append_log_entries: Vec::new(),
                committed_index,
                messages: Vec::new(),
                failed_requests: Vec::new(),
                changed_match_index: false,
            },
            role: &mut state.role,
            original: OriginalState {
                hard_state,
                last_log_index,
            },
        }
    }

    pub fn handle(&mut self, event: Event<D>) {
        match event {
            Event::Tick => {}
            Event::LoadedLog(loaded_log) => {
                // Incorporate the loaded log entries into our local store
                self.overlay.common.loaded_log_entries.extend(
                    loaded_log
                        .entries
                        .into_iter()
                        .filter(|(k, _)| self.overlay.common.requested_log_entries.contains(k)),
                );
            }
            Event::ReceivedMessage(message) => {
                if let Err(e) = self.handle_message(message) {
                    eprintln!("{:?}", e);
                }
            }
            Event::ClientRequest(client_request) => {
                let request_id = client_request.request_id;
                if let Err(error) = self.handle_client_request(client_request) {
                    if let Some(request_id) = request_id {
                        self.overlay
                            .failed_requests
                            .push(FailedRequest { request_id, error });
                    }
                }
            }
        };
        self.advance();
    }
    fn update_role_from_membership(&mut self) {
        let applied_learner = self
            .overlay
            .common
            .last_applied_membership
            .is_learner_or_unknown(self.overlay.common.this_id);
        let current_learner = self
            .overlay
            .common
            .current_membership()
            .is_learner_or_unknown(self.overlay.common.this_id);

        match (&self.role, applied_learner, current_learner) {
            // The Leader -> Learner transition is special, and doesn't occur until the membership
            // change is applied.
            (Role::Leader(_), true, true) => self.become_learner(),

            // Other transitions occur immediately.
            (Role::Candidate(_), _, true)
            | (Role::Follower, _, true)
            | (Role::Applicant(_), _, true) => self.become_learner(),
            (Role::Learner, _, false) => self.become_follower(),

            // Do nothing.
            _ => {}
        }
    }
    fn advance(&mut self) {
        // Update role in case of membership changes
        self.update_role_from_membership();

        // Check for election timeout
        if self.overlay.election_timeout_elapsed() {
            match &self.role {
                Role::Learner | Role::Leader(_) => unreachable!(),
                Role::Follower | Role::Applicant(_) if self.overlay.config.pre_vote => {
                    self.become_applicant();
                }
                Role::Follower | Role::Applicant(_) | Role::Candidate(_) => {
                    self.become_candidate();
                }
            }
        }

        // Execute leader logic
        if let Role::Leader(leader_state) = self.role {
            // Check if we can advance commit index
            if self.overlay.changed_match_index {
                // Compute the median match index for each membership group. This is the latest log
                // entry we could possibly commit if all other criteria are met.
                let majority_match_index = leader_state.compute_median_match_index(&self.overlay);

                // We can't commit entries that we don't have on the leader
                let mut candidate_commit_index =
                    cmp::min(majority_match_index, self.overlay.common.last_log_index());

                // Walk backwards looking for an entry from the current term that we
                // can commit.
                while candidate_commit_index > self.overlay.committed_index {
                    let offset =
                        candidate_commit_index - self.overlay.common.last_applied_log_index - 1;
                    match self.overlay.common.unapplied_log_terms[offset as usize]
                        .cmp(&self.overlay.common.hard_state.current_term)
                    {
                        // If the current candidate's term is less than the current term, then
                        // we can give up early, as we won't find one that's equal.
                        Ordering::Less => break,
                        // If we find an entry that has the correct term, then advance our
                        // commit index to that entry.
                        Ordering::Equal => {
                            self.overlay.committed_index = candidate_commit_index;
                            break;
                        }
                        // There shouldn't be any entries from a future term if we are the leader.
                        Ordering::Greater => {}
                    };
                    candidate_commit_index -= 1;
                }
            }

            let membership = self.overlay.common.current_membership();

            // Synchronize replication state with current membership
            leader_state.replication_state.retain(|node_id, _| {
                membership.nodes.contains_key(node_id) && *node_id != self.overlay.common.this_id
            });

            for &node_id in membership.nodes.keys() {
                if node_id != self.overlay.common.this_id {
                    leader_state
                        .replication_state
                        .entry(node_id)
                        .or_insert(ReplicationState {
                            send_after_index: self.original.last_log_index,
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
                    self.overlay.common.last_log_index() - replication_state.send_after_index;

                // If the hearbeat timeout is up, or if we have unsent log entries and we don't have an
                // in-flight request, then we need to send one.
                if replication_state.should_retry(self.overlay.timestamp)
                    || (unsent_entries > 0 && !replication_state.in_flight_request)
                {
                    let num_to_send = cmp::min(unsent_entries, self.overlay.config.batch_size);

                    let prev_log_index = replication_state.send_after_index;

                    // Try to get the term of the previous log entry
                    let maybe_prev_log_term =
                        if prev_log_index >= self.overlay.common.last_applied_log_index {
                            Some(
                                if prev_log_index == self.overlay.common.last_applied_log_index {
                                    self.overlay.common.last_applied_log_term
                                } else {
                                    self.overlay.common.unapplied_log_terms[(prev_log_index
                                        - self.overlay.common.last_applied_log_index
                                        - 1)
                                        as usize]
                                },
                            )
                        } else if let Some(entry) =
                            self.overlay.common.loaded_log_entries.get(&prev_log_index)
                        {
                            Some(entry.term)
                        } else {
                            replication_state.waiting_on_storage = true;
                            None
                        };

                    // Try to satisfy the request using log entries already in the cache
                    let mut entries = Vec::new();
                    for i in 0..num_to_send {
                        let log_index = prev_log_index + i + 1;
                        if let Some(entry) = self.overlay.common.loaded_log_entries.get(&log_index)
                        {
                            entries.push(entry.clone());
                        } else {
                            replication_state.waiting_on_storage = true;
                            break;
                        }
                    }

                    if !replication_state.waiting_on_storage {
                        // If all the log entries we needed were present, then send the request
                        self.overlay.send_message(
                            node_id,
                            MessagePayload::AppendEntriesRequest(AppendEntriesRequest {
                                database_id: self.overlay.common.hard_state.database_id,
                                term: self.overlay.common.hard_state.current_term,
                                prev_log_index,
                                prev_log_term: maybe_prev_log_term
                                    .expect("To be set if not waiting on storage"),
                                entries,
                                leader_commit: self.overlay.committed_index,
                            }),
                        );
                        replication_state.in_flight_request = true;
                        replication_state.retry_at =
                            Some(self.overlay.timestamp + self.overlay.config.heartbeat_interval);
                    } else {
                        // Otherwise, we were missing an entry, so populate our desired set of
                        // log entries.
                        if maybe_prev_log_term.is_none() {
                            self.overlay.desired_log_entries.insert(prev_log_index);
                        }
                        for i in 0..num_to_send {
                            self.overlay
                                .desired_log_entries
                                .insert(replication_state.send_after_index + i + 1);
                        }
                    }
                }
            }

            // If we applied the first half of a membership change, and there are no further membership changes proposed
            if self.overlay.common.last_applied_membership.is_changing()
                && self.overlay.common.unapplied_membership_changes.is_empty()
            {
                // Then trigger the second half of the membership change
                let mut new_membership = self.overlay.common.last_applied_membership.clone();
                new_membership.complete_change();
                let _ = self.internal_request(
                    EntryPayload::MembershipChange(new_membership),
                    None,
                    false,
                );
            }
        }
    }

    fn acknowledge_term(&mut self, term: Term, is_from_leader: bool) {
        if term > self.overlay.common.hard_state.current_term {
            self.overlay.common.hard_state.current_term = term;
            self.overlay.common.hard_state.voted_for = None;
            match self.role {
                // If leader stickiness is enabled, we don't want to revert to follower unless
                // we actually receive an append entries request, otherwise we'll start rejecting
                // other vote requests on the basis that the leader is still healthy.
                Role::Candidate(_) if !is_from_leader && self.overlay.config.leader_stickiness => {}
                Role::Learner | Role::Applicant(_) | Role::Follower => {}
                Role::Leader(_) | Role::Candidate(_) => self.become_follower(),
            }
        }
    }
    fn acknowledge_leader(&mut self, leader_id: NodeId) {
        self.overlay.common.leader_id = Some(leader_id);
        if !self.role.is_learner() && leader_id != self.overlay.common.this_id {
            self.become_follower();
        }
    }
    fn become_follower(&mut self) {
        *self.role = Role::Follower;
        self.overlay.common.mark_not_leader();
        self.overlay.schedule_election_timeout();
    }
    fn become_learner(&mut self) {
        *self.role = Role::Learner;
        self.overlay.common.mark_not_leader();
        self.overlay.common.election_timeout = None;
    }
    fn become_applicant(&mut self) {
        let current_membership = self.overlay.common.current_membership().clone();
        let mut election_state = ElectionState::new(current_membership.clone());

        // Vote for ourselves
        election_state.add_vote(self.overlay.common.this_id);

        *self.role = Role::Applicant(election_state);
        self.overlay.common.mark_not_leader();
        self.overlay.schedule_election_timeout();

        let pre_vote_request = PreVoteRequest {
            database_id: self.overlay.common.hard_state.database_id,
            next_term: self.overlay.common.hard_state.current_term.next(),
            candidate_id: self.overlay.common.this_id,
            last_log_index: self.overlay.common.last_log_index(),
            last_log_term: self.overlay.common.last_log_term(),
        };

        // Request a pre-vote from all voting nodes
        for (&node_id, &membership_type) in &current_membership.nodes {
            // Don't send messages to ourself
            if node_id != self.overlay.common.this_id {
                // Don't bother sending vote requests to learners
                if membership_type.is_voter_next || membership_type.is_voter_prev {
                    self.overlay.send_message(
                        node_id,
                        MessagePayload::PreVoteRequest(pre_vote_request.clone()),
                    )
                }
            }
        }
    }
    fn become_candidate(&mut self) {
        self.overlay.common.hard_state.current_term.inc();
        self.overlay.common.hard_state.voted_for = Some(self.overlay.common.this_id);

        let current_membership = self.overlay.common.current_membership().clone();
        let mut election_state = ElectionState::new(current_membership.clone());

        // Vote for ourselves
        election_state.add_vote(self.overlay.common.this_id);

        *self.role = Role::Candidate(election_state);
        self.overlay.common.mark_not_leader();
        self.overlay.schedule_election_timeout();

        let vote_request = VoteRequest {
            database_id: self.overlay.common.hard_state.database_id,
            term: self.overlay.common.hard_state.current_term,
            candidate_id: self.overlay.common.this_id,
            last_log_index: self.overlay.common.last_log_index(),
            last_log_term: self.overlay.common.last_log_term(),
        };

        // Request a pre-vote from all voting nodes
        for (&node_id, &membership_type) in &current_membership.nodes {
            // Don't send messages to ourself
            if node_id != self.overlay.common.this_id {
                // Don't bother sending vote requests to learners
                if membership_type.is_voter_next || membership_type.is_voter_prev {
                    self.overlay
                        .send_message(node_id, MessagePayload::VoteRequest(vote_request.clone()))
                }
            }
        }
    }
    fn become_leader(&mut self) {
        *self.role = Role::Leader(LeaderState::default());
        self.acknowledge_leader(self.overlay.common.this_id);
        self.overlay.common.election_timeout = None;

        // Request should be infallible because we disable rate limiting and we know we are the leader
        self.internal_request(EntryPayload::Blank, None, false)
            .expect("Infallible");
    }
    fn handle_append_entries_request(
        &mut self,
        from_id: NodeId,
        mut payload: AppendEntriesRequest<D>,
    ) -> Result<(), Error> {
        self.overlay
            .common
            .hard_state
            .acknowledge_database_id(payload.database_id)?;
        self.acknowledge_term(payload.term, true);

        // Ignore requests from old terms
        let resp = if payload.term != self.overlay.common.hard_state.current_term {
            AppendEntriesResponse {
                match_index: None,
                term: self.overlay.common.hard_state.current_term,
                conflict_opt: None,
            }
        } else {
            self.acknowledge_leader(from_id);

            // Remove any entries prior to our commit index, as we know these match
            if payload.prev_log_index < self.overlay.committed_index && !payload.entries.is_empty()
            {
                let num_to_remove = cmp::min(
                    self.overlay.committed_index - payload.prev_log_index,
                    payload.entries.len() as u64,
                );
                payload.prev_log_term = payload.entries[num_to_remove as usize - 1].term;
                payload.prev_log_index += num_to_remove;
                payload.entries.drain(0..num_to_remove as usize);
            }

            // Figure out whether the remaining entries can be applied cleanly
            let (success, conflict_opt) = if self.overlay.common.last_log_term()
                == payload.prev_log_term
                && self.overlay.common.last_log_index() == payload.prev_log_index
            {
                // Happy path, new entries can just be appended
                (true, None)
            } else if payload.prev_log_index < self.overlay.committed_index {
                // There were no entries more recent than our commit index, so we know they all match
                assert!(payload.entries.is_empty());
                (true, None)
            } else if payload.prev_log_index <= self.overlay.common.last_log_index() {
                // We need to check that the entries match our uncommitted entries
                let unapplied_offset =
                    (payload.prev_log_index - self.overlay.common.last_applied_log_index) as usize;

                let expected_log_term = if unapplied_offset == 0 {
                    self.overlay.common.last_applied_log_term
                } else {
                    self.overlay.common.unapplied_log_terms[unapplied_offset - 1]
                };

                if expected_log_term == payload.prev_log_term {
                    let num_matching = payload
                        .entries
                        .iter()
                        .zip(
                            self.overlay
                                .common
                                .unapplied_log_terms
                                .range(unapplied_offset..)
                                .copied(),
                        )
                        .position(|(a, b)| a.term != b)
                        .unwrap_or(cmp::min(
                            self.overlay.common.unapplied_log_terms.len() - unapplied_offset,
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
                            index: self.overlay.committed_index,
                        }),
                    )
                }
            } else {
                // New entries are from the future, we need to fill in the gap
                (
                    false,
                    Some(ConflictOpt {
                        index: self.overlay.common.last_log_index(),
                    }),
                )
            };

            let mut match_index = None;
            if success {
                match_index = Some(payload.prev_log_index + payload.entries.len() as u64);
                if !payload.entries.is_empty() {
                    self.overlay.truncate_log_to(payload.prev_log_index);
                    self.overlay
                        .extend_log(payload.entries.into_iter().map(|entry| EntryFromRequest {
                            request_id: None,
                            entry,
                        }));
                }

                // Advance our commit index. The leader commit index may go ahead of our log entries,
                // but we won't advance our own commit index until we have those log entries.
                if payload.leader_commit > self.overlay.committed_index {
                    self.overlay.committed_index =
                        cmp::min(payload.leader_commit, self.overlay.common.last_log_index());
                }
            }

            AppendEntriesResponse {
                match_index,
                term: self.overlay.common.hard_state.current_term,
                conflict_opt,
            }
        };
        self.overlay
            .send_message(from_id, MessagePayload::AppendEntriesResponse(resp));
        Ok(())
    }
    fn handle_append_entries_response(
        &mut self,
        from_id: NodeId,
        payload: AppendEntriesResponse,
    ) -> Result<(), Error> {
        self.acknowledge_term(payload.term, false);

        if let Role::Leader(leader_state) = self.role {
            if let Some(replication_state) = leader_state.replication_state.get_mut(&from_id) {
                replication_state.in_flight_request = false;
                if let Some(match_index) = payload.match_index {
                    self.overlay.changed_match_index = true;
                    replication_state.match_index = match_index;
                    replication_state.send_after_index = match_index;
                } else if let Some(conflict) = payload.conflict_opt {
                    replication_state.send_after_index = conflict.index;
                } else {
                    // This can only happen if the corresponding "append entries" requst
                    // was from a previous term. In that case, ignore it.
                }
            }
        }
        Ok(())
    }

    fn should_betray_leader(&self) -> bool {
        // If leader stickiness is enabled, reject pre-votes unless
        // we haven't heard from the leader in a while.
        if self.overlay.config.leader_stickiness {
            match self.role {
                Role::Follower | Role::Leader(_) => false,
                Role::Applicant(_) | Role::Candidate(_) | Role::Learner => true,
            }
        } else {
            true
        }
    }

    fn handle_vote_request(&mut self, from_id: NodeId, payload: VoteRequest) -> Result<(), Error> {
        self.overlay
            .common
            .hard_state
            .acknowledge_database_id(payload.database_id)?;
        self.acknowledge_term(payload.term, false);

        let vote_granted = self
            .overlay
            .common
            .hard_state
            .can_vote_for(payload.term, payload.candidate_id)
            && self
                .overlay
                .common
                .is_up_to_date(payload.last_log_term, payload.last_log_index)
            && self.should_betray_leader();

        self.overlay.send_message(
            from_id,
            MessagePayload::VoteResponse(VoteResponse {
                term: self.overlay.common.hard_state.current_term,
                vote_granted,
            }),
        );

        if vote_granted {
            self.overlay.common.hard_state.voted_for = Some(from_id);
            // If we are a follower or applicant and we granted the vote, then reset
            // our election timeout.
            if matches!(self.role, Role::Follower | Role::Applicant(_)) {
                self.become_follower();
            }
        }

        Ok(())
    }

    fn handle_vote_response(
        &mut self,
        from_id: NodeId,
        payload: VoteResponse,
    ) -> Result<(), Error> {
        self.acknowledge_term(payload.term, false);

        if payload.term == self.overlay.common.hard_state.current_term && payload.vote_granted {
            if let Role::Candidate(candidate) = self.role {
                candidate.add_vote(from_id);
                if candidate.has_majority {
                    self.become_leader();
                }
            }
        }

        Ok(())
    }

    fn handle_pre_vote_request(
        &mut self,
        from_id: NodeId,
        payload: PreVoteRequest,
    ) -> Result<(), Error> {
        self.overlay
            .common
            .hard_state
            .acknowledge_database_id(payload.database_id)?;

        let vote_granted = payload.next_term >= self.overlay.common.hard_state.current_term
            && self
                .overlay
                .common
                .is_up_to_date(payload.last_log_term, payload.last_log_index)
            && self.should_betray_leader();

        self.overlay.send_message(
            from_id,
            MessagePayload::PreVoteResponse(PreVoteResponse {
                term: self.overlay.common.hard_state.current_term,
                vote_granted,
            }),
        );

        Ok(())
    }

    fn handle_pre_vote_response(
        &mut self,
        from_id: NodeId,
        payload: PreVoteResponse,
    ) -> Result<(), Error> {
        self.acknowledge_term(payload.term, false);

        if payload.term == self.overlay.common.hard_state.current_term && payload.vote_granted {
            if let Role::Applicant(applicant) = self.role {
                applicant.add_vote(from_id);
                if applicant.has_majority {
                    self.become_candidate();
                }
            }
        }

        Ok(())
    }

    fn handle_message(&mut self, message: Message<D>) -> Result<(), Error> {
        match message.payload {
            MessagePayload::AppendEntriesRequest(payload) => {
                self.handle_append_entries_request(message.from_id, payload)
            }
            MessagePayload::AppendEntriesResponse(payload) => {
                self.handle_append_entries_response(message.from_id, payload)
            }
            MessagePayload::VoteRequest(payload) => {
                self.handle_vote_request(message.from_id, payload)
            }
            MessagePayload::VoteResponse(payload) => {
                self.handle_vote_response(message.from_id, payload)
            }
            MessagePayload::PreVoteRequest(payload) => {
                self.handle_pre_vote_request(message.from_id, payload)
            }
            MessagePayload::PreVoteResponse(payload) => {
                self.handle_pre_vote_response(message.from_id, payload)
            }
            MessagePayload::InstallSnapshotRequest(_) => todo!(),
            MessagePayload::InstallSnapshotResponse(_) => todo!(),
        }
    }
    fn handle_client_request(
        &mut self,
        client_request: ClientRequest<D>,
    ) -> Result<(), RequestError> {
        match client_request.payload {
            ClientRequestPayload::Bootstrap(payload) => {
                self.bootstrap_cluster(client_request.request_id, payload)
            }
            ClientRequestPayload::Application(data) => self.internal_request(
                EntryPayload::Application(data),
                client_request.request_id,
                true,
            ),
            ClientRequestPayload::SetMembers(payload) => {
                self.set_members(client_request.request_id, payload)
            }
            ClientRequestPayload::SetLearners(payload) => {
                self.set_learners(client_request.request_id, payload)
            }
        }
    }
    fn internal_request(
        &mut self,
        payload: EntryPayload<D>,
        request_id: Option<RequestId>,
        rate_limited: bool,
    ) -> Result<(), RequestError> {
        if self.role.is_leader() {
            if !rate_limited
                || (self.overlay.common.unapplied_log_terms.len() as u64)
                    < self.overlay.config.max_unapplied_entries
            {
                self.overlay.extend_log([EntryFromRequest {
                    request_id,
                    entry: Arc::new(Entry {
                        term: self.overlay.common.hard_state.current_term,
                        payload,
                    }),
                }]);
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
    ) -> Result<(), RequestError> {
        let cluster_initialized = self.overlay.common.hard_state.database_id.is_set()
            || self.overlay.common.last_log_index() != LogIndex::ZERO
            || self.overlay.common.hard_state.voted_for.is_some();
        if cluster_initialized {
            return Err(RequestError::Bootstrap(
                BootstrapError::ClusterAlreadyInitialized,
            ));
        }
        if !payload.voter_ids.contains(&self.overlay.common.this_id) {
            return Err(RequestError::Bootstrap(BootstrapError::ThisNodeMustBeVoter));
        }

        self.overlay.common.hard_state.database_id = payload.database_id;
        self.overlay.common.hard_state.voted_for = Some(self.overlay.common.this_id);

        self.become_leader();
        self.internal_request(
            EntryPayload::MembershipChange(Membership::bootstrap(
                payload.voter_ids,
                payload.learner_ids,
            )),
            request_id,
            false,
        )
    }
    fn set_members(
        &mut self,
        request_id: Option<RequestId>,
        payload: SetMembersRequest,
    ) -> Result<(), RequestError> {
        // Check destination state for validity
        if payload.member_ids.is_empty() {
            // Empty cluster is not allowed...
            return Err(RequestError::SetMembers(SetMembersError::InvalidMembers));
        }

        let current_membership = self.overlay.common.current_membership();

        // Check that we are sufficiently fault tolerant
        let original_ids: BTreeSet<NodeId> = current_membership
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
        if current_membership.is_changing() {
            return Err(RequestError::SetMembers(SetMembersError::AlreadyChanging));
        }

        // Check that sufficiently many nodes are up-to-date
        if let Role::Leader(leader_state) = self.role {
            let lagging_ids: BTreeSet<NodeId> = payload
                .member_ids
                .iter()
                .copied()
                .filter(|&node_id| !leader_state.is_up_to_date(&self.overlay, node_id))
                .collect();
            let num_up_to_date = payload.member_ids.len() - lagging_ids.len();
            let min_up_to_date = num_up_to_date > 0
                && (((num_up_to_date - 1) / 2) as u64) >= payload.fault_tolerance;

            let allowed_by_cond = match self.overlay.config.membership_change_condition {
                MembershipChangeCondition::MinimumUpToDate => min_up_to_date,
                MembershipChangeCondition::NewUpToDate => {
                    min_up_to_date
                        && !lagging_ids
                            .iter()
                            .any(|&lagging_id| current_membership.is_learner_or_unknown(lagging_id))
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
        let mut membership = current_membership.clone();
        membership.begin_change(payload.member_ids);

        self.internal_request(EntryPayload::MembershipChange(membership), request_id, true)
    }
    fn set_learners(
        &mut self,
        request_id: Option<RequestId>,
        payload: SetLearnersRequest,
    ) -> Result<(), RequestError> {
        let current_membership = self.overlay.common.current_membership();

        // For simplicity, don't allow learner changes whilst members are changing
        if current_membership.is_changing() {
            return Err(RequestError::SetLearners(SetLearnersError::AlreadyChanging));
        }

        // Don't allow members to be added as learners
        let existing_members: BTreeSet<NodeId> = payload
            .learner_ids
            .iter()
            .copied()
            .filter(|&node_id| !current_membership.is_learner_or_unknown(node_id))
            .collect();

        if !existing_members.is_empty() {
            return Err(RequestError::SetLearners(
                SetLearnersError::ExistingMembers {
                    member_ids: existing_members,
                },
            ));
        }

        // All good, make the change
        let mut membership = current_membership.clone();
        membership.set_learners(payload.learner_ids);

        self.internal_request(EntryPayload::MembershipChange(membership), request_id, true)
    }
}

impl<D> From<WorkingState<'_, D>> for Output<D> {
    fn from(working: WorkingState<'_, D>) -> Self {
        let mut actions = Vec::new();

        // Detect log truncation
        if working.overlay.truncate_log_to < working.original.last_log_index {
            actions.push(Action::TruncateLog(TruncateLogAction {
                last_log_index: working.overlay.truncate_log_to,
            }));
        }

        // Detect log extension
        if !working.overlay.append_log_entries.is_empty() {
            actions.push(Action::ExtendLog(ExtendLogAction {
                entries: working.overlay.append_log_entries,
            }));
        }

        // Detect hard state change
        if working.overlay.common.hard_state != working.original.hard_state {
            actions.push(Action::SaveState(working.overlay.common.hard_state.clone()));
        }

        // Detect sent messages
        if !working.overlay.messages.is_empty() {
            actions.extend(
                working
                    .overlay
                    .messages
                    .into_iter()
                    .map(Action::SendMessage),
            );
        }

        // Detect failed requests
        if !working.overlay.failed_requests.is_empty() {
            actions.extend(
                working
                    .overlay
                    .failed_requests
                    .into_iter()
                    .map(Action::FailedRequest),
            );
        }

        // Detect log entries to be applied
        if working.overlay.committed_index > working.overlay.common.last_applied_log_index {
            working
                .overlay
                .common
                .apply_up_to(working.overlay.committed_index);
            actions.push(Action::ApplyLog(ApplyLogAction {
                up_to_log_index: working.overlay.common.last_applied_log_index,
            }));
        }

        // Detect log entries to be loaded
        let log_entries_to_request: BTreeSet<_> = working
            .overlay
            .desired_log_entries
            .difference(&working.overlay.common.requested_log_entries)
            .copied()
            .collect();
        if !log_entries_to_request.is_empty() {
            working
                .overlay
                .common
                .requested_log_entries
                .extend(log_entries_to_request.iter().copied());
            actions.push(Action::LoadLog(LoadLogAction {
                desired_entries: log_entries_to_request,
            }));
        }

        // Free no-longer-required log entries
        let log_entries_to_free: BTreeSet<_> = working
            .overlay
            .common
            .requested_log_entries
            .difference(&working.overlay.desired_log_entries)
            .copied()
            .collect();
        if !log_entries_to_free.is_empty() {
            for log_index in log_entries_to_free {
                working
                    .overlay
                    .common
                    .requested_log_entries
                    .remove(&log_index);
                working.overlay.common.loaded_log_entries.remove(&log_index);
            }
        }

        // Determine when the state machine should be ticked if no other events happen
        let mut next_tick = None;
        let mut schedule_tick = |maybe_timestamp| {
            if let Some(timestamp) = maybe_timestamp {
                if let Some(prev_timestamp) = next_tick {
                    if timestamp < prev_timestamp {
                        next_tick = Some(timestamp);
                    }
                } else {
                    next_tick = Some(timestamp);
                }
            }
        };

        schedule_tick(working.overlay.common.election_timeout);
        if let Role::Leader(leader_state) = working.role {
            for replication_state in leader_state.replication_state.values() {
                if !replication_state.waiting_on_storage {
                    schedule_tick(replication_state.retry_at);
                }
            }
        }

        Self { actions, next_tick }
    }
}
