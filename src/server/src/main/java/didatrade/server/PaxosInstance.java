package didatrade.server;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import didatrade.server.request.CommandIdKind;

public class PaxosInstance {
	private int instance_nb;
	private int command_id;
	private int write_ballot;
	private int accepted_value;
	private int accept_ballot;
	private final LearnInfo learn_info = new LearnInfo ();
	private boolean decided;
	private boolean value_is_locked;
	private boolean executed;
	// This server's attempt to propose the slot.
	private final LocalProposal proposal = new LocalProposal ();

	public PaxosInstance() {
		this (0);
	}

	public PaxosInstance(int id) {
		this.instance_nb = id;
		this.command_id = 0;
		this.write_ballot = -1;
		this.accepted_value = 0;
		this.accept_ballot = -1;
		this.decided = false;
		this.value_is_locked = false;
		this.executed = false;
	}

	public synchronized boolean accept (int ballot, int value) {
		if (this.decided && value != this.command_id)
			return false;
		if (ballot == this.write_ballot && value != this.accepted_value)
			return false;
		this.accepted_value = value;
		this.write_ballot = ballot;
		return true;
	}

	public synchronized boolean isAccepted () {
		return this.write_ballot != -1;
	}

	public synchronized boolean isNoop () {
		return CommandIdKind.isNoop (this.command_id);
	}

	public synchronized int getInstanceNb () {
		return this.instance_nb;
	}

	public synchronized int getAcceptedValue () {
		return this.accepted_value;
	}

	public synchronized int getWriteBallot () {
		return this.write_ballot;
	}

	public synchronized AcceptedSnapshot getAcceptedSnapshot () {
		return new AcceptedSnapshot (this.accepted_value, this.write_ballot);
	}

	public synchronized boolean recordLearn (
			int ballot, int value, int server_id, int quorum) {
		if (!this.recordVote (ballot, value, server_id, quorum))
			return false;
		return this.updateInstanceState (quorum);
	}

	// Returns true once this vote is stored and its value has a quorum.
	private boolean recordVote (int ballot, int value, int server_id, int quorum) {
		if (this.decided && value != this.command_id)
			return false;
		if (!this.learn_info.recordVote (ballot, value, server_id))
			return false;
		return this.learn_info.acceptCount (ballot, value) >= quorum;
	}

	private boolean updateInstanceState (int quorum) {
		int highest_ballot = this.learn_info.findHighestBallotWithQuorum (quorum);
		if (highest_ballot < 0)
			return false;
		int chosen_value = this.learn_info.valueWithQuorum (highest_ballot, quorum);
		if (chosen_value == CommandIdKind.ABSENT_ID
				|| (this.decided && chosen_value != this.command_id))
			return false;
		boolean advanced = !this.decided || highest_ballot > this.accept_ballot;
		this.command_id = chosen_value;
		this.accept_ballot = Math.max (this.accept_ballot, highest_ballot);
		this.decided = true;
		return advanced;
	}

	public synchronized int findHighestBallotWithQuorum (int quorum) {
		return this.learn_info.findHighestBallotWithQuorum (quorum);
	}

	public synchronized int getAcceptCount (int ballot) {
		return this.learn_info.acceptCount (ballot);
	}

	public synchronized int getAcceptCount (int ballot, int value) {
		return this.learn_info.acceptCount (ballot, value);
	}

	public synchronized boolean cannotReachQuorum (int ballot, int quorum, int n_acceptors) {
		return this.learn_info.cannotReachQuorum (ballot, quorum, n_acceptors);
	}

	// Not proof that nothing was chosen: on a 3-1 split the missing acceptor may still
	// complete the quorum. Message from prefessor:
	// "A node automatically changes the ballot after receiving a fast quorum of “learns”
	// where no value has a supermajority." Therefore we either advance here or when we are already
	// sure a majority cannot be reached. Blocked states are possible, here the console must issue
	// a new ballot.
	public synchronized boolean fastRoundCollided (int ballot, int quorum, int n_acceptors) {
		return this.learn_info.acceptCount (ballot) < quorum
				&& (this.learn_info.responderCount (ballot) >= quorum
						|| this.learn_info.cannotReachQuorum (ballot, quorum, n_acceptors));
	}

	public synchronized int getProposedRequestId () {
		return this.proposal.proposed_request_id;
	}

	public synchronized int getValueToPropose () {
		return this.decided ? this.command_id : this.proposal.proposed_request_id;
	}

	public synchronized int getUnexecutedRequestId () {
		if (this.executed)
			return CommandIdKind.ABSENT_ID;
		if (this.decided)
			return this.command_id;
		return this.isAccepted () ? this.accepted_value : this.proposal.proposed_request_id;
	}

	public synchronized void setProposedRequestId (int request_id) {
		if (!this.decided)
			this.proposal.proposed_request_id = request_id;
	}

	// Only an actual successful Phase 2 changes acceptor-owned proposal bookkeeping.
	public synchronized void noteClassicAcceptance (int ballot, int value) {
		if (!this.decided)
			this.proposal.proposed_request_id = value;
	}

	public synchronized int phaseTwoValueFor (int ballot, int selected_value) {
		return this.proposal.phaseTwoValueFor (ballot, selected_value);
	}

	// A claim belongs to one local worker; it is unrelated to Paxos acceptance. Nothing can
	// change a decided slot, so it is re-proposed once per ballot and never retried; an
	// undecided one is retried as soon as its backoff expires.
	public synchronized boolean tryClaim (int ballot, long now_nanos) {
		if (this.proposal.claimed || CommandIdKind.isAbsent (this.getValueToPropose ()))
			return false;
		if (this.decided && this.accept_ballot >= ballot)
			return false;
		if (this.proposal.retry_ballot == ballot
				&& now_nanos < this.proposal.retry_after_nanos)
			return false;
		this.proposal.claimed = true;
		return true;
	}

	public synchronized long releaseClaim (int ballot, long retry_delay_nanos) {
		this.proposal.claimed = false;
		this.proposal.retry_ballot = ballot;
		this.proposal.retry_after_nanos = System.nanoTime () + retry_delay_nanos;
		return this.proposal.retry_after_nanos;
	}

	// -1 means no retry is scheduled; a positive result is a bounded wait.
	public synchronized long retryDelayMillis (int ballot, long now_nanos) {
		if (this.proposal.claimed || (this.decided && this.accept_ballot >= ballot)
				|| CommandIdKind.isAbsent (this.getValueToPropose ()))
			return -1;
		if (this.proposal.retry_ballot != ballot
				|| now_nanos >= this.proposal.retry_after_nanos)
			return 0;
		long remaining = this.proposal.retry_after_nanos - now_nanos;
		return Math.max (1, (remaining + 999_999) / 1_000_000);
	}

	public synchronized boolean decide (int ballot, int value) {
		if (this.decided) {
			// A successful classic reproposal of an already-chosen value confirms the
			// higher ballot even if its asynchronous Learns never arrive.
			if (this.command_id != value)
				return false;
			this.accept_ballot = Math.max (this.accept_ballot, ballot);
			return false;
		}
		this.command_id = value;
		this.accept_ballot = ballot;
		this.decided = true;
		return true;
	}

	public synchronized boolean isDecided () {
		return this.decided;
	}

	public synchronized int getCommandId () {
		return this.command_id;
	}

	public synchronized int getAcceptBallot () {
		return this.accept_ballot;
	}

	public synchronized boolean isExecuted () {
		return this.executed;
	}

	public synchronized void markExecuted () {
		this.executed = true;
	}

	private static final class LearnInfo {
		// Mapping ballot numbers to values to votes for that value
		private final Map<Integer, Map<Integer, LearnVotes>> learn_votes = new HashMap<> ();

		boolean recordVote (int ballot, int value, int server_id) {
			Map<Integer, LearnVotes> values = this.learn_votes.computeIfAbsent (
					ballot, ignored -> new HashMap<> ());
			for (LearnVotes votes : values.values ())
				if (votes.accepted_by.contains (server_id))
					return false;
			values.computeIfAbsent (value, ignored -> new LearnVotes ()).accepted_by.add (server_id);
			return true;
		}

		int acceptCount (int ballot) {
			Map<Integer, LearnVotes> values = this.learn_votes.get (ballot);
			if (values == null)
				return 0;
			int highest = 0;
			for (LearnVotes votes : values.values ())
				highest = Math.max (highest, votes.accepted_by.size ());
			return highest;
		}

		int acceptCount (int ballot, int value) {
			Map<Integer, LearnVotes> values = this.learn_votes.get (ballot);
			if (values == null || !values.containsKey (value))
				return 0;
			return values.get (value).accepted_by.size ();
		}

		int findHighestBallotWithQuorum (int quorum) {
			int result = -1;
			for (int ballot : this.learn_votes.keySet ())
				if (this.acceptCount (ballot) >= quorum && ballot > result)
					result = ballot;
			return result;
		}

		int valueWithQuorum (int ballot, int quorum) {
			Map<Integer, LearnVotes> values = this.learn_votes.get (ballot);
			if (values == null)
				return CommandIdKind.ABSENT_ID;
			for (Map.Entry<Integer, LearnVotes> entry : values.entrySet ())
				if (entry.getValue ().accepted_by.size () >= quorum)
					return entry.getKey ();
			return CommandIdKind.ABSENT_ID;
		}

		// recordVote() keeps one vote per acceptor, so the vote sets are disjoint.
		int responderCount (int ballot) {
			Map<Integer, LearnVotes> values = this.learn_votes.get (ballot);
			if (values == null)
				return 0;
			int responders = 0;
			for (LearnVotes votes : values.values ())
				responders += votes.accepted_by.size ();
			return responders;
		}

		boolean cannotReachQuorum (int ballot, int quorum, int n_acceptors) {
			Map<Integer, LearnVotes> values = this.learn_votes.get (ballot);
			if (values == null || values.isEmpty ())
				return false;
			int remaining = Math.max (0, n_acceptors - this.responderCount (ballot));
			return this.acceptCount (ballot) + remaining < quorum;
		}

		private static final class LearnVotes {
			private final Set<Integer> accepted_by = new HashSet<> ();
		}
	}

	private static final class LocalProposal {
		private int proposed_request_id = CommandIdKind.ABSENT_ID;
		private boolean claimed;
		private int retry_ballot = -1;
		private long retry_after_nanos;
		private int phase_two_ballot = -1;
		private int phase_two_value;

		// A ballot must never send two different values, including across retries.
		private int phaseTwoValueFor (int ballot, int selected_value) {
			if (ballot < this.phase_two_ballot)
				throw new IllegalStateException ("Cannot restart an older Phase 2 ballot");
			if (ballot > this.phase_two_ballot) {
				this.phase_two_ballot = ballot;
				this.phase_two_value = selected_value;
			}
			return this.phase_two_value;
		}
	}

	public static final class AcceptedSnapshot {
		public final int value;
		public final int valballot;

		AcceptedSnapshot(int value, int valballot) {
			this.value = value;
			this.valballot = valballot;
		}
	}
}
