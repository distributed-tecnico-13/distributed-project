package didatrade.server;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PaxosInstance {
	private int instance_nb;
	private int command_id;
	private int read_ballot;
	private int write_ballot;
	private int accepted_value;
	private int accept_ballot;
	private final Map<Integer, LearnVotes> learn_votes = new HashMap<> ();
	private boolean decided;
	private boolean value_is_locked;
	private boolean executed;
	// This server's attempt to propose the slot.
	private final LocalProposal proposal = new LocalProposal ();

	public PaxosInstance() {
		this (0, -1);
	}

	public PaxosInstance(int id) {
		this (id, -1);
	}

	public PaxosInstance(int id, int ballot) {
		this.instance_nb = id;
		this.command_id = 0;
		this.read_ballot = ballot;
		this.write_ballot = -1;
		this.accepted_value = 0;
		this.accept_ballot = -1;
		this.decided = false;
		this.value_is_locked = false;
		this.executed = false;
	}

	public synchronized PhaseOneState phaseOneState (
			boolean accepted, int ballot, int maxballot) {
		if (accepted)
			this.read_ballot = ballot;
		return new PhaseOneState (
				accepted, this.accepted_value, this.write_ballot, maxballot);
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
		return CommandIdHelper.isNoop (this.command_id);
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
		if (this.decided)
			return false;
		LearnVotes votes = this.learn_votes.get (ballot);
		if (votes == null) {
			votes = new LearnVotes (value);
			this.learn_votes.put (ballot, votes);
		}
		if (votes.value != value)
			return false;
		votes.accepted_by.add (server_id);
		if (votes.accepted_by.size () < quorum)
			return false;
		this.command_id = votes.value;
		this.accept_ballot = ballot;
		this.decided = true;
		return true;
	}

	public synchronized int getAcceptCount (int ballot) {
		LearnVotes votes = this.learn_votes.get (ballot);
		return votes == null ? 0 : votes.accepted_by.size ();
	}

	public synchronized int getProposedRequestId () {
		return this.proposal.proposed_request_id;
	}

	public synchronized int getUnexecutedRequestId () {
		if (this.executed)
			return CommandIdHelper.ABSENT_VALUE;
		return this.decided ? this.command_id : this.proposal.proposed_request_id;
	}

	public synchronized void setProposedRequestId (int request_id) {
		if (!this.decided)
			this.proposal.proposed_request_id = request_id;
	}

	public synchronized int phaseTwoValueFor (int ballot, int selected_value) {
		return this.proposal.phaseTwoValueFor (ballot, selected_value);
	}

	// A claim belongs to one local worker; it is unrelated to Paxos acceptance.
	public synchronized boolean tryClaim (int ballot, long now_nanos) {
		if (this.proposal.claimed || this.decided
				|| CommandIdHelper.isAbsent (this.proposal.proposed_request_id)
				|| (this.proposal.retry_ballot == ballot
						&& now_nanos < this.proposal.retry_after_nanos))
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
		if (this.proposal.claimed || this.decided
				|| CommandIdHelper.isAbsent (this.proposal.proposed_request_id))
			return -1;
		if (this.proposal.retry_ballot != ballot
				|| now_nanos >= this.proposal.retry_after_nanos)
			return 0;
		long remaining = this.proposal.retry_after_nanos - now_nanos;
		return Math.max (1, (remaining + 999_999) / 1_000_000);
	}

	public synchronized boolean decide (int ballot, int value) {
		if (this.decided)
			return false;
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

	private static final class LearnVotes {
		private final int value;
		private final Set<Integer> accepted_by = new HashSet<> ();

		private LearnVotes(int value) {
			this.value = value;
		}
	}

	private static final class LocalProposal {
		private int proposed_request_id = CommandIdHelper.ABSENT_VALUE;
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
		final int value;
		final int valballot;

		AcceptedSnapshot(int value, int valballot) {
			this.value = value;
			this.valballot = valballot;
		}
	}

	public static final class PhaseOneState {
		final boolean accepted;
		final int value;
		final int valballot;
		final int maxballot;

		PhaseOneState(boolean accepted, int value, int valballot, int maxballot) {
			this.accepted = accepted;
			this.value = value;
			this.valballot = valballot;
			this.maxballot = maxballot;
		}
	}
}
