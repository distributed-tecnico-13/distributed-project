package didatrade.server;

import java.util.HashSet;
import java.util.Set;

public class PaxosInstance {
	int instance_nb;
	private int command_id;
	int read_ballot;
	private int write_ballot;
	private int accepted_value;
	private int accept_ballot;
	private int learned_value;
	private final Set<Integer> accepted_by = new HashSet<> ();
	private boolean decided;
	boolean value_is_locked;
	private boolean executed;
	// This server's attempt to propose the slot.
	private final LocalProposal proposal = new LocalProposal ();

	public PaxosInstance() {
		this.instance_nb = 0;
		this.command_id = 0;
		this.read_ballot = -1;
		this.write_ballot = -1;
		this.accepted_value = 0;
		this.accept_ballot = -1;
		this.learned_value = 0;
		this.decided = false;
		this.value_is_locked = false;
		this.executed = false;
	}

	public PaxosInstance(int id) {
		this.instance_nb = id;
		this.command_id = 0;
		this.read_ballot = -1;
		this.write_ballot = -1;
		this.accepted_value = 0;
		this.accept_ballot = -1;
		this.learned_value = 0;
		this.decided = false;
		this.value_is_locked = false;
		this.executed = false;
	}

	public PaxosInstance(int id, int ballot) {
		this.instance_nb = id;
		this.command_id = 0;
		this.read_ballot = ballot;
		this.write_ballot = -1;
		this.accepted_value = 0;
		this.accept_ballot = -1;
		this.learned_value = 0;
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

	public synchronized void accept (int ballot, int value) {
		this.accepted_value = value;
		this.write_ballot = ballot;
	}

	public synchronized boolean recordLearn (
			int ballot, int value, int server_id, int quorum) {
		if (this.decided || ballot < this.accept_ballot)
			return false;
		if (ballot > this.accept_ballot) {
			this.accept_ballot = ballot;
			this.learned_value = value;
			this.accepted_by.clear ();
		} else if (value != this.learned_value) {
			return false;
		}
		this.accepted_by.add (server_id);
		if (this.accepted_by.size () < quorum)
			return false;
		this.command_id = this.learned_value;
		this.decided = true;
		return true;
	}

	public synchronized int getAcceptCount () {
		return this.accepted_by.size ();
	}

	public synchronized int getProposedRequestId () {
		return this.proposal.proposed_request_id;
	}

	public synchronized int getUnexecutedRequestId () {
		if (this.executed)
			return -1;
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
		if (this.proposal.claimed || this.decided || this.proposal.proposed_request_id < 0
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
		if (this.proposal.claimed || this.decided || this.proposal.proposed_request_id < 0)
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

	private static final class LocalProposal {
		private int proposed_request_id = -1;
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
