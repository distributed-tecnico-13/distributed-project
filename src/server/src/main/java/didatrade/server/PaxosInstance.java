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
	private final Map<Integer, Map<Integer, LearnVotes>> learn_votes = new HashMap<> ();
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
		if (this.decided && value != this.command_id)
			return false;

		Map<Integer, LearnVotes> values = this.learn_votes.computeIfAbsent (
				ballot, ignored -> new HashMap<> ());

		// One acceptor may cast at most one vote in a ballot. A duplicated Learn for the
		// same value is harmless; a conflicting Learn from the same acceptor is ignored.
		for (LearnVotes candidate : values.values ()) {
			if (candidate.accepted_by.contains (server_id))
				return false;
		}

		LearnVotes votes = values.computeIfAbsent (value, ignored -> new LearnVotes ());
		votes.accepted_by.add (server_id);
		if (votes.accepted_by.size () < quorum)
			return false;

		int highest_ballot = findHighestBallotWithQuorum (quorum);
		if (highest_ballot < 0)
			return false;
		int chosen_value = valueWithQuorum (highest_ballot, quorum);
		if (chosen_value == CommandIdHelper.ABSENT_VALUE)
			return false;
		if (this.decided && chosen_value != this.command_id)
			return false;

		boolean advanced = !this.decided || highest_ballot > this.accept_ballot;
		this.command_id = chosen_value;
		this.accept_ballot = highest_ballot;
		this.decided = true;
		return advanced;
	}

	public synchronized int findHighestBallotWithQuorum (int quorum) {
		int highest_ballot = -1;
		for (int ballot : this.learn_votes.keySet ()) {
			if (this.maxAcceptCount (ballot) >= quorum && ballot > highest_ballot)
				highest_ballot = ballot;
		}
		return highest_ballot;
	}

	public synchronized int getAcceptCount (int ballot) {
		return this.maxAcceptCount (ballot);
	}

	public synchronized int getAcceptCount (int ballot, int value) {
		Map<Integer, LearnVotes> values = this.learn_votes.get (ballot);
		if (values == null)
			return 0;
		LearnVotes votes = values.get (value);
		return votes == null ? 0 : votes.accepted_by.size ();
	}

	public synchronized boolean cannotReachQuorum (
			int ballot, int quorum, int n_acceptors) {
		Map<Integer, LearnVotes> values = this.learn_votes.get (ballot);
		if (values == null || values.isEmpty ())
			return false;

		Set<Integer> responders = new HashSet<> ();
		int highest_count = 0;
		for (LearnVotes votes : values.values ()) {
			responders.addAll (votes.accepted_by);
			highest_count = Math.max (highest_count, votes.accepted_by.size ());
		}

		int remaining = Math.max (0, n_acceptors - responders.size ());
		return highest_count + remaining < quorum;
	}

	private int maxAcceptCount (int ballot) {
		Map<Integer, LearnVotes> values = this.learn_votes.get (ballot);
		if (values == null)
			return 0;
		int max = 0;
		for (LearnVotes votes : values.values ())
			max = Math.max (max, votes.accepted_by.size ());
		return max;
	}

	private int valueWithQuorum (int ballot, int quorum) {
		Map<Integer, LearnVotes> values = this.learn_votes.get (ballot);
		if (values == null)
			return CommandIdHelper.ABSENT_VALUE;
		for (Map.Entry<Integer, LearnVotes> candidate : values.entrySet ()) {
			if (candidate.getValue ().accepted_by.size () >= quorum)
				return candidate.getKey ();
		}
		return CommandIdHelper.ABSENT_VALUE;
	}

	public synchronized int getProposedRequestId () {
		return this.proposal.proposed_request_id;
	}

	public synchronized int getValueToPropose () {
		return this.decided ? this.command_id : this.proposal.proposed_request_id;
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

	// A claim belongs to one local worker; it is unrelated to Paxos acceptance. Nothing can
	// change a decided slot, so it is re-proposed once per ballot and never retried; an
	// undecided one is retried as soon as its backoff expires.
	public synchronized boolean tryClaim (int ballot, long now_nanos) {
		if (this.proposal.claimed || CommandIdHelper.isAbsent (this.getValueToPropose ()))
			return false;
		if (this.decided && ballot <= this.proposal.phase_two_ballot)
			return false;
		if (!this.decided && this.proposal.retry_ballot == ballot
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
		private final Set<Integer> accepted_by = new HashSet<> ();
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
