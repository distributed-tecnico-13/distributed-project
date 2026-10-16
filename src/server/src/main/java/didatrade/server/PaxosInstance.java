package didatrade.server;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import didatrade.server.request.CommandIdKind;

/**
 * One slot of the Paxos log, kept in separate parts that are all guarded by this object's monitor:
 * <ul>
 * <li>the decision (top level): the value chosen for the slot and the ballot it was confirmed in,
 * <li>{@code accepted}: what this server accepted as an acceptor; answers Phase 1,
 * <li>{@code votes}: what the acceptors reported to this server as a learner, until a quorum agrees,
 * <li>{@code proposal}: this server's own attempt to propose the slot as a leader.
 * </ul>
 */
public class PaxosInstance {
	private final int instance_nb;

	// The decision. Only a value chosen by a quorum ends up here, whichever way we learned it.
	private boolean decided;
	private int decided_value;
	private int decision_ballot = -1;
	private boolean executed;

	private final Accepted accepted = new Accepted ();
	private final LearnInfo votes = new LearnInfo ();
	// This server's attempt to propose the slot.
	private final LocalProposal proposal = new LocalProposal ();

	public PaxosInstance() {
		this (0);
	}

	public PaxosInstance(int id) {
		this.instance_nb = id;
	}

	public synchronized int getInstanceNb () {
		return this.instance_nb;
	}

	// ---- Acceptor: accepted value and ballot ----

	public synchronized boolean accept (int ballot, int value) {
		// Defensive rejections, should never actually happen. TODO: Maybe remove them.
		if (this.decided && value != this.decided_value)
			return false;
		return this.accepted.accept (ballot, value);
	}

	public synchronized boolean isAccepted () {
		return this.accepted.exists ();
	}

	// Value and ballot read together, so Phase 1 never pairs a value with the wrong ballot.
	public synchronized AcceptedSnapshot getAcceptedSnapshot () {
		return new AcceptedSnapshot (this.accepted.value, this.accepted.ballot);
	}

	// ---- Learner: votes from the acceptors ----

	// Learner: counts one acceptor's vote. True if this vote newly decided the slot or raised its
	// decision ballot. The vote that completes a quorum for a value decides the slot, so a later
	// quorum can only confirm the same value in a higher ballot (or conflict and be rejected).
	public synchronized boolean recordLearn (
			int ballot, int value, int server_id, int quorum) {
		// E.g. a late minority vote in a Fast ballot, after the slot was decided on another value.
		if (this.decided && value != this.decided_value)
			return false;
		if (!this.votes.recordVote (ballot, value, server_id))
			return false;
		if (this.votes.acceptCount (ballot, value) < quorum)
			return false;
		return this.confirm (ballot, value);
	}

	public synchronized int getAcceptCount (int ballot) {
		return this.votes.acceptCount (ballot);
	}

	// Not proof that nothing was chosen: on a 3-1 split the missing acceptor may still
	// complete the quorum. Message from prefessor:
	// "A node automatically changes the ballot after receiving a fast quorum of “learns”
	// where no value has a supermajority." Therefore we either advance here or when we are already
	// sure a majority cannot be reached. Blocked states are possible, here the console must issue
	// a new ballot.
	public synchronized boolean fastRoundCollided (int ballot, int quorum, int n_acceptors) {
		return this.votes.acceptCount (ballot) < quorum
				&& (this.votes.responderCount (ballot) >= quorum
						|| this.votes.cannotReachQuorum (ballot, quorum, n_acceptors));
	}

	// ---- Decision ----

	// Records the decision, for example after a successful classic Phase 2 or a state transfer.
	// The first decision wins; true only if this call decided the slot.
	public synchronized boolean decide (int ballot, int value) {
		boolean first = !this.decided;
		this.confirm (ballot, value);
		return first;
	}

	// The single place that sets the decision: used for a quorum of votes, a successful Phase 2
	// and a state transfer. A decided value never changes. A successful classic reproposal of an
	// already-chosen value confirms the higher ballot even if its asynchronous Learns never
	// arrive. True if the decision is new or its ballot rose.
	private boolean confirm (int ballot, int value) {
		if (this.decided && value != this.decided_value)
			return false;
		boolean advanced = !this.decided || ballot > this.decision_ballot;
		this.decided_value = value;
		this.decision_ballot = Math.max (this.decision_ballot, ballot);
		this.decided = true;
		return advanced;
	}

	public synchronized boolean isDecided () {
		return this.decided;
	}

	public synchronized int getDecidedValue () {
		return this.decided_value;
	}

	public synchronized int getDecisionBallot () {
		return this.decision_ballot;
	}

	public synchronized boolean isNoop () {
		return CommandIdKind.isNoop (this.decided_value);
	}

	// ---- Proposer: assigned request, claim and retry backoff ----

	public synchronized int getProposedRequestId () {
		return this.proposal.proposed_request_id;
	}

	// A decided value always wins. Otherwise the request assigned to this slot: by the leader
	// (from a client, or a value recovered in Phase 1), by a Fast vote,
	// or the value of an accepted Phase 2.
	public synchronized int getValueToPropose () {
		return this.decided ? this.decided_value : this.proposal.proposed_request_id;
	}

	// The request this slot currently owns (decided, else accepted, else proposed), or ABSENT once
	// executed. Prevents one request from being assigned to two slots.
	public synchronized int getUnexecutedRequestId () {
		if (this.executed)
			return CommandIdKind.ABSENT_ID;
		if (this.decided)
			return this.decided_value;
		return this.accepted.exists () ? this.accepted.value : this.proposal.proposed_request_id;
	}

	public synchronized void setProposedRequestId (int request_id) {
		if (!this.decided)
			this.proposal.proposed_request_id = request_id;
	}

	// Only an actual successful Phase 2 changes acceptor-owned proposal bookkeeping.
	public synchronized void noteClassicAcceptance (int value) {
		if (!this.decided)
			this.proposal.proposed_request_id = value;
	}

	// Pins the value Phase 2 sends for this slot in this ballot
	public synchronized int pinPhaseTwoValue (int ballot, int selected_value) {
		return this.proposal.pinPhaseTwoValue (ballot, selected_value);
	}

	// A claim belongs to one local worker; it is unrelated to Paxos acceptance.
	public synchronized boolean tryClaim (int ballot, long now_nanos) {
		// Claimable exactly when there is nothing left to wait for (see retryDelayMillis).
		if (this.retryDelayMillis (ballot, now_nanos) != 0)
			return false;
		this.proposal.claimed = true;
		return true;
	}

	// Ends the claim and delays the next attempt in this ballot by the backoff. E.g. after a
	// Phase 2 that found no quorum
	public synchronized long releaseClaim (int ballot, long retry_delay_nanos) {
		this.proposal.claimed = false;
		this.proposal.retry_ballot = ballot;
		this.proposal.retry_after_nanos = System.nanoTime () + retry_delay_nanos;
		return this.proposal.retry_after_nanos;
	}

	// Milliseconds until this slot may be claimed in this ballot: 0 means now, a positive result is a
	// bounded wait, -1 means never (held by a worker, already decided at this ballot, or no value).
	public synchronized long retryDelayMillis (int ballot, long now_nanos) {
		if (this.proposal.claimed || (this.decided && this.decision_ballot >= ballot)
				|| CommandIdKind.isAbsent (this.getValueToPropose ()))
			return -1;
		if (this.proposal.retry_ballot != ballot
				|| now_nanos >= this.proposal.retry_after_nanos)
			return 0;
		long remaining = this.proposal.retry_after_nanos - now_nanos;
		return Math.max (1, (remaining + 999_999) / 1_000_000);
	}

	// ---- Execution ----

	public synchronized boolean isExecuted () {
		return this.executed;
	}

	public synchronized void markExecuted () {
		this.executed = true;
	}

	// ---- Nested types ----

	// What this server accepted as an acceptor: the highest ballot it accepted a value in, and
	// that value. Independent of the decision: Phase 1 must report exactly this.
	private static final class Accepted {
		private int ballot = -1;
		private int value;

		boolean exists () {
			return this.ballot != -1;
		}

		boolean accept (int ballot, int value) {
			// A ballot never sends two different values for one slot.
			if (ballot == this.ballot && value != this.value)
				return false;
			this.ballot = ballot;
			this.value = value;
			return true;
		}
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

		// True if the quorum stays out of reach even when every acceptor that has not reported yet
		// votes for the leading value.
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
		private int pinPhaseTwoValue (int ballot, int selected_value) {
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
