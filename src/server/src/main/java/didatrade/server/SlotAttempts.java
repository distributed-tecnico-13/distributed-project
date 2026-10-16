package didatrade.server;

import java.util.HashMap;
import java.util.Map;
import didatrade.server.request.CommandIdKind;

/**
 * Leader-side work scheduling per slot: which attempt is running (the claim), when a failed slot
 * may be retried (the backoff), and the Phase 2 value pinned for each ballot. This is not Paxos
 * state, it only decides when this server tries a slot. Guarded by its own monitor.
 */
final class SlotAttempts {
	// Slot number -> this server's attempt state for that slot (claim, backoff, pinned value).
	// A slot has an entry once it was first tried.
	private final Map<Integer, Attempt> attempts = new HashMap<> ();

	// A claim belongs to one local worker; it is unrelated to Paxos acceptance.
	synchronized boolean tryClaim (PaxosInstance entry, int value_to_propose, int ballot, long now_nanos) {
		// Claimable exactly when there is nothing left to wait for (see retryDelayMillis).
		if (this.retryDelayMillis (entry, value_to_propose, ballot, now_nanos) != 0)
			return false;
		this.attempts.computeIfAbsent (entry.getInstanceNb (), slot -> new Attempt ()).claimed = true;
		return true;
	}

	// Ends the claim and delays the next attempt in this ballot by the backoff. E.g. after a
	// Phase 2 that found no quorum
	synchronized long releaseClaim (int slot, int ballot, long retry_delay_nanos) {
		Attempt attempt = this.attempts.computeIfAbsent (slot, s -> new Attempt ());
		attempt.claimed = false;
		attempt.retry_ballot = ballot;
		attempt.retry_after_nanos = System.nanoTime () + retry_delay_nanos;
		return attempt.retry_after_nanos;
	}

	// Milliseconds until this slot may be claimed in this ballot: 0 means now, a positive result is a
	// bounded wait, -1 means never (held by a worker, already decided at this ballot, or no value).
	synchronized long retryDelayMillis (PaxosInstance entry, int value_to_propose, int ballot, long now_nanos) {
		Attempt attempt = this.attempts.getOrDefault (entry.getInstanceNb (), Attempt.NONE);
		if (attempt.claimed || (entry.isDecided () && entry.getDecisionBallot () >= ballot)
				|| CommandIdKind.isAbsent (value_to_propose))
			return -1;
		if (attempt.retry_ballot != ballot || now_nanos >= attempt.retry_after_nanos)
			return 0;
		long remaining = attempt.retry_after_nanos - now_nanos;
		return Math.max (1, (remaining + 999_999) / 1_000_000);
	}

	// A ballot must never send two different values for a slot, including across retries.
	synchronized int pinPhaseTwoValue (int slot, int ballot, int selected_value) {
		Attempt attempt = this.attempts.computeIfAbsent (slot, s -> new Attempt ());
		if (ballot < attempt.phase_two_ballot)
			throw new IllegalStateException ("Cannot restart an older Phase 2 ballot");
		if (ballot > attempt.phase_two_ballot) {
			attempt.phase_two_ballot = ballot;
			attempt.phase_two_value = selected_value;
		}
		return attempt.phase_two_value;
	}

	private static final class Attempt {
		// What a slot that was never tried looks like. Only read, never changed.
		static final Attempt NONE = new Attempt ();

		private boolean claimed;
		private int retry_ballot = -1;
		private long retry_after_nanos;
		private int phase_two_ballot = -1;
		private int phase_two_value;
	}
}
