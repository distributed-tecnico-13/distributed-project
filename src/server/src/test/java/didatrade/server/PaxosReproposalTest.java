package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PaxosReproposalTest {

	@Test
	void decidedSlotRetriesFailedPhaseTwoButNotSuccessfulReproposal () {
		PaxosLog log = new PaxosLog ();
		PaxosInstance entry = log.testAndSetEntry (3);
		assertTrue (entry.decide (4, 101));

		assertTrue (log.tryClaim (entry, 5, 0));
		assertEquals (101, log.valueToPropose (entry));
		assertEquals (101, log.pinPhaseTwoValue (entry, 5, log.valueToPropose (entry)));
		assertFalse (log.tryClaim (entry, 5, Long.MAX_VALUE)); // another worker cannot claim it

		// Phase 2 failed before a quorum: release the worker but preserve the value.
		long retry_at = log.releaseClaim (entry, 5, 1_000_000_000L);
		assertFalse (log.tryClaim (entry, 5, retry_at - 1)); // backoff still active
		assertTrue (log.tryClaim (entry, 5, retry_at)); // same-ballot retry allowed
		assertEquals (101, log.pinPhaseTwoValue (entry, 5, 999)); // value cannot change

		// Successful higher-ballot reproposal must not be repeated in this ballot.
		assertFalse (entry.decide (5, 101)); // slot was already decided
		assertEquals (5, entry.getDecisionBallot ());
		log.releaseClaim (entry, 5, 0);
		assertFalse (log.tryClaim (entry, 5, Long.MAX_VALUE));
		assertTrue (log.tryClaim (entry, 6, Long.MAX_VALUE));
		assertEquals (101, log.pinPhaseTwoValue (entry, 6, log.valueToPropose (entry)));
	}

	@Test
	void decidedSlotAlwaysReproposesItsChosenValueNotAStaleLocalCandidate () {
		PaxosLog log = new PaxosLog ();
		PaxosInstance entry = log.testAndSetEntry (2);
		log.adopt (2, 999);
		assertTrue (entry.decide (4, 101));

		assertEquals (101, log.valueToPropose (entry));
		log.adopt (2, 888);
		assertEquals (101, log.valueToPropose (entry));
		assertEquals (999, log.assignedRequest (2));
	}

	@Test
	void sameBallotPhaseTwoRetryIsIdempotentlyLockedToFirstValue () {
		PaxosLog log = new PaxosLog ();
		PaxosInstance entry = log.testAndSetEntry (0);

		assertEquals (101, log.pinPhaseTwoValue (entry, 5, 101));
		assertEquals (101, log.pinPhaseTwoValue (entry, 5, 202));
		assertEquals (303, log.pinPhaseTwoValue (entry, 6, 303));
	}

	@Test
	void restartingOlderPhaseTwoBallotIsRejectedLocally () {
		PaxosLog log = new PaxosLog ();
		PaxosInstance entry = log.testAndSetEntry (0);
		log.pinPhaseTwoValue (entry, 7, 101);

		boolean threw = false;
		try {
			log.pinPhaseTwoValue (entry, 6, 101);
		} catch (IllegalStateException e) {
			threw = true;
		}
		assertTrue (threw);
	}
}
