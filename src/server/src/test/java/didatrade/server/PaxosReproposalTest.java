package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PaxosReproposalTest {

	@Test
	void decidedSlotRetriesFailedPhaseTwoButNotSuccessfulReproposal () {
		PaxosInstance entry = new PaxosInstance (3);
		assertTrue (entry.decide (4, 101));

		assertTrue (entry.tryClaim (5, 0));
		assertEquals (101, entry.getValueToPropose ());
		assertEquals (101, entry.pinPhaseTwoValue (5, entry.getValueToPropose ()));
		assertFalse (entry.tryClaim (5, Long.MAX_VALUE)); // another worker cannot claim it

		// Phase 2 failed before a quorum: release the worker but preserve the value.
		long retry_at = entry.releaseClaim (5, 1_000_000_000L);
		assertFalse (entry.tryClaim (5, retry_at - 1)); // backoff still active
		assertTrue (entry.tryClaim (5, retry_at)); // same-ballot retry allowed
		assertEquals (101, entry.pinPhaseTwoValue (5, 999)); // value cannot change

		// Successful higher-ballot reproposal must not be repeated in this ballot.
		assertFalse (entry.decide (5, 101)); // slot was already decided
		assertEquals (5, entry.getAcceptBallot ());
		entry.releaseClaim (5, 0);
		assertFalse (entry.tryClaim (5, Long.MAX_VALUE));
		assertTrue (entry.tryClaim (6, Long.MAX_VALUE));
		assertEquals (101, entry.pinPhaseTwoValue (6, entry.getValueToPropose ()));
	}

	@Test
	void decidedSlotAlwaysReproposesItsChosenValueNotAStaleLocalCandidate () {
		PaxosInstance entry = new PaxosInstance (2);
		entry.setProposedRequestId (999);
		assertTrue (entry.decide (4, 101));

		assertEquals (101, entry.getValueToPropose ());
		entry.setProposedRequestId (888);
		assertEquals (101, entry.getValueToPropose ());
		assertEquals (999, entry.getProposedRequestId ());
	}

	@Test
	void sameBallotPhaseTwoRetryIsIdempotentlyLockedToFirstValue () {
		PaxosInstance entry = new PaxosInstance (0);

		assertEquals (101, entry.pinPhaseTwoValue (5, 101));
		assertEquals (101, entry.pinPhaseTwoValue (5, 202));
		assertEquals (303, entry.pinPhaseTwoValue (6, 303));
	}

	@Test
	void restartingOlderPhaseTwoBallotIsRejectedLocally () {
		PaxosInstance entry = new PaxosInstance (0);
		entry.pinPhaseTwoValue (7, 101);

		boolean threw = false;
		try {
			entry.pinPhaseTwoValue (6, 101);
		} catch (IllegalStateException e) {
			threw = true;
		}
		assertTrue (threw);
	}
}
