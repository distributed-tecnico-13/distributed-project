package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PaxosInstanceLearnTest {

	@Test
	void duplicateLearnFromOneAcceptorDoesNotDecide () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertEquals (1, entry.getAcceptCount (5));
		assertFalse (entry.isDecided ());
	}

	@Test
	void twoDistinctAcceptorsDecide () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertTrue (entry.recordLearn (5, 100, 1, 2));
		assertEquals (2, entry.getAcceptCount (5));
		assertTrue (entry.isDecided ());
		assertEquals (100, entry.getDecidedValue ());
	}

	@Test
	void higherBallotDoesNotDiscardPreviousBallotVotes () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (4, 100, 0, 3));
		assertFalse (entry.recordLearn (4, 100, 1, 3));

		assertFalse (entry.recordLearn (5, 200, 2, 3));

		assertEquals (2, entry.getAcceptCount (4));
		assertEquals (1, entry.getAcceptCount (5));
		assertEquals (-1, entry.getDecisionBallot ());
		assertFalse (entry.isDecided ());
	}

	@Test
	void duplicateAfterDecisionCannotChangeChosenValue () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertTrue (entry.recordLearn (5, 100, 1, 2));

		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertFalse (entry.recordLearn (6, 200, 2, 2));

		assertEquals (100, entry.getDecidedValue ());
		assertEquals (5, entry.getDecisionBallot ());
		assertEquals (2, entry.getAcceptCount (5));
		assertEquals (0, entry.getAcceptCount (6));
	}

	@Test
	void differentValueAtSameBallotDoesNotCount () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertFalse (entry.recordLearn (5, 200, 1, 2));
		assertEquals (1, entry.getAcceptCount (5));
		assertFalse (entry.isDecided ());
	}

	@Test
	void votesFromDifferentBallotsDoNotFormAQuorum () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (4, 100, 0, 2));
		assertFalse (entry.recordLearn (5, 100, 1, 2));
		assertEquals (1, entry.getAcceptCount (4));
		assertEquals (1, entry.getAcceptCount (5));
		assertFalse (entry.isDecided ());
	}

	@Test
	void delayedLowerBallotLearnCanStillCompleteChosenQuorum () {
		PaxosInstance entry = new PaxosInstance (0);

		// First Learn from ballot 4.
		assertFalse (entry.recordLearn (4, 100, 0, 2));

		// A Learn from a newer ballot arrives before the second
		// ballot-4 Learn message.
		assertFalse (entry.recordLearn (5, 100, 2, 2));

		// This delayed Learn completes the ballot-4 quorum.
		assertTrue (entry.recordLearn (4, 100, 1, 2));

		assertEquals (2, entry.getAcceptCount (4));
		assertEquals (1, entry.getAcceptCount (5));
		assertTrue (entry.isDecided ());
		assertEquals (100, entry.getDecidedValue ());
		assertEquals (4, entry.getDecisionBallot ());
	}

	@Test
	void sameValueReproposalAdvancesAcceptBallot () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertTrue (entry.recordLearn (5, 100, 1, 2));

		assertFalse (entry.recordLearn (7, 100, 0, 2));
		assertTrue (entry.recordLearn (7, 100, 1, 2));

		assertEquals (100, entry.getDecidedValue ());
		assertEquals (7, entry.getDecisionBallot ());
		assertFalse (entry.recordLearn (7, 100, 0, 2));
	}

	@Test
	void delayedLowerBallotDoesNotRegressAcceptBallot () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (7, 100, 0, 2));
		assertTrue (entry.recordLearn (7, 100, 1, 2));

		assertFalse (entry.recordLearn (4, 100, 2, 2));
		assertFalse (entry.recordLearn (4, 100, 0, 2));

		assertEquals (100, entry.getDecidedValue ());
		assertEquals (7, entry.getDecisionBallot ());
	}
	@Test
	void fastBallotTracksCompetingValuesIndependently () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertFalse (entry.recordLearn (5, 200, 1, 2));
		assertEquals (1, entry.getAcceptCount (5, 100));
		assertEquals (1, entry.getAcceptCount (5, 200));
		assertFalse (entry.isDecided ());

		assertTrue (entry.recordLearn (5, 100, 2, 2));
		assertEquals (2, entry.getAcceptCount (5, 100));
		assertEquals (100, entry.getDecidedValue ());
	}

	@Test
	void fastBallotCanDetectThatNoValueCanStillReachQuorum () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertFalse (entry.recordLearn (5, 200, 1, 2));
		assertFalse (entry.cannotReachQuorum (5, 2, 3));

		assertFalse (entry.recordLearn (5, 300, 2, 2));
		assertTrue (entry.cannotReachQuorum (5, 2, 3));
		assertFalse (entry.isDecided ());
	}

	@Test
	void fastRoundCollidesOnQuorumOfLearnsOrUnreachableQuorum () {
		PaxosInstance split = new PaxosInstance (0);
		split.recordLearn (5, 100, 0, 4);
		split.recordLearn (5, 100, 1, 4);
		split.recordLearn (5, 100, 2, 4);
		assertFalse (split.fastRoundCollided (5, 4, 5));
		split.recordLearn (5, 200, 3, 4);
		assertTrue (split.fastRoundCollided (5, 4, 5));

		PaxosInstance unreachable = new PaxosInstance (1);
		unreachable.recordLearn (5, 100, 0, 4);
		unreachable.recordLearn (5, 200, 1, 4);
		unreachable.recordLearn (5, 300, 2, 4);
		assertTrue (unreachable.fastRoundCollided (5, 4, 5));

		PaxosInstance chosen = new PaxosInstance (2);
		for (int acceptor = 0; acceptor < 4; acceptor++)
			chosen.recordLearn (5, 100, acceptor, 4);
		assertFalse (chosen.fastRoundCollided (5, 4, 5));
	}

	@Test
	void conflictingDuplicateFromSameAcceptorIsIgnored () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertFalse (entry.recordLearn (5, 200, 0, 2));

		assertEquals (1, entry.getAcceptCount (5, 100));
		assertEquals (0, entry.getAcceptCount (5, 200));
		assertFalse (entry.isDecided ());
	}

	@Test
	void conflictingDuplicateCannotPreventLaterMatchingQuorum () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertFalse (entry.recordLearn (5, 200, 0, 2));
		assertTrue (entry.recordLearn (5, 100, 2, 2));

		assertTrue (entry.isDecided ());
		assertEquals (100, entry.getDecidedValue ());
	}

	@Test
	void twoWaySplitIsStillRecoverableWhileThirdAcceptorIsMissing () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertFalse (entry.recordLearn (5, 200, 1, 2));

		assertFalse (entry.cannotReachQuorum (5, 2, 3));
	}

}
