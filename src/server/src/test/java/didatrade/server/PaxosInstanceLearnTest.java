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
		assertEquals (100, entry.getCommandId ());
	}

	@Test
	void higherBallotDoesNotDiscardPreviousBallotVotes () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (4, 100, 0, 3));
		assertFalse (entry.recordLearn (4, 100, 1, 3));

		assertFalse (entry.recordLearn (5, 200, 2, 3));

		assertEquals (2, entry.getAcceptCount (4));
		assertEquals (1, entry.getAcceptCount (5));
		assertEquals (-1, entry.getAcceptBallot ());
		assertFalse (entry.isDecided ());
	}

	@Test
	void duplicateAfterDecisionCannotChangeChosenValue () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertTrue (entry.recordLearn (5, 100, 1, 2));

		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertFalse (entry.recordLearn (6, 200, 2, 2));

		assertEquals (100, entry.getCommandId ());
		assertEquals (5, entry.getAcceptBallot ());
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
		assertEquals (100, entry.getCommandId ());
		assertEquals (4, entry.getAcceptBallot ());
	}

	@Test
	void sameValueReproposalAdvancesAcceptBallot () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertTrue (entry.recordLearn (5, 100, 1, 2));

		assertFalse (entry.recordLearn (7, 100, 0, 2));
		assertTrue (entry.recordLearn (7, 100, 1, 2));

		assertEquals (100, entry.getCommandId ());
		assertEquals (7, entry.getAcceptBallot ());
		assertFalse (entry.recordLearn (7, 100, 0, 2));
	}

	@Test
	void delayedLowerBallotDoesNotRegressAcceptBallot () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (7, 100, 0, 2));
		assertTrue (entry.recordLearn (7, 100, 1, 2));

		assertFalse (entry.recordLearn (4, 100, 2, 2));
		assertFalse (entry.recordLearn (4, 100, 0, 2));

		assertEquals (100, entry.getCommandId ());
		assertEquals (7, entry.getAcceptBallot ());
	}
}
