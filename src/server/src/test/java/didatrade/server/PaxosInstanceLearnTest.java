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
		assertEquals (1, entry.getAcceptCount ());
		assertFalse (entry.isDecided ());
	}

	@Test
	void twoDistinctAcceptorsDecide () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertTrue (entry.recordLearn (5, 100, 1, 2));
		assertEquals (2, entry.getAcceptCount ());
		assertTrue (entry.isDecided ());
		assertEquals (100, entry.getCommandId ());
	}

	@Test
	void higherBallotStartsWithAnewAcceptorSet () {
		PaxosInstance entry = new PaxosInstance (0);

		entry.recordLearn (4, 100, 0, 3);
		entry.recordLearn (4, 100, 1, 3);
		assertFalse (entry.recordLearn (5, 200, 2, 3));
		assertEquals (1, entry.getAcceptCount ());
		assertEquals (5, entry.getAcceptBallot ());
		assertFalse (entry.recordLearn (4, 100, 0, 3));
		assertEquals (1, entry.getAcceptCount ());
	}

	@Test
	void duplicateAfterDecisionCannotChangeChosenValue () {
		PaxosInstance entry = new PaxosInstance (0);

		entry.recordLearn (5, 100, 0, 2);
		assertTrue (entry.recordLearn (5, 100, 1, 2));
		assertFalse (entry.recordLearn (5, 100, 0, 2));
		assertFalse (entry.recordLearn (6, 200, 2, 2));
		assertEquals (100, entry.getCommandId ());
		assertEquals (5, entry.getAcceptBallot ());
		assertEquals (2, entry.getAcceptCount ());
	}

	@Test
	void differentValueAtSameBallotDoesNotCount () {
		PaxosInstance entry = new PaxosInstance (0);

		entry.recordLearn (5, 100, 0, 2);
		assertFalse (entry.recordLearn (5, 200, 1, 2));
		assertEquals (1, entry.getAcceptCount ());
		assertFalse (entry.isDecided ());
	}
}
