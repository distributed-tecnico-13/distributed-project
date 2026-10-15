package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Regression for the newballot RPC waiting on a recovered, already-decided slot. */
class PaxosBallotCompletionRegressionTest {

	@Test
	void successfulHigherBallotReproposalUpdatesCompletionWithoutLearns () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.paxos_log = new PaxosLog ();
		PaxosInstance slot = state.paxos_log.testAndSetEntry (0);
		assertTrue (slot.decide (0, 140));
		state.updateCompletedBallot (0);
		assertEquals (0, state.getCompletedBallot ());

		// This is the path following a successful classic Phase-2 quorum in ballot 1.
		// No asynchronous Learn deliveries are needed to unblock the master RPC.
		new PaxosProposer (state).finishSuccessfulPhaseTwo (slot, 1, 140);

		assertTrue (slot.isDecided ());
		assertEquals (1, slot.getAcceptBallot ());
		assertEquals (1, state.getCompletedBallot ());
		assertFalse (slot.tryClaim (1, System.nanoTime ()));
	}

	@Test
	void freshPhaseTwoDecisionStillUpdatesCompletion () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.paxos_log = new PaxosLog ();
		PaxosInstance slot = state.paxos_log.testAndSetEntry (0);

		new PaxosProposer (state).finishSuccessfulPhaseTwo (slot, 1, 140);

		assertTrue (slot.isDecided ());
		assertEquals (140, slot.getCommandId ());
		assertEquals (1, state.getCompletedBallot ());
	}
}
