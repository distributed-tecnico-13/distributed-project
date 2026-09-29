package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import didatrade.configs.ConfigurationScheduler;

class ServerStateBallotInvariantTest {

	@Test
	void currentBallotNeverMovesBackward () {
		DidaTradeServerState state = new DidaTradeServerState ();

		state.setCurrentBallot (7);
		state.setCurrentBallot (3);

		assertEquals (7, state.getCurrentBallot ());
	}

	@Test
	void onlyLeaderOfTheExactCurrentBallotMayPropose () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.scheduler = new ConfigurationScheduler ('A');
		state.my_id = 1;
		state.setCurrentBallot (1);

		assertTrue (state.canProposeFor (1));
		assertFalse (state.canProposeFor (0));
		assertFalse (state.canProposeFor (2));

		state.my_id = 0;
		assertFalse (state.canProposeFor (1));
	}

	@Test
	void lowerPrepareCannotUndoAHigherPromise () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.setCurrentBallot (9);

		DidaTradeServerState.PhaseOneState result = state.processPhaseOne (8);

		assertFalse (result.accepted);
		assertEquals (9, result.maxballot);
		assertEquals (9, state.getCurrentBallot ());
	}

	@Test
	void completedBallotTracksOnlyContiguousDecidedPrefix () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.paxos_log = new PaxosLog ();
		state.paxos_log.testAndSetEntry (0).decide (2, 100);
		state.paxos_log.testAndSetEntry (2).decide (7, 300);

		state.updateCompletedBallot (7);
		assertEquals (2, state.getCompletedBallot ());

		state.paxos_log.testAndSetEntry (1).decide (5, 200);
		state.updateCompletedBallot (7);
		assertEquals (7, state.getCompletedBallot ());
	}
}
