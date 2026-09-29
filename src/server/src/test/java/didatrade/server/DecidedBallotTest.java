package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class DecidedBallotTest {

	@Test
	void reportsTheHighestDecidedBallotPastAHole () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.paxos_log = new PaxosLog ();
		state.paxos_log.testAndSetEntry (0).decide (1, 101);
		state.paxos_log.testAndSetEntry (1);
		state.paxos_log.testAndSetEntry (5).decide (4, 201);

		assertEquals (4, state.findMaxDecidedBallot ());
	}

	@Test
	void usesTheHighestBallotInTheDecidedPrefix () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.paxos_log = new PaxosLog ();
		state.paxos_log.testAndSetEntry (0).decide (1, 101);
		state.paxos_log.testAndSetEntry (1).decide (3, 201);

		state.updateCompletedBallot (0);

		assertEquals (3, state.getCompletedBallot ());
	}
}
