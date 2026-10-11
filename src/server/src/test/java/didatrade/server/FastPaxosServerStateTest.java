package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

import didatrade.configs.ConfigurationScheduler;

class FastPaxosServerStateTest {

	@Test
	void fastVoteIsRejectedUntilRoundIsOpened () {
		DidaTradeServerState state = state ('B', 0);

		DidaTradeServerState.FastVoteState vote = state.processFastVote (100);

		assertFalse (vote.accepted);
		assertEquals (0, vote.ballot);
		assertEquals (-1, vote.instance);
		assertEquals (0, state.paxos_log.length ());
	}

	@Test
	void openingFastRoundEnablesDirectAcceptance () {
		DidaTradeServerState state = state ('B', 0);
		assertTrue (state.openFastRound (0));

		DidaTradeServerState.FastVoteState vote = state.processFastVote (100);

		assertTrue (vote.accepted);
		assertEquals (0, vote.ballot);
		assertEquals (0, vote.instance);
		assertEquals (100, state.paxos_log.getEntry (0).getAcceptedValue ());
		assertEquals (0, state.paxos_log.getEntry (0).getWriteBallot ());
	}

	@Test
	void duplicateRequestReusesItsLocalFastSlot () {
		DidaTradeServerState state = state ('B', 0);
		assertTrue (state.openFastRound (0));

		DidaTradeServerState.FastVoteState first = state.processFastVote (100);
		DidaTradeServerState.FastVoteState duplicate = state.processFastVote (100);

		assertTrue (first.accepted);
		assertTrue (duplicate.accepted);
		assertEquals (first.instance, duplicate.instance);
		assertEquals (1, state.paxos_log.length ());
	}

	@Test
	void distinctRequestsReceiveDistinctLocalSlots () {
		DidaTradeServerState state = state ('B', 0);
		assertTrue (state.openFastRound (0));

		assertEquals (0, state.processFastVote (100).instance);
		assertEquals (1, state.processFastVote (101).instance);
		assertEquals (2, state.processFastVote (102).instance);
	}

	@Test
	void higherBallotClosesPreviouslyOpenedFastRound () {
		DidaTradeServerState state = state ('B', 0);
		assertTrue (state.openFastRound (0));
		assertTrue (state.isFastRoundOpen (0));

		state.setCurrentBallot (1);

		assertFalse (state.isFastRoundOpen (0));
		assertFalse (state.isFastRoundOpen (1));
		assertFalse (state.processFastVote (101).accepted);
	}

	@Test
	void higherPhaseOnePromiseAlsoClosesFastRound () {
		DidaTradeServerState state = state ('B', 0);
		assertTrue (state.openFastRound (0));

		DidaTradeServerState.PhaseOneState promise = state.processPhaseOne (1);

		assertTrue (promise.accepted);
		assertEquals (1, state.getCurrentBallot ());
		assertFalse (state.getFastPaxosMode ());
	}

	@Test
	void higherClassicPhaseTwoAlsoClosesFastRound () {
		DidaTradeServerState state = state ('B', 0);
		assertTrue (state.openFastRound (0));
		PaxosInstance entry = state.paxos_log.testAndSetEntry (0);

		DidaTradeServerState.PhaseTwoState result = state.processPhaseTwo (entry, 1, 200);

		assertTrue (result.accepted);
		assertEquals (1, state.getCurrentBallot ());
		assertFalse (state.getFastPaxosMode ());
	}

	@Test
	void staleOrFutureOpenRequestIsRejected () {
		DidaTradeServerState state = state ('B', 1);
		state.setCurrentBallot (2);

		assertFalse (state.openFastRound (1));
		assertFalse (state.openFastRound (3));
		assertFalse (state.getFastPaxosMode ());
	}

	@Test
	void classicScheduleABallotCannotBeOpenedAsFast () {
		DidaTradeServerState state = state ('A', 1);
		state.setCurrentBallot (1);

		assertFalse (state.openFastRound (1));
	}

	@Test
	void nonAcceptorCannotOpenFastRound () {
		DidaTradeServerState state = state ('B', 99);

		assertFalse (state.openFastRound (0));
	}

	@Test
	void failedFastBallotAdvancesExactlyOnce () {
		DidaTradeServerState state = state ('B', 0);
		assertTrue (state.openFastRound (0));

		assertTrue (state.advanceFromFailedFastBallot (0));
		assertEquals (1, state.getCurrentBallot ());
		assertFalse (state.getFastPaxosMode ());
		assertFalse (state.advanceFromFailedFastBallot (0));
		assertEquals (1, state.getCurrentBallot ());
	}

	@Test
	void concurrentFastRequestsNeverShareALocalSlot () throws Exception {
		DidaTradeServerState state = state ('B', 0);
		assertTrue (state.openFastRound (0));
		ExecutorService workers = Executors.newFixedThreadPool (8);
		try {
			List<Future<DidaTradeServerState.FastVoteState>> futures = new ArrayList<> ();
			for (int i = 0; i < 24; i++) {
				int request = 1000 + i;
				futures.add (workers.submit ( () -> state.processFastVote (request)));
			}

			Set<Integer> instances = new HashSet<> ();
			for (Future<DidaTradeServerState.FastVoteState> future : futures) {
				DidaTradeServerState.FastVoteState vote = future.get ();
				assertTrue (vote.accepted);
				assertTrue (instances.add (vote.instance));
			}
			assertEquals (futures.size (), state.paxos_log.length ());
		} finally {
			workers.shutdownNow ();
		}
	}

	@Test
	void concurrentDuplicatesAllReuseOneLocalSlot () throws Exception {
		DidaTradeServerState state = state ('B', 0);
		assertTrue (state.openFastRound (0));
		ExecutorService workers = Executors.newFixedThreadPool (8);
		try {
			List<Future<DidaTradeServerState.FastVoteState>> futures = new ArrayList<> ();
			for (int i = 0; i < 20; i++)
				futures.add (workers.submit ( () -> state.processFastVote (777)));

			for (Future<DidaTradeServerState.FastVoteState> future : futures) {
				DidaTradeServerState.FastVoteState vote = future.get ();
				assertTrue (vote.accepted);
				assertEquals (0, vote.instance);
			}
			assertEquals (1, state.paxos_log.length ());
		} finally {
			workers.shutdownNow ();
		}
	}

	private DidaTradeServerState state (char schedule, int server_id) {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.scheduler = new ConfigurationScheduler (schedule);
		state.paxos_log = new PaxosLog ();
		state.req_history = new RequestHistory ();
		state.my_id = server_id;
		return state;
	}
}
