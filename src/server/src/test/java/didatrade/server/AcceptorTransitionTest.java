package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import didatrade.configs.ConfigurationScheduler;

class AcceptorTransitionTest {

	@Test
	void lowerBallotCannotBeAcceptedAfterHigherPromise () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.scheduler = new ConfigurationScheduler ('A');
		PaxosInstance entry = new PaxosInstance (0);

		assertTrue (state.processPhaseOne (5).accepted);
		DidaTradeServerState.PhaseTwoState result = state.processPhaseTwo (entry, 4, 100);

		assertFalse (result.accepted);
		assertEquals (5, result.maxballot);
		assertEquals (-1, entry.getAcceptedSnapshot ().valballot);
	}

	@Test
	void sameBallotCannotAcceptTwoDifferentValuesBeforeDecision () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.scheduler = new ConfigurationScheduler ('A');
		PaxosInstance entry = new PaxosInstance (0);

		assertTrue (state.processPhaseTwo (entry, 5, 100).accepted);
		assertTrue (state.processPhaseTwo (entry, 5, 100).accepted);
		DidaTradeServerState.PhaseTwoState conflicting = state.processPhaseTwo (entry, 5, 200);

		assertFalse (conflicting.accepted);
		assertEquals (5, conflicting.maxballot);
		PaxosInstance.AcceptedSnapshot snapshot = entry.getAcceptedSnapshot ();
		assertEquals (100, snapshot.value);
		assertEquals (5, snapshot.valballot);
		assertFalse (entry.isDecided ());
	}

	@Test
	void decidedSlotAcceptsSameValueAtNewerBallot () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.scheduler = new ConfigurationScheduler ('A');
		PaxosInstance entry = new PaxosInstance (0);
		assertTrue (entry.decide (4, 100));

		DidaTradeServerState.PhaseTwoState result = state.processPhaseTwo (entry, 5, 100);

		assertTrue (result.accepted);
		assertEquals (5, result.maxballot);
		assertEquals (100, entry.getDecidedValue ());
		assertEquals (4, entry.getDecisionBallot ());
		PaxosInstance.AcceptedSnapshot snapshot = entry.getAcceptedSnapshot ();
		assertEquals (100, snapshot.value);
		assertEquals (5, snapshot.valballot);
	}

	@Test
	void decidedSlotRejectsConflictingValueButObservesHigherBallot () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.scheduler = new ConfigurationScheduler ('A');
		PaxosInstance entry = new PaxosInstance (0);
		assertTrue (state.processPhaseTwo (entry, 4, 100).accepted);
		assertTrue (entry.decide (4, 100));
		assertTrue (state.processPhaseOne (5).accepted);

		DidaTradeServerState.PhaseTwoState result = state.processPhaseTwo (entry, 8, 200);

		assertFalse (result.accepted);
		assertEquals (8, result.maxballot);
		assertEquals (8, state.getCurrentBallot ());
		assertEquals (100, entry.getDecidedValue ());
		assertEquals (4, entry.getDecisionBallot ());
		PaxosInstance.AcceptedSnapshot snapshot = entry.getAcceptedSnapshot ();
		assertEquals (100, snapshot.value);
		assertEquals (4, snapshot.valballot);
		assertFalse (state.processPhaseTwo (entry, 6, 100).accepted);
	}

	@Test
	void phaseOneSnapshotAlwaysPairsValueWithItsAcceptedBallot () throws Exception {
		ExecutorService workers = Executors.newFixedThreadPool (2);
		try {
			for (int i = 0; i < 50; i++) {
				DidaTradeServerState state = new DidaTradeServerState ();
				state.scheduler = new ConfigurationScheduler ('A');
				PaxosInstance entry = new PaxosInstance (i);
				state.processPhaseTwo (entry, 4, 100);
				CountDownLatch start = new CountDownLatch (1);

				Future<PaxosInstance.AcceptedSnapshot> snapshot = workers.submit ( () -> {
					start.await ();
					return entry.getAcceptedSnapshot ();
				});
				Future<DidaTradeServerState.PhaseTwoState> acceptance = workers.submit ( () -> {
					start.await ();
					return state.processPhaseTwo (entry, 5, 200);
				});
				start.countDown ();
				PaxosInstance.AcceptedSnapshot result = snapshot.get (2, TimeUnit.SECONDS);
				assertTrue (acceptance.get (2, TimeUnit.SECONDS).accepted);
				assertTrue ((result.value == 100 && result.valballot == 4)
						|| (result.value == 200 && result.valballot == 5));
			}
		} finally {
			workers.shutdownNow ();
		}
	}
}
