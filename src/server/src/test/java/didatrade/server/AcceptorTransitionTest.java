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

class AcceptorTransitionTest {

	@Test
	void lowerBallotCannotBeAcceptedAfterHigherPromise () {
		DidaTradeServerState state = new DidaTradeServerState ();
		PaxosInstance entry = new PaxosInstance (0);

		assertTrue (state.processPhaseOne (entry, 5).accepted);
		DidaTradeServerState.PhaseTwoState result = state.processPhaseTwo (entry, 4, 100);

		assertFalse (result.accepted);
		assertEquals (5, result.maxballot);
		assertEquals (-1, state.processPhaseOne (entry, 5).valballot);
	}

	@Test
	void phaseOneSnapshotAlwaysPairsValueWithItsAcceptedBallot () throws Exception {
		ExecutorService workers = Executors.newFixedThreadPool (2);
		try {
			for (int i = 0; i < 50; i++) {
				DidaTradeServerState state = new DidaTradeServerState ();
				PaxosInstance entry = new PaxosInstance (i);
				state.processPhaseTwo (entry, 4, 100);
				CountDownLatch start = new CountDownLatch (1);

				Future<PaxosInstance.PhaseOneState> snapshot = workers.submit ( () -> {
					start.await ();
					return state.processPhaseOne (entry, 5);
				});
				Future<DidaTradeServerState.PhaseTwoState> acceptance = workers.submit ( () -> {
					start.await ();
					return state.processPhaseTwo (entry, 5, 200);
				});
				start.countDown ();
				PaxosInstance.PhaseOneState result = snapshot.get (2, TimeUnit.SECONDS);
				assertTrue (acceptance.get (2, TimeUnit.SECONDS).accepted);
				assertTrue ((result.value == 100 && result.valballot == 4)
						|| (result.value == 200 && result.valballot == 5));
			}
		} finally {
			workers.shutdownNow ();
		}
	}
}
