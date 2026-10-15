package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;
import io.grpc.stub.StreamObserver;

// Reproduces the review finding: the Phase 1 handler snapshots accepted entries and then
// promises the new ballot. Those two steps must be atomic with respect to a concurrent
// Phase 2 accept, otherwise an acceptor can reply to a higher-ballot Phase 1 without a value
// it legitimately accepted for a lower ballot in between.
class PhaseOneAtomicityTest {

	@Test
	void phaseOneSnapshotAndPromiseAreAtomicWithRespectToPhaseTwo () throws Exception {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.scheduler = new ConfigurationScheduler ('A');
		setDebugState (state, new DebugState ());

		CountDownLatch snapshotTaken = new CountDownLatch (1);
		CountDownLatch releasePromise = new CountDownLatch (1);

		// Delays the snapshot read exactly where the old code left a gap before the promise,
		// giving a concurrent Phase 2 accept a window to race in.
		// Deliberately not synchronized on the PaxosLog instance: it must call through to the
		// real (synchronized, momentary) snapshot and only then pause, otherwise this hook
		// would itself serialize against testAndSetEntry/processPhaseTwo and hide the bug.
		state.paxos_log = new PaxosLog () {
			@Override
			public PaxosInstance[] getAcceptedEntries () {
				PaxosInstance[] entries = super.getAcceptedEntries ();
				snapshotTaken.countDown ();
				try {
					releasePromise.await (2, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					Thread.currentThread ().interrupt ();
				}
				return entries;
			}
		};

		DidaTradePaxosServiceImpl service = new DidaTradePaxosServiceImpl (state);

		List<DidaTradePaxos.PhaseOneReply> phase_one_replies = new ArrayList<> ();
		Thread phase_one_thread = new Thread ( () -> {
			DidaTradePaxos.PhaseOneRequest request = DidaTradePaxos.PhaseOneRequest.newBuilder ()
					.setRequestballot (5)
					.build ();
			service.phaseone (request, capture (phase_one_replies));
		});
		phase_one_thread.start ();

		assertTrue (snapshotTaken.await (2, TimeUnit.SECONDS), "Phase 1 never took its snapshot");

		DidaTradeServerState.PhaseTwoState[] phase_two_result = new DidaTradeServerState.PhaseTwoState[1];
		CountDownLatch phaseTwoDone = new CountDownLatch (1);
		Thread phase_two_thread = new Thread ( () -> {
			PaxosInstance entry = state.paxos_log.testAndSetEntry (0);
			phase_two_result[0] = state.processPhaseTwo (entry, 4, 999);
			phaseTwoDone.countDown ();
		});
		phase_two_thread.start ();

		// While Phase 1 still holds its snapshot open, a racing Phase 2 accept for a lower
		// ballot must not be able to run to completion: it has to wait for Phase 1's
		// snapshot+promise to finish as a single atomic step.
		assertFalse (phaseTwoDone.await (500, TimeUnit.MILLISECONDS),
				"Phase 2 completed while Phase 1's snapshot+promise was still in flight");

		releasePromise.countDown ();

		assertTrue (phaseTwoDone.await (2, TimeUnit.SECONDS), "Phase 2 never completed");
		phase_one_thread.join (2000);

		assertEquals (1, phase_one_replies.size ());
		if (phase_two_result[0].accepted) {
			assertTrue (
					phase_one_replies.get (0).getResponseLogObjectsList ().stream ()
							.anyMatch (obj -> obj.getInstance () == 0 && obj.getValue () == 999),
					"Phase 2's accepted value is missing from the Phase 1 reply it should have been ordered against");
		}
	}

	private static void setDebugState (DidaTradeServerState state, DebugState value) throws Exception {
		Field field = DidaTradeServerState.class.getDeclaredField ("debug_state");
		field.setAccessible (true);
		field.set (state, value);
	}

	private static StreamObserver<DidaTradePaxos.PhaseOneReply> capture (
			List<DidaTradePaxos.PhaseOneReply> sink) {
		return new StreamObserver<DidaTradePaxos.PhaseOneReply> () {
			@Override
			public void onNext (DidaTradePaxos.PhaseOneReply value) {
				sink.add (value);
			}

			@Override
			public void onError (Throwable t) {
				throw new RuntimeException (t);
			}

			@Override
			public void onCompleted () {
			}
		};
	}
}
