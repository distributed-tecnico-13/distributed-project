package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import didatrade.configs.ConfigurationScheduler;
import didatrade.core.TradeManager;
import didatrade.server.request.DidaTradeAction;
import didatrade.server.request.DidaTradeCommand;
import didatrade.server.request.RequestHistory;
import didatrade.server.request.RequestRecord;

class MainLoopExecutionTest {

	@Test
	void latePayloadWakeupResumesOrderedExecution () throws Exception {
		DidaTradeServerState state = new DidaTradeServerState ();
		TrackingHistory history = new TrackingHistory ();
		state.req_history = history;
		state.paxos_log = new PaxosLog ();
		state.trade_manager = new TradeManager ();
		state.scheduler = new ConfigurationScheduler ('A');
		state.my_id = 99; // This test exercises only the follower execution path.
		state.main_loop = new MainLoop (state);
		state.paxos_log.testAndSetEntry (0).decide (0, 101);

		Thread loop_thread = new Thread (state.main_loop);
		ExecutorService rpc_waiter = Executors.newSingleThreadExecutor ();
		try {
			loop_thread.start ();
			state.main_loop.wakeup ();
			assertTrue (history.missing_payload.await (2, TimeUnit.SECONDS));
			RequestRecord record = new RequestRecord (101,
					new DidaTradeCommand (DidaTradeAction.ADDUSER, 101, 10, 10));
			var response = rpc_waiter.submit (record::waitForResponse);
			history.addToPending (101, record);
			state.main_loop.wakeup ();

			assertTrue (response.get (2, TimeUnit.SECONDS));
			// setResponse wakes this thread before markExecuted runs.
			assertTrue (executedWithin (state.paxos_log.getEntry (0), 2, TimeUnit.SECONDS));
		} finally {
			loop_thread.interrupt ();
			loop_thread.join (2_000);
			rpc_waiter.shutdownNow ();
			assertFalse (loop_thread.isAlive ());
		}
	}

	private static boolean executedWithin (PaxosInstance entry, long timeout, TimeUnit unit)
			throws InterruptedException {
		long deadline = System.nanoTime () + unit.toNanos (timeout);
		while (!entry.isExecuted ()) {
			if (System.nanoTime () >= deadline)
				return false;
			Thread.sleep (1);
		}
		return true;
	}

	private static final class TrackingHistory extends RequestHistory {
		final CountDownLatch missing_payload = new CountDownLatch (1);

		@Override
		public synchronized RequestRecord getIfExists (int request_id) {
			RequestRecord record = super.getIfExists (request_id);
			if (record == null)
				this.missing_payload.countDown ();
			return record;
		}
	}
}
