package didatrade.server;

import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class CausalBalanceSnapshotTest {
	@Test
	@Timeout(12)
	void readCannotExposeTradeBeforePublishingItsCausalPrefix () throws Exception {
		DidaTradeServerState a = StateTransferTest.state (5);
		DidaTradeServerState b = StateTransferTest.state (6);
		OrderedPaxosExecutor ea = new OrderedPaxosExecutor (a);
		OrderedPaxosExecutor eb = new OrderedPaxosExecutor (b);
		addUser (a);
		addUser (b);
		ea.drain ();
		eb.drain ();
		CountDownLatch applied = new CountDownLatch (1);
		CountDownLatch release = new CountDownLatch (1);
		a.req_history.addToPending (201, new RequestRecord (201,
				new DidaTradeCommand (DidaTradeAction.BUY, 77, 50)) {
			@Override
			public synchronized void setResponse (boolean result) {
				applied.countDown ();
				try {
					if (!release.await (5, TimeUnit.SECONDS))
						throw new AssertionError ("test release timed out");
				} catch (InterruptedException e) {
					Thread.currentThread ().interrupt ();
					throw new AssertionError (e);
				}
				super.setResponse (result);
			}
		});
		a.paxos_log.testAndSetEntry (1).decide (0, 201);
		ExecutorService workers = Executors.newFixedThreadPool (2);
		try {
			Future<Integer> execution = workers.submit (ea::drain);
			assertTrue (applied.await (2, TimeUnit.SECONDS));
			Future<DidaTradeServerState.BalanceResult> first = workers.submit ( () -> a.getBalance (0, 77));
			assertThrows (TimeoutException.class, () -> first.get (100, TimeUnit.MILLISECONDS));
			release.countDown ();
			assertEquals (1, execution.get (2, TimeUnit.SECONDS));
			DidaTradeServerState.BalanceResult snapshot = first.get (2, TimeUnit.SECONDS);
			assertEquals (50, snapshot.balance ());
			assertEquals (1, snapshot.lastExecutedInstance ());
			Future<DidaTradeServerState.BalanceResult> next = workers.submit ( () -> b.getBalance (snapshot.lastExecutedInstance (), 77));
			assertThrows (TimeoutException.class, () -> next.get (100, TimeUnit.MILLISECONDS));
			StateTransferTest.receive (b, 201, new DidaTradeCommand (DidaTradeAction.BUY, 77, 50));
			b.paxos_log.testAndSetEntry (1).decide (0, 201);
			eb.drain ();
			assertEquals (50, next.get (2, TimeUnit.SECONDS).balance ());
		} finally {
			release.countDown ();
			workers.shutdownNow ();
		}
	}

	private static void addUser (DidaTradeServerState s) {
		StateTransferTest.receive (s, 101, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10));
		s.paxos_log.testAndSetEntry (0).decide (0, 101);
	}
}
