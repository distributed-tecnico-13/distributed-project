package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import didatrade.DidaTradeMain;
import didatrade.core.TradeManager;
import io.grpc.stub.StreamObserver;

class CausalBalanceTest {

	private static final long STILL_BLOCKED_MILLIS = 200;

	private final ExecutorService rpc_threads = Executors.newCachedThreadPool ();

	@AfterEach
	void stopRpcThreads () {
		this.rpc_threads.shutdownNow ();
	}

	@Test
	void balanceWithoutObservedInstanceDoesNotWait () {
		DidaTradeServerState state = newState ();
		state.trade_manager.add_user (7, 100, 10);

		int balance = assertTimeoutPreemptively (Duration.ofSeconds (2),
				() -> state.getBalance (-1, 7).balance ());
		assertEquals (100, balance);
	}

	@Test
	void balanceWaitsUntilObservedInstanceIsExecuted () throws Exception {
		DidaTradeServerState state = newState ();
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (state);
		addUserRequest (state, 101, 7, 100);
		decide (state, 0, 101);

		Future<Integer> balance = this.rpc_threads.submit ( () -> state.getBalance (0, 7).balance ());
		assertStillBlocked (balance);

		assertEquals (1, executor.drain ());
		assertEquals (100, balance.get (2, TimeUnit.SECONDS));
	}

	@Test
	void balanceWaitsForGapBeforeObservedInstance () throws Exception {
		DidaTradeServerState state = newState ();
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (state);
		addUserRequest (state, 101, 7, 100);
		addUserRequest (state, 201, 8, 200);
		decide (state, 1, 201);

		Future<Integer> balance = this.rpc_threads.submit ( () -> state.getBalance (1, 8).balance ());
		assertEquals (0, executor.drain ());
		assertStillBlocked (balance);

		decide (state, 0, 101);
		assertEquals (2, executor.drain ());
		assertEquals (200, balance.get (2, TimeUnit.SECONDS));
	}

	@Test
	void balanceCannotObserveEffectBeforeInstanceIsPublished () throws Exception {
		DidaTradeServerState state = newState ();
		TradeManagerPausingAfterAcquire trades = new TradeManagerPausingAfterAcquire ();
		state.trade_manager = trades;
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (state);
		addUserRequest (state, 101, 7, 100);
		decide (state, 0, 101);
		assertEquals (1, executor.drain ());

		addBuyRequest (state, 201, 7, 50);
		decide (state, 1, 201);
		Future<Integer> drained = this.rpc_threads.submit (executor::drain);
		assertTrue (trades.awaitApplied ());

		Future<DidaTradeServerState.BalanceResult> balance = this.rpc_threads.submit (
				() -> state.getBalance (-1, 7));
		try {
			Thread.sleep (STILL_BLOCKED_MILLIS);
			if (balance.isDone ()) {
				DidaTradeServerState.BalanceResult early = balance.get ();
				fail ("balance " + early.balance () + " was read at instance "
						+ early.lastExecutedInstance ()
						+ " while slot 1 was applied but not yet published");
			}
			trades.release ();
			DidaTradeServerState.BalanceResult snapshot = balance.get (2, TimeUnit.SECONDS);
			assertEquals (50, snapshot.balance ());
			assertEquals (1, snapshot.lastExecutedInstance ());
			assertEquals (1, drained.get (2, TimeUnit.SECONDS));
		} finally {
			trades.release ();
		}
	}

	@Test
	void executorTagsCommandWithItsInstanceEvenWhenItFails () {
		DidaTradeServerState state = newState ();
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (state);
		RequestRecord first = addUserRequest (state, 101, 7, 100);
		RequestRecord duplicate = addUserRequest (state, 201, 7, 300);
		decide (state, 0, 101);
		decide (state, 1, 201);

		assertEquals (2, executor.drain ());
		assertTrue (first.getResponseValue ());
		assertFalse (duplicate.getResponseValue ());
		assertEquals (0, first.getRequest ().getInstance ());
		assertEquals (1, duplicate.getRequest ().getInstance ());
	}

	@Test
	void lastExecutedInstanceStopsBeforeGap () {
		DidaTradeServerState state = newState ();
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (state);
		assertEquals (-1, state.getLastExecutedInstance ());

		addUserRequest (state, 101, 7, 100);
		addUserRequest (state, 201, 8, 200);
		addUserRequest (state, 401, 9, 300);
		decide (state, 0, 101);
		decide (state, 1, 201);
		decide (state, 3, 401);

		assertEquals (2, executor.drain ());
		assertEquals (1, state.getLastExecutedInstance ());
	}

	@Test
	void balanceReplyCarriesLastExecutedInstance () throws Exception {
		DidaTradeServerState state = newState ();
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (state);
		DidaTradeMainServiceImpl service = new DidaTradeMainServiceImpl (state);
		addUserRequest (state, 101, 7, 100);
		addUserRequest (state, 201, 8, 200);
		decide (state, 0, 101);
		decide (state, 1, 201);
		assertEquals (2, executor.drain ());

		DidaTradeMain.BalanceReply reply = callBalance (service, 7, 0);
		assertTrue (reply.getResult ());
		assertEquals (100, reply.getBalance ());
		assertEquals (1, reply.getInstance ());
	}

	@Test
	void balanceReplyForUnknownUserStillCarriesInstance () throws Exception {
		DidaTradeServerState state = newState ();
		DidaTradeMainServiceImpl service = new DidaTradeMainServiceImpl (state);

		DidaTradeMain.BalanceReply reply = callBalance (service, 7, -1);
		assertFalse (reply.getResult ());
		assertEquals (-1, reply.getInstance ());
	}

	private DidaTradeMain.BalanceReply callBalance (
			DidaTradeMainServiceImpl service, int user_id, int min_instance) throws Exception {
		DidaTradeMain.BalanceRequest request = DidaTradeMain.BalanceRequest.newBuilder ()
				.setReqid (999)
				.setUserid (user_id)
				.setMininstance (min_instance)
				.build ();
		CapturingObserver<DidaTradeMain.BalanceReply> observer = new CapturingObserver<> ();
		this.rpc_threads.submit ( () -> service.balance (request, observer)).get (2, TimeUnit.SECONDS);
		assertTrue (observer.completed);
		assertNotNull (observer.value);
		return observer.value;
	}

	private static void assertStillBlocked (Future<?> future) throws InterruptedException {
		Thread.sleep (STILL_BLOCKED_MILLIS);
		assertFalse (future.isDone ());
	}

	private static DidaTradeServerState newState () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.paxos_log = new PaxosLog ();
		state.req_history = new RequestHistory ();
		state.trade_manager = new TradeManager ();
		return state;
	}

	private static RequestRecord addUserRequest (
			DidaTradeServerState state, int request_id, int user_id, int cash) {
		RequestRecord record = new RequestRecord (request_id,
				new DidaTradeCommand (DidaTradeAction.ADDUSER, user_id, cash, 10));
		state.req_history.addToPending (request_id, record);
		return record;
	}

	private static RequestRecord addBuyRequest (
			DidaTradeServerState state, int request_id, int user_id, int fraction) {
		RequestRecord record = new RequestRecord (request_id,
				new DidaTradeCommand (DidaTradeAction.BUY, user_id, fraction));
		state.req_history.addToPending (request_id, record);
		return record;
	}

	// Pauses after the wallet changes and before acquire returns, which is the gap before
	// the executor publishes last_executed_instance.
	private static final class TradeManagerPausingAfterAcquire extends TradeManager {
		private final CountDownLatch applied = new CountDownLatch (1);
		private final CountDownLatch continue_execution = new CountDownLatch (1);

		@Override
		public boolean acquire (int user_id, int fraction) {
			boolean result = super.acquire (user_id, fraction);
			this.applied.countDown ();
			try {
				if (!this.continue_execution.await (5, TimeUnit.SECONDS))
					throw new IllegalStateException ("execution was not released");
			} catch (InterruptedException e) {
				Thread.currentThread ().interrupt ();
				throw new IllegalStateException ("interrupted while holding an applied buy", e);
			}
			return result;
		}

		boolean awaitApplied () throws InterruptedException {
			return this.applied.await (2, TimeUnit.SECONDS);
		}

		void release () {
			this.continue_execution.countDown ();
		}
	}

	private static void decide (DidaTradeServerState state, int instance, int request_id) {
		state.paxos_log.testAndSetEntry (instance).decide (0, request_id);
	}

	private static final class CapturingObserver<T> implements StreamObserver<T> {
		volatile T value;
		volatile boolean completed;

		@Override
		public void onNext (T reply) {
			this.value = reply;
		}

		@Override
		public void onError (Throwable t) {
			throw new AssertionError (t);
		}

		@Override
		public void onCompleted () {
			this.completed = true;
		}
	}
}
