package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class DebugStateTest {

	@Test
	void startsWithAllModesOff () {
		DebugState state = new DebugState ();

		assertFalse (state.isBlocking ());
		assertFalse (state.isThrottled ());
		assertFalse (state.shouldExit ());
	}

	@Test
	void crashSetsShouldExit () {
		DebugState state = new DebugState ();

		state.setDebugMode (1);

		assertTrue (state.shouldExit ());
		assertFalse (state.isBlocking ());
		assertFalse (state.isThrottled ());
	}

	@Test
	void freezeAndUnfreezeToggleBlocking () {
		DebugState state = new DebugState ();

		state.setDebugMode (2);
		assertTrue (state.isBlocking ());

		state.setDebugMode (3);
		assertFalse (state.isBlocking ());
	}

	@Test
	void slowModeTogglesThrottled () {
		DebugState state = new DebugState ();

		state.setDebugMode (4);
		assertTrue (state.isThrottled ());

		state.setDebugMode (5);
		assertFalse (state.isThrottled ());
	}

	@Test
	void disableAllClearsFreezeAndSlowMode () {
		DebugState state = new DebugState ();
		state.setDebugMode (2);
		state.setDebugMode (4);

		state.setDebugMode (0);

		assertFalse (state.isBlocking ());
		assertFalse (state.isThrottled ());
	}

	@Test
	void unknownModeLeavesFlagsUnchanged () {
		DebugState state = new DebugState ();
		state.setDebugMode (2);
		state.setDebugMode (4);

		state.setDebugMode (99);

		assertTrue (state.isBlocking ());
		assertTrue (state.isThrottled ());
		assertFalse (state.shouldExit ());
	}

	@Test
	void applyWaitsWhileFrozenAndResumesOnUnfreeze () throws Exception {
		DebugState state = new DebugState ();
		state.setDebugMode (2);
		CountDownLatch finished = new CountDownLatch (1);

		Thread waiter = new Thread ( () -> {
			state.apply ();
			finished.countDown ();
		});
		waiter.start ();

		assertFalse (finished.await (200, TimeUnit.MILLISECONDS));

		state.setDebugMode (3);

		assertTrue (finished.await (1, TimeUnit.SECONDS));
		waiter.join (1000);
	}

	@Test
	void unfreezeReachesARequestBlockedOnTheServer () throws Exception {
		DidaTradeServerState server = new DidaTradeServerState ();
		server.setDebugMode (2);
		CountDownLatch finished = new CountDownLatch (1);

		Thread frozen = new Thread ( () -> {
			server.applyDebugState ();
			finished.countDown ();
		});
		frozen.setDaemon (true);
		frozen.start ();
		long deadline = System.nanoTime () + TimeUnit.SECONDS.toNanos (1);
		while (frozen.getState () != Thread.State.WAITING) {
			assertTrue (System.nanoTime () < deadline);
			Thread.onSpinWait ();
		}

		CountDownLatch unfroze = new CountDownLatch (1);
		Thread unfreeze = new Thread ( () -> {
			server.setDebugMode (3);
			unfroze.countDown ();
		});
		unfreeze.setDaemon (true);
		unfreeze.start ();

		assertTrue (unfroze.await (1, TimeUnit.SECONDS));
		assertTrue (finished.await (1, TimeUnit.SECONDS));
		frozen.join (1000);
	}

	@Test
	void applyResumesWhenDisableAllClearsFreeze () throws Exception {
		DebugState state = new DebugState ();
		state.setDebugMode (2);
		CountDownLatch finished = new CountDownLatch (1);

		Thread waiter = new Thread ( () -> {
			state.apply ();
			finished.countDown ();
		});
		waiter.start ();

		assertFalse (finished.await (200, TimeUnit.MILLISECONDS));

		state.setDebugMode (0);

		assertTrue (finished.await (1, TimeUnit.SECONDS));
		waiter.join (1000);
	}

	@Test
	void applyDoesNotBlockWhenNotFrozen () throws Exception {
		DebugState state = new DebugState ();
		CountDownLatch finished = new CountDownLatch (1);

		Thread waiter = new Thread ( () -> {
			state.apply ();
			finished.countDown ();
		});
		waiter.start ();

		assertTrue (finished.await (1, TimeUnit.SECONDS));
		waiter.join (1000);
	}
}
