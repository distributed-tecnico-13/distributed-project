package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class PaxosPipelineTest {

	@Test
	void logAssignsDistinctPendingRequestsToDistinctSlots () throws Exception {
		PaxosLog log = new PaxosLog ();
		RequestHistory history = new RequestHistory ();
		for (int id = 100; id < 132; id++)
			history.addToPending (id, new RequestRecord (id));

		ExecutorService allocators = Executors.newFixedThreadPool (8);
		CountDownLatch start = new CountDownLatch (1);
		try {
			for (int slot = 0; slot < 32; slot++) {
				int instance = slot;
				allocators.submit ( () -> {
					await (start);
					log.assignPendingRequest (instance, history);
				});
			}
			start.countDown ();
			allocators.shutdown ();
			assertTrue (allocators.awaitTermination (2, TimeUnit.SECONDS));
			Set<Integer> assigned = new HashSet<> ();
			for (int slot = 0; slot < 32; slot++) {
				PaxosInstance entry = log.getEntry (slot);
				assertNotNull (entry);
				assertTrue (assigned.add (entry.getProposedRequestId ()));
			}
			assertEquals (32, assigned.size ());
		} finally {
			allocators.shutdownNow ();
		}
	}

	@Test
	void decidedButUnexecutedRequestIsNotAllocatedAgain () {
		PaxosLog log = new PaxosLog ();
		RequestHistory history = new RequestHistory ();
		history.addToPending (100, new RequestRecord (100));
		history.addToPending (200, new RequestRecord (200));
		log.testAndSetEntry (0).decide (5, 100);

		PaxosInstance next = log.assignPendingRequest (1, history);
		assertNotNull (next);
		assertEquals (200, next.getProposedRequestId ());
	}

	@Test
	void onlyOneWorkerCanClaimASlot () throws Exception {
		PaxosInstance entry = new PaxosInstance (0);
		entry.setProposedRequestId (100);
		ExecutorService workers = Executors.newFixedThreadPool (2);
		CountDownLatch start = new CountDownLatch (1);
		try {
			var first = workers.submit ( () -> {
				await (start);
				return entry.tryClaim (5, 1000);
			});
			var second = workers.submit ( () -> {
				await (start);
				return entry.tryClaim (5, 1000);
			});
			start.countDown ();
			assertTrue (first.get (2, TimeUnit.SECONDS) ^ second.get (2, TimeUnit.SECONDS));
		} finally {
			workers.shutdownNow ();
		}
	}

	@Test
	void adoptOverwritesAStaleUndecidedCandidateWithAFreshlyDiscoveredValue () {
		PaxosLog log = new PaxosLog ();
		assertTrue (log.adopt (5, 301));
		assertEquals (301, log.getEntry (5).getProposedRequestId ());

		assertTrue (log.adopt (5, 301)); // idempotent re-adopt of the same value
		assertTrue (log.adopt (5, 302)); // a newer Phase 1 discovery overrides the stale candidate
		assertEquals (302, log.getEntry (5).getProposedRequestId ());
	}

	@Test
	void adoptRefusesToDoubleBindAStillActiveRequestToAnotherInstance () {
		PaxosLog log = new PaxosLog ();
		assertTrue (log.adopt (5, 301));

		assertFalse (log.adopt (6, 301));
		assertNull (log.getEntry (6));
	}

	@Test
	void adoptCannotOverrideADecidedInstance () {
		PaxosLog log = new PaxosLog ();
		log.testAndSetEntry (5).decide (3, 301);

		assertTrue (log.adopt (5, 301));
		assertFalse (log.adopt (5, 302));
		assertEquals (301, log.getEntry (5).getCommandId ());
	}

	@Test
	void claimedSlotCannotBeTakenEvenAfterAnyRetryDeadline () {
		PaxosInstance entry = new PaxosInstance (0);
		entry.setProposedRequestId (100);
		assertTrue (entry.tryClaim (5, 0));
		assertFalse (entry.tryClaim (5, Long.MAX_VALUE));
	}

	@Test
	void failedAttemptRetriesAfterBackoffWithoutAnotherWakeup () {
		PaxosInstance entry = new PaxosInstance (0);
		entry.setProposedRequestId (100);
		assertTrue (entry.tryClaim (5, 1000));
		long deadline = entry.releaseClaim (5, TimeUnit.MILLISECONDS.toNanos (200));

		assertFalse (entry.tryClaim (5, deadline - 1));
		assertEquals (1, entry.retryDelayMillis (5, deadline - 1));
		assertTrue (entry.tryClaim (5, deadline));
	}

	@Test
	void newerBallotCanClaimImmediatelyAfterFailedAttempt () {
		PaxosInstance entry = new PaxosInstance (0);
		entry.setProposedRequestId (100);
		assertTrue (entry.tryClaim (5, 1000));
		long deadline = entry.releaseClaim (5, TimeUnit.MILLISECONDS.toNanos (200));
		assertTrue (entry.tryClaim (8, deadline - 1));
	}

	@Test
	void fixedPoolRunsDifferentSlotsConcurrently () throws Exception {
		CountDownLatch completions = new CountDownLatch (PaxosPipeline.CAPACITY);
		PaxosPipeline pipeline = new PaxosPipeline (completions::countDown);
		CountDownLatch started = new CountDownLatch (PaxosPipeline.CAPACITY);
		CountDownLatch release = new CountDownLatch (1);
		CountDownLatch done = new CountDownLatch (PaxosPipeline.CAPACITY);
		AtomicInteger active = new AtomicInteger ();
		try {
			for (int slot = 0; slot < PaxosPipeline.CAPACITY; slot++)
				assertTrue (pipeline.submitTask ( () -> {
					active.incrementAndGet ();
					started.countDown ();
					await (release);
					active.decrementAndGet ();
					done.countDown ();
				}));
			assertTrue (started.await (2, TimeUnit.SECONDS));
			assertEquals (PaxosPipeline.CAPACITY, active.get ());
			assertFalse (pipeline.submitTask ( () -> {
			}));
			release.countDown ();
			assertTrue (done.await (2, TimeUnit.SECONDS));
			assertTrue (completions.await (2, TimeUnit.SECONDS));
			assertTrue (pipeline.hasCapacity ());
		} finally {
			release.countDown ();
			pipeline.shutdown ();
		}
	}

	private static void await (CountDownLatch latch) {
		try {
			latch.await ();
		} catch (InterruptedException e) {
			Thread.currentThread ().interrupt ();
		}
	}
}
