package didatrade.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class GenericResponseCollectorConcurrencyTest {

	@Test
	void concurrentCallbacksDoNotInvokeProcessorAfterItDeclaresCompletion () throws Exception {
		AtomicInteger processor_calls = new AtomicInteger ();
		GenericResponseProcessor<Integer> processor = new GenericResponseProcessor<> () {
			@Override
			public boolean onNext (ArrayList<Integer> all_responses, Integer last_response) {
				processor_calls.incrementAndGet ();
				return true;
			}
		};
		GenericResponseCollector<Integer> collector = new GenericResponseCollector<> (
				new ArrayList<> (), 32, processor);
		ExecutorService workers = Executors.newFixedThreadPool (8);
		CountDownLatch start = new CountDownLatch (1);
		CountDownLatch finished = new CountDownLatch (32);
		try {
			for (int i = 0; i < 32; i++) {
				int value = i;
				workers.submit ( () -> {
					await (start);
					collector.addResponse (value);
					finished.countDown ();
				});
			}
			start.countDown ();
			collector.waitUntilDone ();
			assertTrue (finished.await (2, TimeUnit.SECONDS));
			assertEquals (1, processor_calls.get ());
		} finally {
			workers.shutdownNow ();
		}
	}

	@Test
	void mixOfResponsesAndFailuresAlwaysTerminatesWhenAllCallbacksArrive () throws Exception {
		GenericResponseCollector<Integer> collector = new GenericResponseCollector<> (
				new ArrayList<> (), 20);
		ExecutorService workers = Executors.newFixedThreadPool (8);
		CountDownLatch start = new CountDownLatch (1);
		try {
			for (int i = 0; i < 20; i++) {
				int callback = i;
				workers.submit ( () -> {
					await (start);
					if ((callback & 1) == 0)
						collector.addResponse (callback);
					else
						collector.addNoResponse ();
				});
			}
			start.countDown ();
			CountDownLatch returned = new CountDownLatch (1);
			Thread waiter = new Thread ( () -> {
				collector.waitUntilDone ();
				returned.countDown ();
			});
			waiter.start ();
			assertTrue (returned.await (2, TimeUnit.SECONDS));
			waiter.join (1000);
		} finally {
			workers.shutdownNow ();
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
