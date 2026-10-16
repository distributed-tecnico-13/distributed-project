package didatrade.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/** Fixed workers run independent attempts and wake the coordinator when one finishes. */
final class PaxosPipeline {
	static final int CAPACITY = 4;

	private final ExecutorService workers;
	private final Semaphore capacity;
	private final Runnable completion_signal;

	PaxosPipeline(Runnable completion_signal) {
		this.workers = Executors.newFixedThreadPool (CAPACITY);
		this.capacity = new Semaphore (CAPACITY);
		this.completion_signal = completion_signal;
	}

	boolean hasCapacity () {
		return this.capacity.availablePermits () > 0;
	}

	boolean submitTask (Runnable task) {
		if (!this.capacity.tryAcquire ())
			return false;
		try {
			this.workers.execute ( () -> {
				try {
					task.run ();
				} catch (RuntimeException e) {
					// Would otherwise end this worker thread with only a stack trace on stderr.
					System.err.println ("PaxosPipeline: task failed: " + e);
					e.printStackTrace ();
				} finally {
					this.capacity.release ();
					this.completion_signal.run ();
				}
			});
		} catch (RuntimeException e) {
			this.capacity.release ();
			throw e;
		}
		return true;
	}

	void shutdown () {
		this.workers.shutdownNow ();
	}
}
