package didatrade.util;

import java.util.ArrayList;

/**
 * Collects async replies. Done when the processor says so, or when every reply or failure has
 * arrived.
 */
public class GenericResponseCollector<T> {
	private GenericResponseProcessor<T> processor;
	private ArrayList<T> collected_responses;
	private int received;
	private int pending;
	private boolean done;

	public GenericResponseCollector(ArrayList<T> responses, int maxresponses) {
		this.processor = null;
		this.collected_responses = responses;
		this.received = 0;
		this.pending = maxresponses;
		this.done = false;
	}

	public GenericResponseCollector(ArrayList<T> responses, int maxresponses, GenericResponseProcessor<T> p) {
		this.processor = p;
		this.collected_responses = responses;
		this.received = 0;
		this.pending = maxresponses;
		this.done = false;
	}

	public synchronized void addResponse (T resp) {
		if (!this.done) {
			collected_responses.add (resp);
			if (this.processor != null)
				this.done = this.processor.onNext (this.collected_responses, resp);
		}
		this.received++;
		this.pending--;
		if (this.pending == 0)
			this.done = true;
		notifyAll ();
	}

	// A failed call still counts towards "everyone answered".
	public synchronized void addNoResponse () {
		this.pending--;
		if (this.pending == 0)
			this.done = true;
		notifyAll ();
	}

	// For callers without a processor: waits for quorum replies, or until all calls have replied or
	// failed.
	public synchronized void waitForQuorum (int quorum) {
		while ((this.done == false) && (this.received < quorum)) {
			try {
				wait ();
			} catch (InterruptedException e) {
			}
		}
		this.done = true;
	}

	public synchronized void waitUntilDone () {
		while (this.done == false) {
			try {
				wait ();
			} catch (InterruptedException e) {
			}
		}
	}
}
