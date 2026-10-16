package didatrade.util;

import io.grpc.stub.StreamObserver;

/** Forwards at most one outcome (reply or failure) of a call to the collector. */
public class CollectorStreamObserver<T> implements StreamObserver<T> {

	GenericResponseCollector<T> collector;
	private boolean done;

	public CollectorStreamObserver(GenericResponseCollector<T> c) {
		collector = c;
		this.done = false;
	}

	@Override
	public void onNext (T value) {
		// Handle the received response of type T
		// System.out.println("Received response: " + value);
		if (this.done == false) {
			collector.addResponse (value);
			this.done = true;
		}
	}

	@Override
	public void onError (Throwable t) {
		// Handle error
		// System.err.println("Error occurred: " + t.getMessage());
		if (this.done == false) {
			collector.addNoResponse ();
			this.done = true;
		}
	}

	@Override
	public void onCompleted () {
		// Handle stream completion
		// System.out.println("Stream completed");
		if (this.done == false) {
			collector.addNoResponse ();
			this.done = true;
		}
	}
}
