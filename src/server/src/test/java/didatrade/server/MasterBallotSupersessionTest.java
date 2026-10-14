package didatrade.server;

import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import didatrade.DidaTradeMaster;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

class MasterBallotSupersessionTest {
	@Test
	void higherConsoleBallotReleasesThePreviousWaiter () throws Exception {
		assertSuperseded (s -> s.setCurrentBallot (3));
	}

	@Test
	void higherPrepareReleasesThePreviousWaiter () throws Exception {
		assertSuperseded (s -> assertTrue (s.processPhaseOne (3).accepted));
	}

	private static void assertSuperseded (Consumer<DidaTradeServerState> supersede) throws Exception {
		DidaTradeServerState s = StateTransferTest.state (3);
		CountDownLatch finished = new CountDownLatch (1);
		AtomicReference<Throwable> error = new AtomicReference<> ();
		Thread waiter = new Thread ( () -> new DidaTradeMasterServiceImpl (s).newballot (
				DidaTradeMaster.NewBallotRequest.newBuilder ().setNewballot (2).build (),
				new StreamObserver<DidaTradeMaster.NewBallotReply> () {
					@Override
					public void onNext (DidaTradeMaster.NewBallotReply reply) {
					}
					@Override
					public void onError (Throwable failure) {
						error.set (failure);
						finished.countDown ();
					}
					@Override
					public void onCompleted () {
						finished.countDown ();
					}
				}), "superseded-ballot-waiter");
		waiter.start ();
		try {
			long deadline = System.nanoTime () + TimeUnit.SECONDS.toNanos (3);
			while (waiter.getState () != Thread.State.WAITING && System.nanoTime () < deadline)
				Thread.sleep (1);
			assertEquals (Thread.State.WAITING, waiter.getState ());
			assertEquals (2, s.getCurrentBallot ());
			supersede.accept (s);
			assertTrue (finished.await (1, TimeUnit.SECONDS), "a superseded ballot cannot complete on its old leader");
			assertNotNull (error.get ());
			assertEquals (Status.Code.ABORTED, Status.fromThrowable (error.get ()).getCode ());
			assertEquals (-1, s.getCompletedBallot ());
		} finally {
			// Release the original buggy waiter too, so a failing regression never leaks a thread.
			s.setCompletedBallot (3);
			waiter.join (2000);
		}
	}
}
