package didatrade.console;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import didatrade.DidaTradeMaster;
import didatrade.DidaTradeMasterServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

@Timeout(10)
class ConsoleTest {
	@Test
	void rejectedBallotDoesNotPreventAValidBallot () throws Exception {
		assertRejectedCommandDoesNotAdvanceBallot ("ballot 5 0");
	}

	@Test
	void rejectedActivationDoesNotPreventAValidBallot () throws Exception {
		assertRejectedCommandDoesNotAdvanceBallot ("activate 4 0");
	}

	private void assertRejectedCommandDoesNotAdvanceBallot (String firstCommand) throws Exception {
		CountDownLatch rejected = new CountDownLatch (1);
		CountDownLatch valid = new CountDownLatch (1);
		CountDownLatch accepted = new CountDownLatch (1);
		Server server = ServerBuilder.forPort (0).addService (new DidaTradeMasterServiceGrpc.DidaTradeMasterServiceImplBase () {
			@Override
			public void newballot (DidaTradeMaster.NewBallotRequest request, StreamObserver<DidaTradeMaster.NewBallotReply> observer) {
				if (request.getNewballot () == 0) {
					observer.onNext (DidaTradeMaster.NewBallotReply.newBuilder ().setCompletedballot (0).build ());
					observer.onCompleted ();
					valid.countDown ();
				} else {
					observer.onError (Status.INVALID_ARGUMENT.asRuntimeException ());
					rejected.countDown ();
				}
			}
			@Override
			public void activate (DidaTradeMaster.ActivateRequest request, StreamObserver<DidaTradeMaster.ActivateReply> observer) {
				observer.onNext (DidaTradeMaster.ActivateReply.newBuilder ().setAck (false).build ());
				observer.onCompleted ();
				rejected.countDown ();
			}
		}).build ().start ();
		InputStream original = System.in;
		var worker = Executors.newSingleThreadExecutor ();
		try (PipedInputStream terminal = new PipedInputStream ();
				PipedOutputStream input = new PipedOutputStream (terminal)) {
			System.setIn (terminal);
			Console console = new Console () {
				@Override
				public void setBallotCompleted (int ballot) {
					super.setBallotCompleted (ballot);
					accepted.countDown ();
				}
			};
			var loop = worker.submit ( () -> {
				console.main_loop (new String[]{"127.0.0.1", Integer.toString (server.getPort ()), "C"});
				return null;
			});
			try {
				input.write ((firstCommand + "\n").getBytes (StandardCharsets.UTF_8));
				input.flush ();
				assertTrue (rejected.await (3, TimeUnit.SECONDS));
				assertEquals (-1, console.getLastBallot ());
				input.write ("ballot 0 0\n".getBytes (StandardCharsets.UTF_8));
				input.flush ();
				assertTrue (valid.await (3, TimeUnit.SECONDS), "the console must send the valid ballot after rejection");
				assertTrue (accepted.await (3, TimeUnit.SECONDS));
				assertEquals (0, console.getLastBallot ());
			} finally {
				input.write ("exit\n".getBytes (StandardCharsets.UTF_8));
				input.flush ();
				loop.get (2, TimeUnit.SECONDS);
			}
		} finally {
			System.setIn (original);
			worker.shutdownNow ();
			server.shutdownNow ();
			server.awaitTermination (2, TimeUnit.SECONDS);
		}
	}
}
