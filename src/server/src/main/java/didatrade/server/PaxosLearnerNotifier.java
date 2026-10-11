package didatrade.server;

import java.util.List;
import java.util.concurrent.TimeUnit;

import didatrade.DidaTradePaxos;
import io.grpc.Context;

final class PaxosLearnerNotifier {
	private PaxosLearnerNotifier() {
	}

	static void notifyAccepted (
			DidaTradeServerState state, int instance, int ballot, int value) {
		Context context = Context.current ().fork ();
		context.run ( () -> {
			List<Integer> learners = state.scheduler.learners (ballot);
			DidaTradePaxos.LearnRequest request = DidaTradePaxos.LearnRequest.newBuilder ()
					.setInstance (instance)
					.setValue (value)
					.setBallot (ballot)
					.setServerid (state.my_id)
					.build ();

			System.out.println ("Paxos acceptor: going to notify learners for entry "
					+ instance + " with timestamp " + ballot + " request = " + request);

			for (int learner : learners) {
				state.async_stubs[learner]
						.withDeadlineAfter (2, TimeUnit.SECONDS)
						.learn (request, new io.grpc.stub.StreamObserver<DidaTradePaxos.LearnReply> () {
							@Override
							public void onNext (DidaTradePaxos.LearnReply value) {
							}

							@Override
							public void onError (Throwable throwable) {
							}

							@Override
							public void onCompleted () {
							}
						});
			}
		});
	}
}
