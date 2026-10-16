package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;
import io.grpc.stub.StreamObserver;
import didatrade.server.service.DidaTradePaxosServiceImpl;

class DidaTradePaxosServiceLearnTest {

	@Test
	void nonAcceptorLearnCannotAdvanceBallotOrCreateLogEntry () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.scheduler = new ConfigurationScheduler ('A');
		state.paxos_log = new PaxosLog ();
		DidaTradePaxosServiceImpl service = new DidaTradePaxosServiceImpl (state);
		DidaTradePaxos.LearnRequest request = DidaTradePaxos.LearnRequest.newBuilder ()
				.setInstance (3).setBallot (8).setValue (100).setServerid (99).build ();

		StreamObserver<DidaTradePaxos.LearnReply> observer = new StreamObserver<> () {
			@Override
			public void onNext (DidaTradePaxos.LearnReply reply) {
			}

			@Override
			public void onError (Throwable error) {
			}

			@Override
			public void onCompleted () {
			}
		};
		service.learn (request, observer);

		assertEquals (0, state.getCurrentBallot ());
		assertNull (state.paxos_log.getEntry (3));

		service.learn (request.toBuilder ().setServerid (0).build (), observer);

		assertEquals (8, state.getCurrentBallot ());
		PaxosInstance entry = state.paxos_log.getEntry (3);
		assertNotNull (entry);
		assertEquals (1, entry.getAcceptCount (8));
	}
}
