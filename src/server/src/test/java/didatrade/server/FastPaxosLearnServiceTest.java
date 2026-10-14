package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;
import io.grpc.stub.StreamObserver;

class FastPaxosLearnServiceTest {

	@Test
	void matchingFastQuorumDecidesWithoutChangingBallot () {
		DidaTradeServerState state = state ();
		DidaTradePaxosServiceImpl service = new DidaTradePaxosServiceImpl (state);

		learn (service, 0, 0, 100, 0);
		learn (service, 0, 0, 100, 1);
		learn (service, 0, 0, 100, 2);
		learn (service, 0, 0, 100, 3);

		PaxosInstance entry = state.paxos_log.getEntry (0);
		assertTrue (entry.isDecided ());
		assertEquals (100, entry.getCommandId ());
		assertEquals (0, state.getCurrentBallot ());
	}

	@Test
	void threeWayFastCollisionWaitsForManualBallotChange () {
		DidaTradeServerState state = state ();
		DidaTradePaxosServiceImpl service = new DidaTradePaxosServiceImpl (state);

		learn (service, 0, 0, 100, 0);
		learn (service, 0, 0, 200, 1);
		assertEquals (0, state.getCurrentBallot ());
		learn (service, 0, 0, 300, 2);

		PaxosInstance entry = state.paxos_log.getEntry (0);
		assertFalse (entry.isDecided ());
		assertTrue (entry.cannotReachQuorum (0, 4, 5));
		assertEquals (0, state.getCurrentBallot ());
		// The operator must explicitly select a new ballot to initiate recovery.
		state.setCurrentBallot (1);
		assertEquals (1, state.getCurrentBallot ());
	}

	@Test
	void staleCollisionEvidenceNeverAdvancesBallotAutomatically () {
		DidaTradeServerState state = state ();
		DidaTradePaxosServiceImpl service = new DidaTradePaxosServiceImpl (state);

		learn (service, 0, 0, 100, 0);
		learn (service, 0, 0, 200, 1);
		learn (service, 0, 0, 300, 2);
		assertEquals (0, state.getCurrentBallot ());

		learn (service, 0, 0, 100, 0);

		assertEquals (0, state.getCurrentBallot ());
	}

	@Test
	void conflictingDuplicateFromOneAcceptorDoesNotPoisonLaterMajority () {
		DidaTradeServerState state = state ();
		DidaTradePaxosServiceImpl service = new DidaTradePaxosServiceImpl (state);

		learn (service, 0, 0, 100, 0);
		learn (service, 0, 0, 200, 0);
		learn (service, 0, 0, 200, 1);
		assertFalse (state.paxos_log.getEntry (0).isDecided ());
		learn (service, 0, 0, 100, 2);
		learn (service, 0, 0, 100, 3);
		learn (service, 0, 0, 100, 4);

		assertTrue (state.paxos_log.getEntry (0).isDecided ());
		assertEquals (100, state.paxos_log.getEntry (0).getCommandId ());
		assertEquals (0, state.getCurrentBallot ());
	}

	@Test
	void threeClassicPhaseTwoLearnsDecideRecoveredSlotInScheduleB () {
		DidaTradeServerState state = state ();
		DidaTradePaxosServiceImpl service = new DidaTradePaxosServiceImpl (state);

		for (int acceptor = 0; acceptor < 2; acceptor++)
			learnClassic (service, 0, 1, 100, acceptor);
		assertFalse (state.paxos_log.getEntry (0).isDecided ());
		learnClassic (service, 0, 1, 100, 2);
		assertTrue (state.paxos_log.getEntry (0).isDecided ());
		assertEquals (100, state.paxos_log.getEntry (0).getCommandId ());
	}

	private void learnClassic (
			DidaTradePaxosServiceImpl service, int instance, int ballot, int value, int server_id) {
		DidaTradePaxos.LearnRequest request = DidaTradePaxos.LearnRequest.newBuilder ()
				.setInstance (instance).setBallot (ballot).setValue (value)
				.setServerid (server_id).setClassic (true).build ();
		service.learn (request, NOOP_OBSERVER);
	}

	private DidaTradeServerState state () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.scheduler = new ConfigurationScheduler ('B');
		state.paxos_log = new PaxosLog ();
		state.req_history = new RequestHistory ();
		state.my_id = 0;
		state.main_loop = new MainLoop (state);
		return state;
	}

	private void learn (
			DidaTradePaxosServiceImpl service,
			int instance,
			int ballot,
			int value,
			int server_id) {
		DidaTradePaxos.LearnRequest request = DidaTradePaxos.LearnRequest.newBuilder ()
				.setInstance (instance)
				.setBallot (ballot)
				.setValue (value)
				.setServerid (server_id)
				.build ();
		service.learn (request, NOOP_OBSERVER);
	}

	private static final StreamObserver<DidaTradePaxos.LearnReply> NOOP_OBSERVER = new StreamObserver<> () {
		@Override
		public void onNext (DidaTradePaxos.LearnReply value) {
		}

		@Override
		public void onError (Throwable error) {
		}

		@Override
		public void onCompleted () {
		}
	};
}
