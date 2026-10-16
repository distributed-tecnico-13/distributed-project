package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;
import io.grpc.stub.StreamObserver;
import didatrade.server.request.RequestHistory;
import didatrade.server.service.DidaTradePaxosServiceImpl;

class FastPaxosLearnServiceTest {

	@Test
	void scheduleCRequiresFourFastVotesButThreeClassicVotesInBothConfigurations () {
		for (int ballot : new int[]{0, 2}) {
			DidaTradeServerState state = state ();
			state.scheduler = new ConfigurationScheduler ('C');
			DidaTradePaxosServiceImpl service = new DidaTradePaxosServiceImpl (state);
			java.util.List<Integer> acceptors = state.scheduler.acceptors (ballot);
			for (int i = 0; i < 3; i++)
				learn (service, 0, ballot, 100, acceptors.get (i));
			assertFalse (state.paxos_log.getEntry (0).isDecided ());
			learn (service, 0, ballot, 100, acceptors.get (0));
			assertFalse (state.paxos_log.getEntry (0).isDecided ());
			learn (service, 0, ballot, 100, acceptors.get (3));
			assertTrue (state.paxos_log.getEntry (0).isDecided ());
			assertEquals (100, state.paxos_log.getEntry (0).getDecidedValue ());

			for (int i = 0; i < 2; i++)
				learnClassic (service, 1, ballot, 200, acceptors.get (i));
			assertFalse (state.paxos_log.getEntry (1).isDecided ());
			learnClassic (service, 1, ballot, 200, acceptors.get (2));
			assertTrue (state.paxos_log.getEntry (1).isDecided ());
			assertEquals (200, state.paxos_log.getEntry (1).getDecidedValue ());
		}
	}

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
		assertEquals (100, entry.getDecidedValue ());
		assertEquals (0, state.getCurrentBallot ());
	}

	@Test
	void threeWayFastCollisionAdvancesToNextBallot () {
		DidaTradeServerState state = state ();
		DidaTradePaxosServiceImpl service = new DidaTradePaxosServiceImpl (state);

		learn (service, 0, 0, 100, 0);
		learn (service, 0, 0, 200, 1);
		assertEquals (0, state.getCurrentBallot ());
		learn (service, 0, 0, 300, 2);

		assertFalse (state.paxos_log.getEntry (0).isDecided ());
		assertEquals (1, state.getCurrentBallot ());
	}

	@Test
	void splitBelowFastQuorumOfLearnsKeepsBallot () {
		DidaTradeServerState state = state ();
		DidaTradePaxosServiceImpl service = new DidaTradePaxosServiceImpl (state);

		learn (service, 0, 0, 100, 0);
		learn (service, 0, 0, 100, 1);
		learn (service, 0, 0, 200, 2);

		assertFalse (state.paxos_log.getEntry (0).isDecided ());
		assertEquals (0, state.getCurrentBallot ());
	}

	@Test
	void fastQuorumOfLearnsWithoutQuorumValueAdvancesBallot () {
		DidaTradeServerState state = state ();
		DidaTradePaxosServiceImpl service = new DidaTradePaxosServiceImpl (state);

		// Acceptor 4 may never vote in this slot, so waiting for it could block forever.
		learn (service, 0, 0, 100, 0);
		learn (service, 0, 0, 100, 1);
		learn (service, 0, 0, 100, 2);
		assertEquals (0, state.getCurrentBallot ());
		learn (service, 0, 0, 200, 3);

		assertFalse (state.paxos_log.getEntry (0).isDecided ());
		assertEquals (1, state.getCurrentBallot ());
	}

	@Test
	void lateLearnsFromCollidedBallotAdvanceOnlyOnce () {
		DidaTradeServerState state = state ();
		DidaTradePaxosServiceImpl service = new DidaTradePaxosServiceImpl (state);

		learn (service, 0, 0, 100, 0);
		learn (service, 0, 0, 200, 1);
		learn (service, 0, 0, 300, 2);
		assertEquals (1, state.getCurrentBallot ());

		learn (service, 0, 0, 400, 3);
		learn (service, 1, 0, 500, 0);
		learn (service, 1, 0, 600, 1);
		learn (service, 1, 0, 700, 2);

		assertEquals (1, state.getCurrentBallot ());
	}

	@Test
	void classicLearnsNeverAdvanceBallot () {
		DidaTradeServerState state = state ();
		DidaTradePaxosServiceImpl service = new DidaTradePaxosServiceImpl (state);

		learnClassic (service, 0, 0, 100, 0);
		learnClassic (service, 0, 0, 200, 1);
		learnClassic (service, 0, 0, 300, 2);

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
		// Four Learns split 3-1 advance the ballot, yet the fifth can still choose 100.
		assertEquals (1, state.getCurrentBallot ());
		learn (service, 0, 0, 100, 4);

		assertTrue (state.paxos_log.getEntry (0).isDecided ());
		assertEquals (100, state.paxos_log.getEntry (0).getDecidedValue ());
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
		assertEquals (100, state.paxos_log.getEntry (0).getDecidedValue ());
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
