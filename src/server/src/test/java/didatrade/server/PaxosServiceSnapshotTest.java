package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;
import io.grpc.stub.StreamObserver;

class PaxosServiceSnapshotTest {

	@Test
	void phaseOneReplyContainsEveryLocallyAcceptedEntryAndNoUnacceptedCandidate () {
		DidaTradeServerState state = newState ();
		PaxosInstance accepted0 = state.paxos_log.testAndSetEntry (0);
		PaxosInstance candidate1 = state.paxos_log.testAndSetEntry (1);
		PaxosInstance accepted2 = state.paxos_log.testAndSetEntry (2);
		candidate1.setProposedRequestId (777);
		assertTrue (state.processPhaseTwo (accepted0, 2, 101).accepted);
		assertTrue (state.processPhaseTwo (accepted2, 4, 303).accepted);

		DidaTradePaxos.PhaseOneReply reply = callPhaseOne (state, 5);

		assertTrue (reply.getAccepted ());
		assertEquals (5, reply.getMaxballot ());
		assertEquals (2, reply.getResponseLogObjectsCount ());
		assertTrue (contains (reply, 0, 2, 101));
		assertTrue (contains (reply, 2, 4, 303));
		assertFalse (reply.getResponseLogObjectsList ().stream ()
				.anyMatch (entry -> entry.getInstance () == 1));
	}

	@Test
	void rejectedLowerPhaseOneReportsCurrentHigherBallot () {
		DidaTradeServerState state = newState ();
		state.setCurrentBallot (8);

		DidaTradePaxos.PhaseOneReply reply = callPhaseOne (state, 5);

		assertFalse (reply.getAccepted ());
		assertEquals (8, reply.getMaxballot ());
		assertEquals (8, state.getCurrentBallot ());
	}

	private static DidaTradeServerState newState () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.scheduler = new ConfigurationScheduler ('A');
		state.paxos_log = new PaxosLog ();
		state.my_id = 1;
		return state;
	}

	private static DidaTradePaxos.PhaseOneReply callPhaseOne (
			DidaTradeServerState state, int ballot) {
		List<DidaTradePaxos.PhaseOneReply> replies = new ArrayList<> ();
		DidaTradePaxosServiceImpl service = new DidaTradePaxosServiceImpl (state);
		service.phaseone (DidaTradePaxos.PhaseOneRequest.newBuilder ()
				.setRequestballot (ballot)
				.build (), new StreamObserver<> () {
					@Override
					public void onNext (DidaTradePaxos.PhaseOneReply value) {
						replies.add (value);
					}

					@Override
					public void onError (Throwable t) {
						throw new AssertionError (t);
					}

					@Override
					public void onCompleted () {
					}
				});
		assertEquals (1, replies.size ());
		return replies.getFirst ();
	}

	private static boolean contains (
			DidaTradePaxos.PhaseOneReply reply, int instance, int ballot, int value) {
		return reply.getResponseLogObjectsList ().stream ()
				.anyMatch (entry -> entry.getInstance () == instance
						&& entry.getValballot () == ballot
						&& entry.getValue () == value);
	}
}
