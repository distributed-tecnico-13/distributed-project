package didatrade.server;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradeMaster;
import io.grpc.stub.StreamObserver;
import didatrade.configs.ConfigurationScheduler;
import didatrade.core.TradeManager;

class StateTransferTest {
	@Test
	void repeatedInstallationPreservesWaitersAndDoesNotRepeatTrade () {
		DidaTradeServerState s = state (5);
		RequestRecord waiter = receive (s, 101, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10));
		receive (s, 201, new DidaTradeCommand (DidaTradeAction.BUY, 77, 50));
		assertTrue (StateTransfer.install (s, prefix ()));
		assertSame (waiter, s.req_history.getIfExists (101));
		assertTrue (s.membershipTransferred ());
		OrderedPaxosExecutor e = new OrderedPaxosExecutor (s);
		assertEquals (2, e.drain ());
		assertEquals (50, s.trade_manager.balance (77));
		assertTrue (StateTransfer.install (s, prefix ()));
		assertEquals (0, e.drain ());
		assertEquals (50, s.trade_manager.balance (77));
	}
	@Test
	void staleInstallationCannotPublishTheMarker () {
		DidaTradeServerState s = state (5);
		s.setCurrentBallot (3);
		assertFalse (StateTransfer.install (s, prefix ()));
		assertFalse (s.membershipTransferred ());
	}
	@Test
	void retiredAcceptorCanPromiseAndLearnButCannotVote () {
		DidaTradeServerState s = state (0);
		receive (s, 101, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10));
		receive (s, 201, new DidaTradeCommand (DidaTradeAction.BUY, 77, 50));
		assertTrue (s.processPhaseOne (2).accepted);
		assertFalse (s.processPhaseTwo (s.paxos_log.testAndSetEntry (0), 2, 101).accepted);
		assertTrue (StateTransfer.install (s, prefix ()));
		assertEquals (2, new OrderedPaxosExecutor (s).drain ());
		assertEquals (50, s.trade_manager.balance (77));
		assertFalse (s.paxos_log.getEntry (0).isAccepted ());
	}
	@Test
	void incompleteAndConflictingPrefixesAreRejectedBeforeInstallation () {
		DidaTradeServerState s = state (5);
		assertFalse (StateTransfer.install (s, prefix ().toBuilder ().setThroughinstance (2).build ()));
		s.paxos_log.testAndSetEntry (1).decide (0, 999);
		assertFalse (StateTransfer.install (s, prefix ()));
		assertNull (s.paxos_log.getEntry (0));
		assertFalse (s.membershipTransferred ());
	}
	@Test
	void invalidBallotsCannotDisableAnOpenFastRound () {
		DidaTradeServerState s = state (3);
		assertTrue (s.openFastRound (2));
		assertFalse (s.processPhaseOne (999).accepted);
		assertTrue (s.isFastRoundOpen (2));
		PaxosInstance entry = s.paxos_log.testAndSetEntry (0);
		assertFalse (s.processPhaseOne (entry, 999).accepted);
		assertTrue (s.isFastRoundOpen (2));
		assertFalse (s.processPhaseTwo (entry, 999, 101).accepted);
		assertTrue (s.isFastRoundOpen (2));
	}

	@Test
	void delayedTransferPreservesVotesFromItsOwnBallot () {
		DidaTradeServerState s = state (3);
		receive (s, 301, new DidaTradeCommand (DidaTradeAction.ADDUSER, 88, 100, 10));
		DidaTradePaxos.InstallStateRequest empty = DidaTradePaxos.InstallStateRequest.newBuilder ()
				.setBallot (2).setSenderid (3).setThroughinstance (-1).build ();
		assertTrue (StateTransfer.install (s, empty));
		assertTrue (s.openFastRound (2));
		DidaTradeServerState.FastVoteState newVote = s.processFastVote (301);
		assertTrue (newVote.accepted);
		assertEquals (0, newVote.instance);
		assertTrue (StateTransfer.install (s, empty), "a delayed retry may arrive after Fast opens");
		assertEquals (2, s.paxos_log.getEntry (0).getWriteBallot ());
		assertEquals (301, s.paxos_log.getEntry (0).getAcceptedValue ());
	}

	@Test
	void activationAcknowledgesOnlyACompletedCurrentLeaderBallot () {
		DidaTradeServerState s = state (3);
		s.setCurrentBallot (2);
		assertFalse (activate (s, 2));
		s.setCompletedBallot (2);
		assertTrue (activate (s, 2));
		assertFalse (activate (s, 1));
		assertFalse (activate (s, 3));
	}

	private static boolean activate (DidaTradeServerState s, int ballot) {
		boolean[] accepted = new boolean[1];
		new DidaTradeMasterServiceImpl (s).activate (DidaTradeMaster.ActivateRequest.newBuilder ()
				.setCompletedballot (ballot).build (), new StreamObserver<DidaTradeMaster.ActivateReply> () {
					@Override
					public void onNext (DidaTradeMaster.ActivateReply reply) {
						accepted[0] = reply.getAck ();
					}
					@Override
					public void onError (Throwable error) {
						throw new AssertionError (error);
					}
					@Override
					public void onCompleted () {
					}
				});
		return accepted[0];
	}

	static DidaTradeServerState state (int id) {
		DidaTradeServerState s = new DidaTradeServerState ();
		s.my_id = id;
		s.scheduler = new ConfigurationScheduler ('C');
		s.req_history = new RequestHistory ();
		s.paxos_log = new PaxosLog ();
		s.trade_manager = new TradeManager ();
		s.setRanPhaseOne (-1);
		s.main_loop = new MainLoop (s);
		return s;
	}
	static DidaTradePaxos.InstallStateRequest prefix () {
		return DidaTradePaxos.InstallStateRequest.newBuilder ().setBallot (2).setSenderid (3).setThroughinstance (1)
				.addEntries (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ().setInstance (0).setValue (101).setValballot (2))
				.addEntries (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ().setInstance (1).setValue (201).setValballot (2)).build ();
	}
	// Clients send every request to every replica; Paxos only orders request ids.
	static RequestRecord receive (DidaTradeServerState s, int id, DidaTradeCommand command) {
		RequestRecord record = new RequestRecord (id, command);
		s.req_history.addToPending (id, record);
		return record;
	}
}
