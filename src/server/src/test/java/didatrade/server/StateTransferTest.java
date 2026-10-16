package didatrade.server;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradeMaster;
import io.grpc.stub.StreamObserver;
import didatrade.configs.ConfigurationScheduler;
import didatrade.core.TradeManager;
import didatrade.server.request.DidaTradeAction;
import didatrade.server.request.DidaTradeCommand;
import didatrade.server.request.RequestHistory;
import didatrade.server.request.RequestRecord;
import didatrade.server.service.DidaTradeMasterServiceImpl;

class StateTransferTest {
	@Test
	void repeatedInstallationPreservesWaitersAndDoesNotRepeatTrade () {
		DidaTradeServerState s = state (5);
		RequestRecord waiter = new RequestRecord (101, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10));
		s.req_history.addToPending (101, waiter);
		s.req_history.addToPending (201, new RequestRecord (201, new DidaTradeCommand (DidaTradeAction.BUY, 77, 50)));
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
		assertEquals (3, s.getCurrentBallot ());
		assertEquals (0, s.paxos_log.length ());
		assertNull (s.req_history.getIfExists (101));
		assertFalse (s.membershipTransferred ());
	}
	@Test
	void retiredAcceptorCanPromiseAndLearnButCannotVote () {
		DidaTradeServerState s = state (0);
		s.req_history.addToPending (101, new RequestRecord (101, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10)));
		s.req_history.addToPending (201, new RequestRecord (201, new DidaTradeCommand (DidaTradeAction.BUY, 77, 50)));
		assertTrue (s.processPhaseOne (2).accepted);
		assertFalse (s.processPhaseTwo (s.paxos_log.testAndSetEntry (0), 2, 101).accepted);
		assertTrue (StateTransfer.install (s, prefix ()));
		assertEquals (2, new OrderedPaxosExecutor (s).drain ());
		assertEquals (50, s.trade_manager.balance (77));
		assertFalse (s.paxos_log.getEntry (0).isAccepted ());
	}

	@Test
	void executionWaitsForClientCommandsWithoutSkippingTheMissingSlot () {
		DidaTradeServerState s = state (5);
		// Client requests may arrive arbitrarily late, even after a completed transfer.
		assertTrue (StateTransfer.install (s, prefix ()));
		assertTrue (s.membershipTransferred ());
		assertNull (s.req_history.getIfExists (101));
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (s);
		assertEquals (0, executor.drain ());
		s.req_history.addToPending (201, new RequestRecord (201, new DidaTradeCommand (DidaTradeAction.BUY, 77, 50)));
		assertEquals (0, executor.drain ());
		assertEquals (-1, s.getLastExecutedInstance ());
		s.req_history.addToPending (101, new RequestRecord (101, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10)));
		assertEquals (2, executor.drain ());
		assertEquals (50, s.trade_manager.balance (77));
		assertEquals (1, s.getLastExecutedInstance ());
	}

	@Test
	void invalidConsoleBallotsCannotDisableAnOpenFastRound () {
		DidaTradeServerState s = state (3);
		assertTrue (s.openFastRound (2));
		for (int ballot : new int[]{-1, 5, 999, 3}) { // ballot 3 belongs to leader 4
			Throwable[] error = new Throwable[1];
			new DidaTradeMasterServiceImpl (s).newballot (
					DidaTradeMaster.NewBallotRequest.newBuilder ().setNewballot (ballot).build (),
					new StreamObserver<DidaTradeMaster.NewBallotReply> () {
						@Override
						public void onNext (DidaTradeMaster.NewBallotReply reply) {
							fail ("Invalid console input must not succeed");
						}
						@Override
						public void onError (Throwable failure) {
							error[0] = failure;
						}
						@Override
						public void onCompleted () {
							fail ("Invalid console input must be rejected");
						}
					});
			assertNotNull (error[0]);
			assertEquals (io.grpc.Status.Code.INVALID_ARGUMENT, io.grpc.Status.fromThrowable (error[0]).getCode ());
			assertEquals (2, s.getCurrentBallot ());
			assertTrue (s.isFastRoundOpen (2));
		}
	}

	@Test
	void delayedTransferPreservesVotesFromItsOwnBallot () {
		DidaTradeServerState s = state (3);
		s.req_history.addToPending (301, new RequestRecord (301, new DidaTradeCommand (DidaTradeAction.ADDUSER, 88, 100, 10)));
		DidaTradePaxos.InstallStateRequest empty = DidaTradePaxos.InstallStateRequest.newBuilder ()
				.setBallot (2).build ();
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
	void staticSchedulesPreserveInitialActivationAcknowledgement () {
		for (char schedule : new char[]{'A', 'B'}) {
			DidaTradeServerState s = state (0);
			s.scheduler = new ConfigurationScheduler (schedule);
			assertEquals (-1, s.getCompletedBallot ());
			assertTrue (activate (s, 0));
		}
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
		return DidaTradePaxos.InstallStateRequest.newBuilder ().setBallot (2)
				.addEntries (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ().setInstance (0).setValue (101).setValballot (2))
				.addEntries (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ().setInstance (1).setValue (201).setValballot (2)).build ();
	}
}
