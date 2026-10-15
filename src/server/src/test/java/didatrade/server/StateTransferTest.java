package didatrade.server;

import static org.junit.jupiter.api.Assertions.*;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradeMaster;
import io.grpc.stub.StreamObserver;
import didatrade.configs.ConfigurationScheduler;
import didatrade.core.TradeManager;

class StateTransferTest {
	@Test
	@Timeout(8)
	void delayedInstallationCanOverlapFreshCommandAssignment () throws Exception {
		DidaTradeServerState s = state (4);
		CountDownLatch assigning = new CountDownLatch (1);
		CountDownLatch installing = new CountDownLatch (1);
		s.req_history = new RequestHistory () {
			@Override
			public RequestRecord getFirstPendingExcluding (Set<Integer> ids) {
				assigning.countDown ();
				try {
					assertTrue (installing.await (2, TimeUnit.SECONDS));
				} catch (InterruptedException e) {
					throw new AssertionError (e);
				}
				return super.getFirstPendingExcluding (ids);
			}
		};
		s.paxos_log = new PaxosLog () {
			@Override
			public PaxosInstance getEntry (int slot) {
				if (Thread.currentThread ().getName ().equals ("delayed-installation")) {
					// Fail before acquiring the log lock, so the old bug cannot leak
					// deadlocked threads when this regression fails.
					try {
						assertFalse (Thread.holdsLock (s.req_history), "assignment already holds the log and needs history");
					} finally {
						installing.countDown ();
					}
				}
				return super.getEntry (slot);
			}
		};
		s.setCurrentBallot (3);
		s.req_history.installIfAbsent (101, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10));
		s.req_history.moveToProcessed (101);
		PaxosInstance recovered = s.paxos_log.testAndSetEntry (0);
		recovered.decide (3, 101);
		recovered.markExecuted ();
		s.req_history.installIfAbsent (301, new DidaTradeCommand (DidaTradeAction.ADDUSER, 88, 200, 10));
		DidaTradePaxos.InstallStateRequest request = DidaTradePaxos.InstallStateRequest.newBuilder ().setBallot (3).setThroughinstance (0)
				.addEntries (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ().setInstance (0).setValue (101).setValballot (3))
				.addCommands (CommandTransfer.encode (s.req_history.getIfExists (101))).build ();
		FutureTask<PaxosInstance> assignment = new FutureTask<> ( () -> s.paxos_log.assignPendingRequest (1, s.req_history));
		FutureTask<Boolean> installation = new FutureTask<> ( () -> StateTransfer.install (s, request));
		Thread a = new Thread (assignment, "fresh-assignment");
		Thread b = new Thread (installation, "delayed-installation");
		try {
			a.start ();
			assertTrue (assigning.await (2, TimeUnit.SECONDS));
			b.start ();
			assertTrue (installation.get (2, TimeUnit.SECONDS));
			assertEquals (301, assignment.get (2, TimeUnit.SECONDS).getProposedRequestId ());
		} finally {
			installing.countDown ();
			a.join (2000);
			b.join (2000);
		}
	}

	@Test
	void missingCommandsCannotPublishACompleteTransfer () {
		DidaTradeServerState s = state (5);
		assertFalse (StateTransfer.install (s, prefix ()));
		assertFalse (s.membershipTransferred ());
		assertEquals (0, s.paxos_log.length ());
		assertEquals (0, s.getCurrentBallot ());
		assertNull (s.req_history.getIfExists (101));
	}

	@Test
	void transferredCommandsPreserveWaitersAndExecuteExactlyOnce () {
		DidaTradeServerState s = state (5);
		RequestRecord waiter = s.req_history.installIfAbsent (201, new DidaTradeCommand (DidaTradeAction.BUY, 77, 50));
		DidaTradePaxos.InstallStateRequest request = payloadPrefix ();
		assertTrue (StateTransfer.install (s, request));
		assertSame (waiter, s.req_history.getIfExists (201));
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (s);
		assertEquals (2, executor.drain ());
		assertEquals (50, s.trade_manager.balance (77));
		assertTrue (StateTransfer.install (s, request));
		assertEquals (0, executor.drain ());
		assertSame (waiter, s.req_history.getIfProcessed (201));
		assertEquals (50, s.trade_manager.balance (77));
	}

	@Test
	void malformedOrConflictingCommandsCannotPartiallyInstall () {
		for (DidaTradePaxos.InstallStateRequest request : new DidaTradePaxos.InstallStateRequest[]{
				payloadPrefix ().toBuilder ().setCommands (1, payloadPrefix ().getCommands (1).toBuilder ().setAction ("INVALID")).build (),
				payloadPrefix ().toBuilder ().addCommands (payloadPrefix ().getCommands (0).toBuilder ().setQuantity (999)).build (),
				payloadPrefix ().toBuilder ().setCommands (1, payloadPrefix ().getCommands (1).toBuilder ().setRequestid (-1)).build ()}) {
			DidaTradeServerState s = state (5);
			assertFalse (StateTransfer.install (s, request));
			assertNull (s.req_history.getIfExists (101));
			assertEquals (0, s.paxos_log.length ());
			assertFalse (s.membershipTransferred ());
		}
		DidaTradeServerState s = state (5);
		RequestRecord existing = s.req_history.installIfAbsent (201, new DidaTradeCommand (DidaTradeAction.BUY, 77, 25));
		assertFalse (StateTransfer.install (s, payloadPrefix ()));
		assertSame (existing, s.req_history.getIfExists (201));
		assertNull (s.req_history.getIfExists (101));
		assertEquals (0, s.paxos_log.length ());
		assertFalse (s.membershipTransferred ());
	}

	private static DidaTradePaxos.InstallStateRequest payloadPrefix () {
		return prefix ().toBuilder ()
				.addCommands (CommandTransfer.encode (new RequestRecord (101, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10))))
				.addCommands (CommandTransfer.encode (new RequestRecord (201, new DidaTradeCommand (DidaTradeAction.BUY, 77, 50))))
				.build ();
	}
	@Test
	void repeatedInstallationPreservesWaitersAndDoesNotRepeatTrade () {
		DidaTradeServerState s = state (5);
		RequestRecord waiter = s.req_history.installIfAbsent (101, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10));
		s.req_history.installIfAbsent (201, new DidaTradeCommand (DidaTradeAction.BUY, 77, 50));
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
	void partialOrStaleInstallationCannotPublishTheMarker () {
		DidaTradeServerState s = state (5);
		assertFalse (StateTransfer.install (s, prefix ().toBuilder ().clearEntries ().build ()));
		assertFalse (s.membershipTransferred ());
		assertEquals (0, s.paxos_log.length ());
		s.setCurrentBallot (3);
		assertFalse (StateTransfer.install (s, prefix ()));
		assertFalse (s.membershipTransferred ());
	}
	@Test
	void retiredAcceptorCanPromiseAndLearnButCannotVote () {
		DidaTradeServerState s = state (0);
		s.req_history.installIfAbsent (101, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10));
		s.req_history.installIfAbsent (201, new DidaTradeCommand (DidaTradeAction.BUY, 77, 50));
		assertTrue (s.processPhaseOne (2).accepted);
		assertFalse (s.processPhaseTwo (s.paxos_log.testAndSetEntry (0), 2, 101).accepted);
		assertTrue (StateTransfer.install (s, prefix ()));
		assertEquals (2, new OrderedPaxosExecutor (s).drain ());
		assertEquals (50, s.trade_manager.balance (77));
		assertFalse (s.paxos_log.getEntry (0).isAccepted ());
	}
	@Test
	void invalidCommandIdsAreRejectedBeforeInstallation () {
		DidaTradeServerState s = state (5);
		for (int value : new int[]{CommandIdHelper.ABSENT_VALUE, -3}) {
			DidaTradePaxos.InstallStateRequest invalid = prefix ().toBuilder ()
					.setEntries (0, prefix ().getEntries (0).toBuilder ().setValue (value)).build ();
			assertFalse (StateTransfer.install (s, invalid));
		}
		assertEquals (0, s.paxos_log.length ());
		assertFalse (s.membershipTransferred ());
	}
	@Test
	void executionWaitsForClientCommandsWithoutSkippingTheMissingSlot () {
		DidaTradeServerState s = state (5);
		// Normal learning may still precede client delivery; it must not skip slots.
		s.paxos_log.testAndSetEntry (0).decide (2, 101);
		s.paxos_log.testAndSetEntry (1).decide (2, 201);
		assertNull (s.req_history.getIfExists (101));
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (s);
		assertEquals (0, executor.drain ());
		s.req_history.installIfAbsent (201, new DidaTradeCommand (DidaTradeAction.BUY, 77, 50));
		assertEquals (0, executor.drain ());
		assertEquals (-1, s.getLastExecutedInstance ());
		s.req_history.installIfAbsent (101, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10));
		assertEquals (2, executor.drain ());
		assertEquals (50, s.trade_manager.balance (77));
		assertEquals (1, s.getLastExecutedInstance ());
	}
	@Test
	void incompleteAndConflictingPrefixesAreRejectedBeforeInstallation () {
		DidaTradeServerState s = state (5);
		assertFalse (StateTransfer.install (s, prefix ().toBuilder ().setThroughinstance (2).build ()));
		s.paxos_log.testAndSetEntry (1).decide (0, 999);
		assertFalse (StateTransfer.install (s, payloadPrefix ()));
		assertNull (s.req_history.getIfExists (101));
		assertNull (s.paxos_log.getEntry (0));
		assertFalse (s.membershipTransferred ());
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
		s.req_history.installIfAbsent (301, new DidaTradeCommand (DidaTradeAction.ADDUSER, 88, 100, 10));
		DidaTradePaxos.InstallStateRequest empty = DidaTradePaxos.InstallStateRequest.newBuilder ()
				.setBallot (2).setThroughinstance (-1).build ();
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
		return DidaTradePaxos.InstallStateRequest.newBuilder ().setBallot (2).setThroughinstance (1)
				.addEntries (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ().setInstance (0).setValue (101).setValballot (2))
				.addEntries (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ().setInstance (1).setValue (201).setValballot (2)).build ();
	}
}
