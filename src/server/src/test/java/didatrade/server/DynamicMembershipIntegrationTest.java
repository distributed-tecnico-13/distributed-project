package didatrade.server;

import static org.junit.jupiter.api.Assertions.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import didatrade.DidaTradeMain;
import didatrade.DidaTradeMainServiceGrpc;
import didatrade.DidaTradeMaster;
import didatrade.DidaTradeMasterServiceGrpc;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;
import io.grpc.stub.StreamObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import didatrade.server.request.DidaTradeAction;
import didatrade.server.request.DidaTradeCommand;
import didatrade.server.service.DidaTradeMainServiceImpl;
import didatrade.server.service.DidaTradeMasterServiceImpl;
import didatrade.server.service.DidaTradePaxosServiceImpl;

@SuppressWarnings("try")
class DynamicMembershipIntegrationTest {
	@Test
	@Timeout(40)
	void transfersOldChosenHistoryAndContinuesAfterRetirement () throws Exception {
		try (Cluster c = new Cluster ()) {
			// Clients have broadcast to every learner, but only an old majority
			// has voted. No Learn reached the new leader or new-exclusive acceptors.
			DidaTradeCommand[] commands = {
					new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10),
					new DidaTradeCommand (DidaTradeAction.BUY, 77, 50),
					new DidaTradeCommand (DidaTradeAction.ADDUSER, 88, 10, 100),
					new DidaTradeCommand (DidaTradeAction.SELL, 88, 25)
			};
			for (int slot = 0; slot < commands.length; slot++)
				c.rememberClientCommand (101 + slot * 100, commands[slot]);
			for (int id : List.of (0, 1, 2)) {
				for (int slot = 0; slot < commands.length; slot++) {
					int request = 101 + slot * 100;
					assertTrue (c.states[id].processPhaseTwo (c.states[id].paxos_log.testAndSetEntry (slot), 1, request).accepted);
				}
			}
			c.startLeader (3);
			c.ballot (2, 3);
			await ( () -> c.states[5].getLastExecutedInstance () >= 3);
			assertEquals (50, c.states[5].trade_manager.balance (77));
			assertEquals (35, c.states[5].trade_manager.balance (88));
			await ( () -> List.of (2, 3, 4, 5, 6).stream ().allMatch (id -> c.states[id].membershipTransferred ()));

			// Three old members are unavailable: the old group has no quorum,
			// but the new group still has four live acceptors.
			c.states[0].setDebugMode (2);
			c.states[1].setDebugMode (2);
			c.states[2].setDebugMode (2);
			c.startLeader (4);
			c.ballot (3, 4);
			DidaTradeMain.AddUserReply reply = broadcastAdd (c, 501, 99, 321, 3, 4, 5, 6);
			assertTrue (reply.getResult ());
			// Redundant proposals may occupy slots after the recovered prefix.
			assertTrue (reply.getInstance () >= commands.length);
			// Replay must preserve the BUY's 25 remaining reserved units.
			List<java.util.concurrent.CompletableFuture<DidaTradeMain.SellReply>> sales = new ArrayList<> ();
			for (int id = 0; id < 7; id++) {
				final int target = id;
				sales.add (java.util.concurrent.CompletableFuture.supplyAsync ( () -> c.client (target)
						.withDeadlineAfter (8, TimeUnit.SECONDS).sell (DidaTradeMain.SellRequest.newBuilder ()
								.setReqid (601).setUserid (88).setQuantity (40).build ())));
			}
			for (int id : List.of (3, 4, 5, 6))
				assertTrue (sales.get (id).get (10, TimeUnit.SECONDS).getResult ());
			await ( () -> c.states[6].getLastExecutedInstance () >= 5);
			assertEquals (60, c.states[6].trade_manager.balance (88));
			assertEquals (50, c.states[6].trade_manager.balance (77));
			c.startLeader (5);
			c.ballot (4, 5);
			DidaTradeMain.BalanceReply balance = c.client (5).withDeadlineAfter (5, TimeUnit.SECONDS).balance (
					DidaTradeMain.BalanceRequest.newBuilder ().setUserid (88).setMininstance (5).build ());
			assertEquals (60, balance.getBalance ());
			assertTrue (balance.getInstance () >= 5);
		}
	}

	@Test
	@Timeout(20)
	void emptyClusterCompletesTransferWithoutAClientCommand () throws Exception {
		try (Cluster c = new Cluster ()) {
			c.startLeader (3);
			c.ballot (2, 3);
			assertEquals (2, c.states[3].getCompletedBallot ());
			assertEquals (-1, c.states[3].getLastExecutedInstance ());
		}
	}

	@Test
	@Timeout(20)
	void transferCompletesBeforeDelayedClientCommandsAndExecutionResumesInOrder () throws Exception {
		try (Cluster c = new Cluster ()) {
			for (int id : List.of (0, 1, 2)) {
				assertTrue (c.states[id].processPhaseTwo (c.states[id].paxos_log.testAndSetEntry (0), 1, 101).accepted);
				assertTrue (c.states[id].processPhaseTwo (c.states[id].paxos_log.testAndSetEntry (1), 1, 201).accepted);
			}
			c.ballot (2, 3);
			await ( () -> c.states[5].membershipTransferred ());
			assertEquals (2, c.states[3].getCompletedBallot ());
			assertEquals (-1, c.states[5].getLastExecutedInstance ());
			assertNull (c.states[5].req_history.getIfExists (101));
			java.util.concurrent.CompletableFuture<DidaTradeMain.BuyReply> buy = java.util.concurrent.CompletableFuture.supplyAsync (
					() -> c.client (5).withDeadlineAfter (8, TimeUnit.SECONDS).buy (DidaTradeMain.BuyRequest.newBuilder ()
							.setReqid (201).setUserid (77).setQuantity (50).build ()));
			await ( () -> c.states[5].req_history.getIfPending (201) != null);
			assertEquals (-1, c.states[5].getLastExecutedInstance ());
			assertFalse (buy.isDone ());
			DidaTradeMain.AddUserReply add = c.client (5).withDeadlineAfter (5, TimeUnit.SECONDS).adduser (
					DidaTradeMain.AddUserRequest.newBuilder ().setReqid (101).setUserid (77).setCash (100).setStock (10).build ());
			assertTrue (add.getResult ());
			assertEquals (0, add.getInstance ());
			DidaTradeMain.BuyReply result = buy.get (5, TimeUnit.SECONDS);
			assertTrue (result.getResult ());
			assertEquals (1, result.getInstance ());
			assertEquals (50, c.states[5].getBalance (1, 77).balance ());
		}
	}

	@Test
	@Timeout(20)
	void nextLeaderRecoversAFastCollisionWithoutAConsoleRequest () throws Exception {
		try (Cluster c = new Cluster ()) {
			for (int id = 0; id < 5; id++) {
				assertTrue (c.states[id].openFastRound (0));
				assertTrue (c.states[id].processFastVote (id < 3 ? 101 : 201).accepted);
			}
			for (int id = 0; id < 4; id++)
				DidaTradePaxosServiceGrpc.newBlockingStub (c.channels[1]).withDeadlineAfter (5, TimeUnit.SECONDS).learn (
						DidaTradePaxos.LearnRequest.newBuilder ().setInstance (0).setBallot (0)
								.setValue (id < 3 ? 101 : 201).setServerid (id).build ());
			await ( () -> c.states[1].getCompletedBallot () >= 1);
			PaxosInstance recovered = c.states[1].paxos_log.getEntry (0);
			assertTrue (recovered.isDecided ());
			assertEquals (1, recovered.getAcceptBallot ());
			assertTrue (recovered.getCommandId () == 101 || recovered.getCommandId () == 201);
			assertTrue (c.phaseOneCalls[0].get () > 0);
		}
	}

	@Test
	@Timeout(20)
	void directMembershipChangePreservesOldFastQuorum () throws Exception {
		try (Cluster c = new Cluster ()) {
			// A was chosen by four old Fast acceptors. The first new majority
			// sees A at 2,3 and B at 4; recovery must preserve A.
			c.rememberClientCommand (101, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10));
			c.rememberClientCommand (201, new DidaTradeCommand (DidaTradeAction.ADDUSER, 88, 100, 10));
			for (int id = 0; id < 5; id++) {
				int request = id < 4 ? 101 : 201;
				assertTrue (c.states[id].openFastRound (0));
				assertTrue (c.states[id].processFastVote (request).accepted);
			}
			// Force the first new majority to be 2,3,4, so it sees both values.
			// Old-group recovery is still required before the first transfer.
			c.states[5].setDebugMode (2);
			c.states[6].setDebugMode (2);
			c.ballot (2, 3);
			assertTrue (c.phaseOneCalls[0].get () > 0, "the first transfer must recover the old acceptor group");
			await ( () -> c.states[4].getLastExecutedInstance () >= 0);
			assertEquals (101, c.states[4].paxos_log.getEntry (0).getCommandId ());
			assertEquals (100, c.states[4].trade_manager.balance (77));
		}
	}

	@Test
	@Timeout(20)
	void transferredMarkerDoesNotRetainAmbiguousUnchosenOldTail () throws Exception {
		try (Cluster c = new Cluster ()) {
			// Neither minority value was chosen. The old majority 0,1,2 can
			// certify an empty history; transfer reaches only new majority 2,5,6.
			for (int id : List.of (3, 4)) {
				int request = id * 100 + 1;
				c.states[id].req_history.addToPending (request, new RequestRecord (request, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77 + id, 100, 10)));
				assertTrue (c.states[id].openFastRound (0));
				assertTrue (c.states[id].processFastVote (request).accepted);
			}
			for (int id : List.of (0, 1, 2))
				assertTrue (c.states[id].processPhaseOne (2).accepted);
			DidaTradePaxos.InstallStateRequest empty = DidaTradePaxos.InstallStateRequest.newBuilder ()
					.setBallot (2).build ();
			for (int id : List.of (2, 5, 6))
				assertTrue (StateTransfer.install (c.states[id], empty));
			assertFalse (c.states[3].membershipTransferred ());
			assertFalse (c.states[4].membershipTransferred ());
			// Force the next new majority to be 3,4,6. Only 6 has the marker;
			// 3 and 4 still hold obsolete, mutually ambiguous old Fast votes.
			for (int id : List.of (0, 1, 2, 5))
				c.states[id].setDebugMode (2);
			// The certified transfer must let the new cohort recover without
			// asking the retired group to disambiguate its obsolete minority votes.
			c.ballot (3, 4);
			assertEquals (3, c.states[4].getCompletedBallot ());
			assertEquals (0, c.phaseOneCalls[0].get ());
		}
	}

	@Test
	@Timeout(20)
	void leaderCompletionDoesNotClaimLocalInstallationWhenItsOwnPaxosServiceIsFrozen () throws Exception {
		try (Cluster c = new Cluster ()) {
			c.rememberClientCommand (101, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10));
			for (int id : List.of (0, 1, 2)) {
				assertTrue (c.states[id].processPhaseTwo (c.states[id].paxos_log.testAndSetEntry (0), 1, 101).accepted);
			}
			// The console/coordinator can run while its incoming Paxos RPCs are frozen.
			c.states[3].setDebugMode (2);
			c.ballot (2, 3);
			assertEquals (2, c.states[3].getCompletedBallot ());
			assertFalse (c.states[3].paxos_log.getEntry (0).isAccepted ());
			assertFalse (c.states[3].membershipTransferred (), "a marker certifies local installation, not just remote acknowledgements");
			c.ballot (3, 4);
			assertEquals (3, c.states[4].getCompletedBallot ());
		}
	}

	@Test
	@Timeout(20)
	void missingOldQuorumBlocksHandoffUntilItBecomesAvailable () throws Exception {
		try (Cluster c = new Cluster ()) {
			c.rememberClientCommand (101, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10));
			for (int id : List.of (0, 1, 2)) {
				assertTrue (c.states[id].processPhaseTwo (c.states[id].paxos_log.testAndSetEntry (0), 1, 101).accepted);
				c.states[id].setDebugMode (2);
			}
			io.grpc.StatusRuntimeException timeout = assertThrows (io.grpc.StatusRuntimeException.class,
					() -> DidaTradeMasterServiceGrpc.newBlockingStub (c.channels[3])
							.withDeadlineAfter (500, TimeUnit.MILLISECONDS)
							.newballot (DidaTradeMaster.NewBallotRequest.newBuilder ().setNewballot (2).build ()));
			assertEquals (io.grpc.Status.Code.DEADLINE_EXCEEDED, timeout.getStatus ().getCode ());
			assertEquals (-1, c.states[3].getCompletedBallot ());
			assertFalse (c.states[3].isFastRoundOpen (2));
			assertFalse (c.states[3].membershipTransferred ());
			// One returning old acceptor restores an old majority (0,3,4).
			c.states[0].setDebugMode (3);
			c.ballot (2, 3);
			await ( () -> c.states[6].getLastExecutedInstance () >= 0);
			assertEquals (100, c.states[6].trade_manager.balance (77));
		}
	}

	@Test
	@Timeout(20)
	void sparseOldHistoryTransfersAContiguousPrefixWithNoopGaps () throws Exception {
		try (Cluster c = new Cluster ()) {
			c.rememberClientCommand (101, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10));
			for (int id : List.of (0, 1, 2)) {
				assertTrue (c.states[id].processPhaseTwo (c.states[id].paxos_log.testAndSetEntry (2), 1, 101).accepted);
			}
			c.ballot (2, 3);
			await ( () -> c.states[6].getLastExecutedInstance () >= 2);
			assertTrue (c.states[6].paxos_log.getEntry (0).isNoop ());
			assertTrue (c.states[6].paxos_log.getEntry (1).isNoop ());
			assertEquals (100, c.states[6].getBalance (2, 77).balance ());
		}
	}

	@Test
	@Timeout(20)
	void partialTransferCannotCompleteButSameBallotCanRetry () throws Exception {
		try (Cluster c = new Cluster ()) {
			c.rejectInstall.addAll (List.of (2, 4, 5, 6));
			io.grpc.StatusRuntimeException timeout = assertThrows (io.grpc.StatusRuntimeException.class,
					() -> DidaTradeMasterServiceGrpc.newBlockingStub (c.channels[3])
							.withDeadlineAfter (500, TimeUnit.MILLISECONDS)
							.newballot (DidaTradeMaster.NewBallotRequest.newBuilder ().setNewballot (2).build ()));
			assertEquals (io.grpc.Status.Code.DEADLINE_EXCEEDED, timeout.getStatus ().getCode ());
			assertEquals (-1, c.states[3].getCompletedBallot ());
			assertFalse (c.states[3].isFastRoundOpen (2));
			c.rejectInstall.clear ();
			c.ballot (2, 3);
			assertEquals (2, c.states[3].getCompletedBallot ());
		}
	}

	@Test
	@Timeout(20)
	void interruptedHandoffStillConsultsOldConfiguration () throws Exception {
		try (Cluster c = new Cluster ()) {
			c.rememberClientCommand (101, new DidaTradeCommand (DidaTradeAction.ADDUSER, 77, 100, 10));
			for (int id : List.of (0, 1, 2)) {
				assertTrue (c.states[id].processPhaseTwo (c.states[id].paxos_log.testAndSetEntry (0), 1, 101).accepted);
			}
			// A previous leader only managed one recovery acceptance in ballot 2.
			assertTrue (c.states[5].processPhaseTwo (c.states[5].paxos_log.testAndSetEntry (0), 2, 101).accepted);
			assertFalse (c.states[5].membershipTransferred ());
			c.ballot (3, 4);
			assertTrue (c.phaseOneCalls[0].get () > 0, "old configuration was skipped without a completed handoff");
			await ( () -> c.states[6].getLastExecutedInstance () >= 0);
			assertEquals (100, c.states[6].trade_manager.balance (77));
		}
	}

	@Test
	@Timeout(25)
	void fastRequestsWorkBeforeAndAfterMembershipChange () throws Exception {
		try (Cluster c = new Cluster ()) {
			broadcastAdd (c, 101, 770, 100);
			c.ballot (1, 1);
			c.ballot (2, 3);
			broadcastAdd (c, 201, 771, 200);
			await ( () -> c.states[5].getLastExecutedInstance () >= 1);
			long fastVotes = List.of (2, 3, 4, 5, 6).stream ().filter (id -> {
				PaxosInstance e = c.states[id].paxos_log.getEntry (1);
				return e != null && e.getWriteBallot () == 2 && e.getAcceptedValue () == 201;
			}).count ();
			assertTrue (fastVotes >= 4, "new cohort did not use direct Fast voting");
			assertEquals (200, c.states[5].getBalance (1, 771).balance ());
		}
	}

	private static DidaTradeMain.AddUserReply broadcastAdd (Cluster c, int request, int user, int cash, int... waitFor) throws Exception {
		List<java.util.concurrent.CompletableFuture<DidaTradeMain.AddUserReply>> replies = new ArrayList<> ();
		for (int id = 0; id < 7; id++) {
			final int target = id;
			replies.add (java.util.concurrent.CompletableFuture.supplyAsync ( () -> c.client (target)
					.withDeadlineAfter (8, TimeUnit.SECONDS).adduser (DidaTradeMain.AddUserRequest.newBuilder ()
							.setReqid (request).setUserid (user).setCash (cash).setStock (20).build ())));
		}
		int[] targets = waitFor.length == 0 ? new int[]{0, 1, 2, 3, 4, 5, 6} : waitFor;
		DidaTradeMain.AddUserReply result = null;
		for (int target : targets) {
			result = replies.get (target).get (10, TimeUnit.SECONDS);
			assertTrue (result.getResult ());
		}
		return result;
	}

	private static void await (BooleanSupplier condition) throws Exception {
		long deadline = System.nanoTime () + TimeUnit.SECONDS.toNanos (8);
		while (!condition.getAsBoolean () && System.nanoTime () < deadline)
			Thread.sleep (10);
		assertTrue (condition.getAsBoolean (), "cluster did not make progress");
	}

	private static final class Cluster implements AutoCloseable {
		final DidaTradeServerState[] states = new DidaTradeServerState[7];
		final Server[] servers = new Server[7];
		final ManagedChannel[] channels = new ManagedChannel[7];
		final List<Thread> threads = new ArrayList<> ();
		final java.util.Set<Integer> rejectInstall = java.util.concurrent.ConcurrentHashMap.newKeySet ();
		final java.util.concurrent.atomic.AtomicInteger[] phaseOneCalls = new java.util.concurrent.atomic.AtomicInteger[7];
		Cluster() throws Exception {
			for (int id = 0; id < 7; id++) {
				states[id] = StateTransferTest.state (id);
				final int serverId = id;
				phaseOneCalls[id] = new java.util.concurrent.atomic.AtomicInteger ();
				servers[id] = ServerBuilder.forPort (0)
						.addService (new DidaTradeMainServiceImpl (states[id]))
						.addService (new DidaTradeMasterServiceImpl (states[id]))
						.addService (new DidaTradePaxosServiceImpl (states[id]) {
							@Override
							public void phaseone (DidaTradePaxos.PhaseOneRequest request, StreamObserver<DidaTradePaxos.PhaseOneReply> observer) {
								phaseOneCalls[serverId].incrementAndGet ();
								super.phaseone (request, observer);
							}
							@Override
							public void installstate (DidaTradePaxos.InstallStateRequest request, StreamObserver<DidaTradePaxos.InstallStateReply> observer) {
								if (rejectInstall.contains (serverId)) {
									observer.onNext (DidaTradePaxos.InstallStateReply.newBuilder ().setServerid (serverId)
											.setMaxballot (states[serverId].getCurrentBallot ()).build ());
									observer.onCompleted ();
								} else
									super.installstate (request, observer);
							}
						}).build ().start ();
				channels[id] = ManagedChannelBuilder.forAddress ("127.0.0.1", servers[id].getPort ()).usePlaintext ().build ();
			}
			for (DidaTradeServerState s : states) {
				s.async_stubs = new DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[7];
				for (int id = 0; id < 7; id++)
					s.async_stubs[id] = DidaTradePaxosServiceGrpc.newStub (channels[id]);
			}
			for (int id = 0; id < 7; id++)
				startLeader (id);
		}
		void rememberClientCommand (int request, DidaTradeCommand command) {
			for (DidaTradeServerState state : states)
				state.req_history.addToPending (request, new RequestRecord (request, new DidaTradeCommand (command.getAction (),
						command.getUserId (), command.getQuantity (), command.getStock ())));
		}
		void startLeader (int id) {
			if (states[id].main_loop_worker != null)
				return;
			Thread t = new Thread (states[id].main_loop, "dynamic-leader-" + id);
			states[id].main_loop_worker = t;
			threads.add (t);
			t.start ();
		}
		void ballot (int ballot, int leader) {
			DidaTradeMaster.NewBallotReply r = DidaTradeMasterServiceGrpc.newBlockingStub (channels[leader])
					.withDeadlineAfter (10, TimeUnit.SECONDS).newballot (DidaTradeMaster.NewBallotRequest.newBuilder ()
							.setNewballot (ballot).setCompletedballot (-1).build ());
			assertTrue (r.getCompletedballot () >= ballot);
		}
		DidaTradeMainServiceGrpc.DidaTradeMainServiceBlockingStub client (int id) {
			return DidaTradeMainServiceGrpc.newBlockingStub (channels[id]);
		}
		@Override
		public void close () throws Exception {
			for (DidaTradeServerState s : states)
				s.setDebugMode (3);
			for (Thread t : threads)
				t.interrupt ();
			for (Server s : servers)
				s.shutdownNow ();
			for (ManagedChannel c : channels)
				c.shutdownNow ();
			for (Thread t : threads)
				t.join (6000);
			for (Server s : servers)
				s.awaitTermination (3, TimeUnit.SECONDS);
		}
	}
}
