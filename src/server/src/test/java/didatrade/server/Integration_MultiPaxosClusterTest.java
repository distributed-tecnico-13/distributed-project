package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import didatrade.DidaTradeMain;
import didatrade.DidaTradeMainServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * Real-TCP cluster tests. The name ends in Test, so they run with the normal suite
 * (`just test`). `just test-integration` selects every class named Integration_*.
 */
class Integration_MultiPaxosClusterTest {

	@Test
	@Timeout(40)
	void threeReplicaClusterProcessesConcurrentRequestsAndConverges () throws Exception {
		try (Cluster cluster = new Cluster ()) {
			List<Future<Boolean>> replies = new ArrayList<> ();
			for (int i = 0; i < 12; i++) {
				for (int replica = 0; replica < 3; replica++)
					replies.add (cluster.addUserTo (replica, 10_000 + i, 100 + i, 1000 + i, 50));
			}
			assertTrue (awaitCondition ( () -> usersPresent (cluster.states[0], 100, 112)
					&& usersPresent (cluster.states[1], 100, 112),
					15, TimeUnit.SECONDS));
			// A replica that missed Phase 2 is still behind. The next ballot re-proposes the log.
			cluster.states[1].setCurrentBallot (1);
			cluster.reconnectPeers ();
			cluster.states[1].main_loop.wakeup ();

			for (Future<Boolean> reply : replies)
				assertTrue (reply.get (20, TimeUnit.SECONDS));
			for (int user_id = 100; user_id < 112; user_id++) {
				int expected_user = user_id;
				assertTrue (awaitCondition ( () -> cluster.states[0].trade_manager.balance (expected_user) != -1
						&& cluster.states[1].trade_manager.balance (expected_user) != -1
						&& cluster.states[2].trade_manager.balance (expected_user) != -1,
						5, TimeUnit.SECONDS));
				int balance = cluster.states[0].trade_manager.balance (user_id);
				assertEquals (balance, cluster.states[1].trade_manager.balance (user_id));
				assertEquals (balance, cluster.states[2].trade_manager.balance (user_id));
			}
		}
	}

	@Test
	@Timeout(30)
	void quorumProgressesWithOneFollowerFrozenAndFollowerCatchesUpAfterUnfreeze () throws Exception {
		try (Cluster cluster = new Cluster ()) {
			cluster.states[1].setDebugMode (2);
			List<Future<Boolean>> replies = cluster.broadcastAddUserAsync (20_001, 200, 900, 40);

			assertTrue (replies.get (0).get (10, TimeUnit.SECONDS));
			assertTrue (replies.get (2).get (10, TimeUnit.SECONDS));

			cluster.states[1].setDebugMode (3);
			assertTrue (replies.get (1).get (10, TimeUnit.SECONDS));
			assertTrue (awaitCondition ( () -> cluster.states[1].trade_manager.balance (200) == 900,
					5, TimeUnit.SECONDS));
			assertEquals (900, cluster.states[0].trade_manager.balance (200));
			assertEquals (900, cluster.states[1].trade_manager.balance (200));
			assertEquals (900, cluster.states[2].trade_manager.balance (200));
		}
	}

	@Test
	@Timeout(30)
	void newLeaderRunsNewBallotAndReproposesAlreadyChosenValue () throws Exception {
		try (Cluster cluster = new Cluster ()) {
			assertTrue (cluster.broadcastAddUser (30_001, 300, 777, 33));
			assertTrue (awaitCondition ( () -> allDecided (cluster.states, 0), 5, TimeUnit.SECONDS));
			int chosen = cluster.states[0].paxos_log.getEntry (0).getCommandId ();

			// Schedule A: ballot 1 is led by replica 1.
			cluster.states[1].setCurrentBallot (1);
			cluster.states[1].main_loop.wakeup ();

			assertTrue (awaitCondition ( () -> cluster.states[1].hasRunPhaseOne (1),
					5, TimeUnit.SECONDS));
			assertTrue (awaitCondition ( () -> cluster.states[0].paxos_log.getEntry (0).getWriteBallot () >= 1
					&& cluster.states[1].paxos_log.getEntry (0).getWriteBallot () >= 1,
					5, TimeUnit.SECONDS));

			for (DidaTradeServerState state : cluster.states) {
				PaxosInstance entry = state.paxos_log.getEntry (0);
				assertEquals (chosen, entry.getCommandId ());
				assertEquals (777, state.trade_manager.balance (300));
			}
		}
	}

	@Test
	@Timeout(30)
	void replicaThatMissedPhaseTwoCatchesUpWhenLeadershipChanges () throws Exception {
		try (Cluster cluster = new Cluster ()) {
			final int request_id = 50_001;
			cluster.dropReplica (2);

			List<Future<Boolean>> quorum = List.of (
					cluster.addUserTo (0, request_id, 500, 640, 12),
					cluster.addUserTo (1, request_id, 500, 640, 12));
			assertTrue (quorum.get (0).get (10, TimeUnit.SECONDS));
			assertTrue (quorum.get (1).get (10, TimeUnit.SECONDS));
			assertTrue (awaitCondition ( () -> decidedCommand (cluster.states[0], 0) == request_id
					&& decidedCommand (cluster.states[1], 0) == request_id,
					5, TimeUnit.SECONDS));
			assertEquals (-1, decidedCommand (cluster.states[2], 0));

			cluster.restoreReplica (2);
			Future<Boolean> late_copy = cluster.addUserTo (2, request_id, 500, 640, 12);
			// Ballot 1 is led by replica 1. Phase 1 recovers slot 0 and Phase 2 sends it again.
			cluster.states[1].setCurrentBallot (1);
			cluster.reconnectPeers ();
			cluster.states[1].main_loop.wakeup ();

			assertTrue (late_copy.get (15, TimeUnit.SECONDS));
			assertTrue (awaitCondition ( () -> {
				PaxosInstance entry = cluster.states[2].paxos_log.getEntry (0);
				return entry != null && entry.isDecided () && entry.getCommandId () == request_id
						&& entry.getAcceptBallot () >= 1
						&& cluster.states[2].trade_manager.balance (500) == 640;
			}, 10, TimeUnit.SECONDS));
			for (DidaTradeServerState state : cluster.states) {
				assertEquals (request_id, state.paxos_log.getEntry (0).getCommandId ());
				assertEquals (640, state.trade_manager.balance (500));
			}
		}
	}

	@Test
	@Timeout(60)
	void frozenReplicaBecomesLeaderAndKeepsQuorumValueInsteadOfItsOwn () throws Exception {
		try (Cluster cluster = new Cluster ()) {
			final int quorum_request = 40_001;
			final int private_request = 40_002;
			final int quorum_user = 400;
			final int private_user = 401;

			// Replica 1 will lead ballot 1. Freeze it before it can accept or learn.
			cluster.states[1].setDebugMode (2);

			List<Future<Boolean>> quorum_replies = List.of (
					cluster.addUserTo (0, quorum_request, quorum_user, 111, 10),
					cluster.addUserTo (2, quorum_request, quorum_user, 111, 10));
			assertTrue (quorum_replies.get (0).get (15, TimeUnit.SECONDS));
			assertTrue (quorum_replies.get (1).get (15, TimeUnit.SECONDS));
			assertTrue (awaitCondition (
					() -> decidedCommand (cluster.states[0], 0) == quorum_request
							&& decidedCommand (cluster.states[2], 0) == quorum_request,
					5, TimeUnit.SECONDS));

			// Freeze blocks the handler before the command is stored, so this divergent
			// request stays in flight and is not a local proposal yet.
			Future<Boolean> private_reply = cluster.addUserTo (
					1, private_request, private_user, 222, 20);

			// Install ballot 1 while the loop is still asleep. Unfreeze records the
			// private command and wakes the loop, which then has to recover slot 0
			// instead of proposing that private command there.
			cluster.states[1].setCurrentBallot (1);
			cluster.states[1].setDebugMode (3);

			// Replica 1 still needs the chosen payload before it can execute slot 0.
			// The other two need the private payload before they can execute it.
			Future<Boolean> quorum_on_leader = cluster.addUserTo (
					1, quorum_request, quorum_user, 111, 10);
			List<Future<Boolean>> private_on_quorum = List.of (
					cluster.addUserTo (0, private_request, private_user, 222, 20),
					cluster.addUserTo (2, private_request, private_user, 222, 20));
			assertTrue (private_reply.get (20, TimeUnit.SECONDS));
			assertTrue (quorum_on_leader.get (20, TimeUnit.SECONDS));
			assertTrue (private_on_quorum.get (0).get (20, TimeUnit.SECONDS));
			assertTrue (private_on_quorum.get (1).get (20, TimeUnit.SECONDS));

			assertTrue (awaitCondition ( () -> {
				for (DidaTradeServerState state : cluster.states) {
					if (decidedCommand (state, 0) != quorum_request)
						return false;
					if (slotOf (state, private_request) <= 0)
						return false;
					if (state.trade_manager.balance (quorum_user) != 111)
						return false;
					if (state.trade_manager.balance (private_user) != 222)
						return false;
				}
				return true;
			}, 10, TimeUnit.SECONDS));

			int private_slot = slotOf (cluster.states[0], private_request);
			for (DidaTradeServerState state : cluster.states) {
				assertEquals (quorum_request, state.paxos_log.getEntry (0).getCommandId ());
				assertEquals (private_slot, slotOf (state, private_request));
				assertEquals (111, state.trade_manager.balance (quorum_user));
				assertEquals (222, state.trade_manager.balance (private_user));
			}
		}
	}

	private static boolean usersPresent (DidaTradeServerState state, int first_user, int last_user) {
		for (int user_id = first_user; user_id < last_user; user_id++) {
			if (state.trade_manager.balance (user_id) == -1)
				return false;
		}
		return true;
	}

	private static int decidedCommand (DidaTradeServerState state, int slot) {
		PaxosInstance entry = state.paxos_log.getEntry (slot);
		if (entry == null || !entry.isDecided ())
			return -1;
		return entry.getCommandId ();
	}

	private static int slotOf (DidaTradeServerState state, int command_id) {
		for (int slot = 0; slot < 8; slot++) {
			if (decidedCommand (state, slot) == command_id)
				return slot;
		}
		return -1;
	}

	private static boolean allDecided (DidaTradeServerState[] states, int slot) {
		for (DidaTradeServerState state : states) {
			PaxosInstance entry = state.paxos_log.getEntry (slot);
			if (entry == null || !entry.isDecided ())
				return false;
		}
		return true;
	}

	private static boolean awaitCondition (
			Callable<Boolean> condition, long timeout, TimeUnit unit) throws Exception {
		long deadline = System.nanoTime () + unit.toNanos (timeout);
		while (System.nanoTime () < deadline) {
			if (condition.call ())
				return true;
			Thread.sleep (10);
		}
		return condition.call ();
	}

	private static final class Cluster implements AutoCloseable {
		private static final int REPLICAS = 3;
		private final int base_port;
		private final DidaTradeServerState[] states = new DidaTradeServerState[REPLICAS];
		private final Server[] servers = new Server[REPLICAS];
		private final ManagedChannel[] client_channels = new ManagedChannel[REPLICAS];
		private final DidaTradeMainServiceGrpc.DidaTradeMainServiceBlockingStub[] clients = new DidaTradeMainServiceGrpc.DidaTradeMainServiceBlockingStub[REPLICAS];
		private final ExecutorService workers = Executors.newCachedThreadPool ();

		Cluster() throws Exception {
			this.base_port = findFreeConsecutivePorts (REPLICAS);
			for (int id = 0; id < REPLICAS; id++) {
				this.states[id] = new DidaTradeServerState (this.base_port, id, 'A');
				this.servers[id] = ServerBuilder.forPort (this.base_port + id)
						.addService (new DidaTradeMainServiceImpl (this.states[id]))
						.addService (new DidaTradeMasterServiceImpl (this.states[id]))
						.addService (new DidaTradePaxosServiceImpl (this.states[id]))
						.build ()
						.start ();
			}
			for (int id = 0; id < REPLICAS; id++) {
				this.client_channels[id] = ManagedChannelBuilder
						.forAddress ("127.0.0.1", this.base_port + id)
						.usePlaintext ()
						.build ();
				this.clients[id] = DidaTradeMainServiceGrpc.newBlockingStub (this.client_channels[id]);
			}
			this.reconnectPeers ();
		}

		void dropReplica (int id) throws InterruptedException {
			this.servers[id].shutdownNow ();
			if (!this.servers[id].awaitTermination (2, TimeUnit.SECONDS))
				throw new IllegalStateException ("replica " + id + " did not stop");
		}

		void restoreReplica (int id) throws IOException {
			DidaTradeServerState state = this.states[id];
			this.servers[id] = ServerBuilder.forPort (this.base_port + id)
					.addService (new DidaTradeMainServiceImpl (state))
					.addService (new DidaTradeMasterServiceImpl (state))
					.addService (new DidaTradePaxosServiceImpl (state))
					.build ()
					.start ();
		}

		// A refused attempt leaves the channel in a long backoff. The next ballot can
		// reach the restored replica only after that delay is cleared.
		void reconnectPeers () {
			for (DidaTradeServerState state : this.states) {
				if (state.channels == null)
					continue;
				for (ManagedChannel channel : state.channels)
					channel.resetConnectBackoff ();
			}
		}

		boolean broadcastAddUser (
				int request_id, int user_id, int cash, int stock) throws Exception {
			List<Future<Boolean>> replies = this.broadcastAddUserAsync (
					request_id, user_id, cash, stock);
			boolean result = true;
			for (Future<Boolean> reply : replies)
				result &= reply.get (12, TimeUnit.SECONDS);
			return result;
		}

		Future<Boolean> addUserTo (
				int replica, int request_id, int user_id, int cash, int stock) {
			DidaTradeMain.AddUserRequest request = DidaTradeMain.AddUserRequest.newBuilder ()
					.setReqid (request_id)
					.setUserid (user_id)
					.setCash (cash)
					.setStock (stock)
					.build ();
			return this.workers.submit ( () -> this.clients[replica]
					.withDeadlineAfter (45, TimeUnit.SECONDS)
					.adduser (request)
					.getResult ());
		}

		List<Future<Boolean>> broadcastAddUserAsync (
				int request_id, int user_id, int cash, int stock) {
			DidaTradeMain.AddUserRequest request = DidaTradeMain.AddUserRequest.newBuilder ()
					.setReqid (request_id)
					.setUserid (user_id)
					.setCash (cash)
					.setStock (stock)
					.build ();
			List<Future<Boolean>> replies = new ArrayList<> ();
			for (int id = 0; id < REPLICAS; id++) {
				int replica = id;
				replies.add (this.workers.submit ( () -> this.clients[replica]
						.withDeadlineAfter (10, TimeUnit.SECONDS)
						.adduser (request)
						.getResult ()));
			}
			return replies;
		}

		@Override
		public void close () {
			boolean interrupted = false;

			for (DidaTradeServerState state : this.states) {
				if (state != null && state.getDebugState ().isBlocking ())
					state.setDebugMode (3);
			}

			this.workers.shutdownNow ();

			for (ManagedChannel channel : this.client_channels) {
				if (channel != null)
					channel.shutdownNow ();
			}

			for (Server server : this.servers) {
				if (server != null)
					server.shutdownNow ();
			}

			for (DidaTradeServerState state : this.states) {
				if (state == null)
					continue;

				if (state.main_loop_worker != null)
					state.main_loop_worker.interrupt ();

				if (state.channels != null) {
					for (ManagedChannel channel : state.channels) {
						if (channel != null)
							channel.shutdownNow ();
					}
				}
			}

			for (Server server : this.servers) {
				if (server == null)
					continue;

				try {
					server.awaitTermination (2, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					interrupted = true;
				}
			}

			if (interrupted)
				Thread.currentThread ().interrupt ();
		}
	}

	private static int findFreeConsecutivePorts (int count) throws IOException {
		for (int base = 18_000; base < 28_000; base += count + 1) {
			List<ServerSocket> sockets = new ArrayList<> ();
			try {
				for (int offset = 0; offset < count; offset++)
					sockets.add (new ServerSocket (base + offset));
				return base;
			} catch (IOException ignored) {
				// Try the next range.
			} finally {
				for (ServerSocket socket : sockets)
					socket.close ();
			}
		}
		throw new IOException ("Could not find three consecutive free ports");
	}
}
