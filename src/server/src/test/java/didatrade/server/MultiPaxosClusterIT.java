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
 * Real-TCP integration tests. The class ends in IT so the normal Surefire test scan does
 * not run it automatically. Run it explicitly with tests/run-integration.sh.
 */
class MultiPaxosClusterIT {

	@Test
	@Timeout (30)
	void threeReplicaClusterProcessesConcurrentRequestsAndConverges () throws Exception {
		try (Cluster cluster = new Cluster ()) {
			List<Future<Boolean>> requests = new ArrayList<> ();
			for (int i = 0; i < 12; i++) {
				int request_id = 10_000 + i;
				int user_id = 100 + i;
				int cash = 1000 + i;
				requests.add (cluster.workers.submit ( () ->
						cluster.broadcastAddUser (request_id, user_id, cash, 50)));
			}
			for (Future<Boolean> request : requests)
				assertTrue (request.get (15, TimeUnit.SECONDS));

			for (int user_id = 100; user_id < 112; user_id++) {
				int expected_user = user_id;
				assertTrue (awaitCondition ( () ->
						cluster.states[0].trade_manager.balance (expected_user) != -1
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
	@Timeout (30)
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
	@Timeout (30)
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
			assertTrue (awaitCondition ( () ->
					cluster.states[0].paxos_log.getEntry (0).getWriteBallot () >= 1
							&& cluster.states[1].paxos_log.getEntry (0).getWriteBallot () >= 1,
					5, TimeUnit.SECONDS));

			for (DidaTradeServerState state : cluster.states) {
				PaxosInstance entry = state.paxos_log.getEntry (0);
				assertEquals (chosen, entry.getCommandId ());
				assertEquals (777, state.trade_manager.balance (300));
			}
		}
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
		private final DidaTradeMainServiceGrpc.DidaTradeMainServiceBlockingStub[] clients =
				new DidaTradeMainServiceGrpc.DidaTradeMainServiceBlockingStub[REPLICAS];
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

