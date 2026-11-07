package didatrade.server;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import didatrade.DidaTradeMain;
import didatrade.DidaTradeMainServiceGrpc;
import didatrade.DidaTradeMaster;
import didatrade.DidaTradeMasterServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

/**
 * Standalone driver that hunts for the safety violation caused by PhaseOneBogusProcessor.
 *
 * Each attempt runs a fresh three-replica cluster in this JVM:
 *
 * 1. replica 1 is frozen, so it misses both paxos rounds and both client requests;
 * 2. command A (reqid 101) is decided by replicas 0 and 2 in log slot 0;
 * 3. command B (reqid 201) is decided by replicas 0 and 2 in log slot 1;
 * 4. replica 1 is given ballot 1, which makes it the leader;
 * 5. replica 1 is unfrozen.
 *
 * On unfreeze the queued learn messages race the main loop. When the main loop wins, phase one
 * answers itself first with valballot = -1, the bogus processor keeps the leader's own value, and
 * replica 1 writes command B into slot 0 that already holds command A. Both commands are adduser
 * calls for the same user id, so the replica that executes B first ends up with a different cash
 * value: that difference is the divergence this driver reports.
 *
 * Run it through repro.sh; it is not picked up by the normal surefire test run.
 */
public final class PhaseOneOverwriteRepro {

	private static final int N_REPLICAS = 3;
	private static final char SCHEDULE = 'A';
	private static final int DEAF_REPLICA = 1;
	private static final int NEW_BALLOT = 1;
	private static final int FREEZE = 2;
	private static final int UNFREEZE = 3;

	/** Hashtable enumeration hands out 201 before 101, so the leader proposes the second command. */
	private static final int FIRST_REQID = 101;
	private static final int SECOND_REQID = 201;
	private static final int PROBE_USER = 50;
	private static final int FIRST_CASH = 11;
	private static final int SECOND_CASH = 22;

	private static final int DEFAULT_ATTEMPTS = 100;
	private static final int DEFAULT_BASE_PORT = 19000;
	private static final int PORT_STRIDE = 10;
	private static final int OBSERVED_SLOTS = 2;
	private static final long RPC_TIMEOUT_MS = 10000;
	private static final long DECIDE_TIMEOUT_MS = 10000;
	private static final long SETTLE_TIMEOUT_MS = 5000;
	private static final long POLL_INTERVAL_MS = 5;

	private static PrintStream console = System.out;

	private PhaseOneOverwriteRepro() {
	}

	public static void main (String[] args) throws Exception {
		int attempts = Integer.getInteger ("repro.attempts", DEFAULT_ATTEMPTS);
		int base_port = Integer.getInteger ("repro.baseport", DEFAULT_BASE_PORT);
		String server_log = System.getProperty ("repro.serverlog", "logs/repro-servers.log");

		console = System.out;
		PrintStream captured = new PrintStream (new FileOutputStream (server_log), true, "UTF-8");
		System.setOut (captured);

		boolean reproduced = false;
		try {
			console.println ("[repro] running up to " + attempts + " attempts, replica chatter goes to " + server_log);

			for (int attempt = 1; (attempt <= attempts) && !reproduced; attempt++) {
				int port = base_port + attempt * PORT_STRIDE;
				Cluster cluster = new Cluster (port);
				try {
					cluster.start ();
					Outcome outcome = cluster.run ();
					reproduced = outcome.diverged ();
					console.println ("[repro] attempt " + attempt + "/" + attempts + " on ports " + port + "-" + (port + N_REPLICAS - 1) + ": "
							+ (reproduced ? "DIVERGED" : outcome.detail ()));
					if (reproduced)
						console.println (outcome.detail ());
				} catch (Exception e) {
					console.println ("[repro] attempt " + attempt + "/" + attempts + " failed to run: " + e);
				} finally {
					cluster.shutdown ();
				}
			}

			console.println ("[repro] RESULT: " + (reproduced ? "reproduced" : "not-reproduced"));
		} finally {
			System.setOut (console);
			captured.close ();
		}

		// every cluster leaves its main loop threads parked forever, so the JVM never ends on its own
		System.exit (reproduced ? 0 : 1);
	}

	private record Outcome(boolean diverged, String detail) {
	}

	private static final class Cluster {
		private final int base_port;
		private final DidaTradeServerState[] states;
		private final Server[] servers;
		private final ManagedChannel[] channels;
		private final DidaTradeMainServiceGrpc.DidaTradeMainServiceStub[] main_stubs;
		private final DidaTradeMasterServiceGrpc.DidaTradeMasterServiceStub[] master_stubs;

		Cluster(int port) {
			this.base_port = port;
			this.states = new DidaTradeServerState[N_REPLICAS];
			this.servers = new Server[N_REPLICAS];
			this.channels = new ManagedChannel[N_REPLICAS];
			this.main_stubs = new DidaTradeMainServiceGrpc.DidaTradeMainServiceStub[N_REPLICAS];
			this.master_stubs = new DidaTradeMasterServiceGrpc.DidaTradeMasterServiceStub[N_REPLICAS];
		}

		void start () throws Exception {
			for (int i = 0; i < N_REPLICAS; i++) {
				this.states[i] = new DidaTradeServerState (this.base_port, i, SCHEDULE);
				this.servers[i] = ServerBuilder.forPort (this.base_port + i).addService (new DidaTradeMainServiceImpl (this.states[i]))
						.addService (new DidaTradeMasterServiceImpl (this.states[i])).addService (new DidaTradePaxosServiceImpl (this.states[i]))
						.build ().start ();
				this.channels[i] = ManagedChannelBuilder.forTarget ("localhost:" + (this.base_port + i)).usePlaintext ().build ();
				this.main_stubs[i] = DidaTradeMainServiceGrpc.newStub (this.channels[i]);
				this.master_stubs[i] = DidaTradeMasterServiceGrpc.newStub (this.channels[i]);
			}
		}

		Outcome run () throws Exception {
			if (!setDebug (DEAF_REPLICA, FREEZE))
				return new Outcome (false, "replica " + DEAF_REPLICA + " did not acknowledge the freeze");

			if (!addUser (FIRST_REQID, FIRST_CASH))
				return new Outcome (false, "no reply to the first command");
			if (!awaitDecided (0, 0) || !awaitDecided (2, 0))
				return new Outcome (false, "the live replicas did not decide slot 0");

			if (!addUser (SECOND_REQID, SECOND_CASH))
				return new Outcome (false, "no reply to the second command");
			if (!awaitDecided (0, 1) || !awaitDecided (2, 1))
				return new Outcome (false, "the live replicas did not decide slot 1");

			newBallot (DEAF_REPLICA, NEW_BALLOT);
			if (!awaitBallot (DEAF_REPLICA, NEW_BALLOT))
				return new Outcome (false, "replica " + DEAF_REPLICA + " did not take ballot " + NEW_BALLOT);

			if (!setDebug (DEAF_REPLICA, UNFREEZE))
				return new Outcome (false, "replica " + DEAF_REPLICA + " did not acknowledge the unfreeze");

			if (!awaitExecuted ())
				return new Outcome (false, "the replicas did not settle in time");

			return compare ();
		}

		void shutdown () {
			for (int i = 0; i < N_REPLICAS; i++) {
				if (this.channels[i] != null)
					this.channels[i].shutdownNow ();
				if (this.states[i] != null)
					for (ManagedChannel peer : this.states[i].channels)
						peer.shutdownNow ();
				if (this.servers[i] != null)
					this.servers[i].shutdownNow ();
			}
		}

		private Outcome compare () {
			StringBuilder detail = new StringBuilder ();
			boolean diverged = false;
			int reference = this.states[0].trade_manager.balance (PROBE_USER);

			for (int i = 0; i < N_REPLICAS; i++) {
				int cash = this.states[i].trade_manager.balance (PROBE_USER);
				if (cash != reference)
					diverged = true;
				detail.append ("[repro]   replica ").append (i).append (": user ").append (PROBE_USER).append (" cash = ").append (cash)
						.append (", paxos log now holds ").append (slots (i)).append (System.lineSeparator ());
			}

			if (!diverged)
				return new Outcome (false, "converged on cash = " + reference);

			return new Outcome (true, detail.toString ());
		}

		private String slots (int replica) {
			StringBuilder out = new StringBuilder ();
			for (int instance = 0; instance < OBSERVED_SLOTS; instance++) {
				PaxosInstance entry = this.states[replica].paxos_log.getEntry (instance);
				out.append ("slot ").append (instance).append (" = ").append (entry == null ? "none" : Integer.toString (entry.command_id))
						.append (' ');
			}
			return out.toString ();
		}

		private boolean addUser (int reqid, int cash) throws InterruptedException {
			CountDownLatch done = new CountDownLatch (1);
			DidaTradeMain.AddUserRequest request = DidaTradeMain.AddUserRequest.newBuilder ().setReqid (reqid).setUserid (PROBE_USER).setCash (cash)
					.setStock (cash).build ();

			for (int i = 0; i < N_REPLICAS; i++)
				this.main_stubs[i].adduser (request, new FirstReply<> (done));

			return done.await (RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);
		}

		private boolean setDebug (int replica, int mode) throws InterruptedException {
			CountDownLatch done = new CountDownLatch (1);
			DidaTradeMaster.SetDebugRequest request = DidaTradeMaster.SetDebugRequest.newBuilder ().setReqid (mode).setMode (mode).build ();

			this.master_stubs[replica].setdebug (request, new FirstReply<> (done));

			return done.await (RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);
		}

		/** newballot blocks server side until the ballot completes, so the reply is never awaited. */
		private void newBallot (int replica, int ballot) {
			DidaTradeMaster.NewBallotRequest request = DidaTradeMaster.NewBallotRequest.newBuilder ().setReqid (ballot).setNewballot (ballot)
					.setCompletedballot (-1).build ();

			this.master_stubs[replica].newballot (request, new FirstReply<> (new CountDownLatch (1)));
		}

		private boolean awaitDecided (int replica, int instance) throws InterruptedException {
			long deadline = System.currentTimeMillis () + DECIDE_TIMEOUT_MS;
			while (System.currentTimeMillis () < deadline) {
				PaxosInstance entry = this.states[replica].paxos_log.getEntry (instance);
				if ((entry != null) && entry.decided)
					return true;
				Thread.sleep (POLL_INTERVAL_MS);
			}
			return false;
		}

		private boolean awaitBallot (int replica, int ballot) throws InterruptedException {
			long deadline = System.currentTimeMillis () + DECIDE_TIMEOUT_MS;
			while (System.currentTimeMillis () < deadline) {
				if (this.states[replica].getCurrentBallot () >= ballot)
					return true;
				Thread.sleep (POLL_INTERVAL_MS);
			}
			return false;
		}

		private boolean awaitExecuted () throws InterruptedException {
			long deadline = System.currentTimeMillis () + SETTLE_TIMEOUT_MS;
			while (System.currentTimeMillis () < deadline) {
				boolean all_executed = true;
				for (int i = 0; i < N_REPLICAS; i++)
					if (this.states[i].trade_manager.balance (PROBE_USER) < 0)
						all_executed = false;
				if (all_executed)
					return true;
				Thread.sleep (POLL_INTERVAL_MS);
			}
			return false;
		}
	}

	private static final class FirstReply<T> implements StreamObserver<T> {
		private final CountDownLatch done;

		FirstReply(CountDownLatch latch) {
			this.done = latch;
		}

		@Override
		public void onNext (T value) {
			this.done.countDown ();
		}

		@Override
		public void onError (Throwable t) {
		}

		@Override
		public void onCompleted () {
		}
	}
}
