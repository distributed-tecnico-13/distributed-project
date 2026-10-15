package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;
import didatrade.configs.ConfigurationScheduler;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import didatrade.server.request.CommandIdHelper;
import didatrade.server.request.RequestHistory;

class MultiPaxosPrepareTest {

	private final List<Server> servers = new ArrayList<> ();
	private final List<ManagedChannel> channels = new ArrayList<> ();

	@AfterEach
	void cleanup () throws Exception {
		for (ManagedChannel channel : this.channels)
			channel.shutdownNow ();
		for (Server server : this.servers)
			server.shutdownNow ();
		for (Server server : this.servers)
			server.awaitTermination (2, TimeUnit.SECONDS);
	}

	@Test
	void successfulPrepareRunsPhaseOneOnlyOncePerBallot () throws Exception {
		AtomicInteger calls = new AtomicInteger ();
		CountDownLatch first_round = new CountDownLatch (3);
		DidaTradeServerState state = this.stateWithAcceptors (
				serverId -> acceptingService (serverId, calls, first_round));
		state.my_id = 0; // leader for ballots 0, 3, 6, ... in Schedule A
		PaxosProposer proposer = new PaxosProposer (state);

		assertTrue (proposer.prepare (0));
		assertTrue (first_round.await (2, TimeUnit.SECONDS));
		assertEquals (3, calls.get ());
		assertTrue (state.hasRunPhaseOne (0));

		// Same ballot: Multi-Paxos must reuse the successful prepare.
		assertTrue (proposer.prepare (0));
		Thread.sleep (100);
		assertEquals (3, calls.get ());

		// A new ballot led by the same replica requires a new Phase 1.
		state.setCurrentBallot (3);
		assertTrue (proposer.prepare (3));
		assertTrue (awaitValue (calls, 6, 2, TimeUnit.SECONDS));
		assertEquals (6, calls.get ());
		assertTrue (state.hasRunPhaseOne (3));
	}

	@Test
	void prepareStillSucceedsWhenOneAcceptorRpcFails () throws Exception {
		DidaTradeServerState state = this.stateWithAcceptors (serverId -> serverId == 2 ? failingService () : acceptingService (serverId, new AtomicInteger (), new CountDownLatch (0)));
		state.my_id = 0;
		PaxosProposer proposer = new PaxosProposer (state);

		assertTrue (proposer.prepare (0));
		assertTrue (state.hasRunPhaseOne (0));
	}

	@Test
	void prepareFailsWhenOnlyOneAcceptorRepliesSuccessfully () throws Exception {
		DidaTradeServerState state = this.stateWithAcceptors (serverId -> serverId == 0 ? acceptingService (serverId, new AtomicInteger (), new CountDownLatch (0)) : failingService ());
		state.my_id = 0;
		PaxosProposer proposer = new PaxosProposer (state);

		assertFalse (proposer.prepare (0));
		assertFalse (state.hasRunPhaseOne (0));
	}

	@Test
	void rejectedPrepareDoesNotMarkBallotPreparedAndObservesHigherBallot () throws Exception {
		DidaTradeServerState state = this.stateWithAcceptors (serverId -> rejectingService (serverId, 8));
		state.my_id = 0;
		PaxosProposer proposer = new PaxosProposer (state);

		assertFalse (proposer.prepare (0));
		assertFalse (state.hasRunPhaseOne (0));
		assertEquals (8, state.getCurrentBallot ());
		assertFalse (state.canProposeFor (0));
	}

	@Test
	void prepareAdoptsHighestAcceptedValuePerInstanceAndFillsHolesWithNoops () throws Exception {
		CountDownLatch first_two = new CountDownLatch (2);
		DidaTradeServerState state = this.stateWithAcceptors (serverId -> {
			if (serverId == 0)
				return logService (serverId, first_two, 0,
						entry (0, 2, 201), entry (2, 1, 401));
			if (serverId == 1)
				return logService (serverId, first_two, 0,
						entry (0, 5, 301), entry (2, 3, 401));
			// Delay the third response so the deterministic quorum is {0,1}.
			return logService (serverId, new CountDownLatch (0), 300,
					entry (0, 99, 999));
		});
		state.my_id = 0;
		PaxosProposer proposer = new PaxosProposer (state);

		assertTrue (proposer.prepare (0));
		assertTrue (first_two.await (2, TimeUnit.SECONDS));

		PaxosInstance slot0 = state.paxos_log.getEntry (0);
		PaxosInstance slot1 = state.paxos_log.getEntry (1);
		PaxosInstance slot2 = state.paxos_log.getEntry (2);
		assertNotNull (slot0);
		assertNotNull (slot1);
		assertNotNull (slot2);
		assertEquals (301, slot0.getProposedRequestId ());
		assertEquals (CommandIdHelper.NOOP_VALUE, slot1.getProposedRequestId ());
		assertEquals (401, slot2.getProposedRequestId ());
	}

	private DidaTradeServerState stateWithAcceptors (ServiceFactory factory) throws Exception {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.setRanPhaseOne (-1);
		state.scheduler = new ConfigurationScheduler ('A');
		state.paxos_log = new PaxosLog ();
		state.req_history = new RequestHistory ();
		state.async_stubs = new DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[3];
		for (int server_id = 0; server_id < 3; server_id++) {
			Server server = ServerBuilder.forPort (0).addService (factory.create (server_id)).build ().start ();
			this.servers.add (server);
			ManagedChannel channel = ManagedChannelBuilder
					.forAddress ("127.0.0.1", server.getPort ())
					.usePlaintext ()
					.build ();
			this.channels.add (channel);
			state.async_stubs[server_id] = DidaTradePaxosServiceGrpc.newStub (channel);
		}
		return state;
	}

	private static DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase acceptingService (
			int server_id, AtomicInteger calls, CountDownLatch seen) {
		return new DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase () {
			@Override
			public void phaseone (DidaTradePaxos.PhaseOneRequest request,
					StreamObserver<DidaTradePaxos.PhaseOneReply> observer) {
				calls.incrementAndGet ();
				seen.countDown ();
				observer.onNext (DidaTradePaxos.PhaseOneReply.newBuilder ()
						.setServerid (server_id)
						.setRequestballot (request.getRequestballot ())
						.setAccepted (true)
						.setMaxballot (request.getRequestballot ())
						.build ());
				observer.onCompleted ();
			}
		};
	}

	private static DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase failingService () {
		return new DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase () {
			@Override
			public void phaseone (DidaTradePaxos.PhaseOneRequest request,
					StreamObserver<DidaTradePaxos.PhaseOneReply> observer) {
				observer.onError (Status.UNAVAILABLE.asRuntimeException ());
			}
		};
	}

	private static DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase rejectingService (
			int server_id, int max_ballot) {
		return new DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase () {
			@Override
			public void phaseone (DidaTradePaxos.PhaseOneRequest request,
					StreamObserver<DidaTradePaxos.PhaseOneReply> observer) {
				observer.onNext (DidaTradePaxos.PhaseOneReply.newBuilder ()
						.setServerid (server_id)
						.setRequestballot (request.getRequestballot ())
						.setAccepted (false)
						.setMaxballot (max_ballot)
						.build ());
				observer.onCompleted ();
			}
		};
	}

	private static DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase logService (
			int server_id, CountDownLatch seen, long delay_millis,
			DidaTradePaxos.PhaseOneResponseLogObject... entries) {
		return new DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase () {
			@Override
			public void phaseone (DidaTradePaxos.PhaseOneRequest request,
					StreamObserver<DidaTradePaxos.PhaseOneReply> observer) {
				if (delay_millis > 0) {
					try {
						Thread.sleep (delay_millis);
					} catch (InterruptedException e) {
						Thread.currentThread ().interrupt ();
					}
				}
				seen.countDown ();
				DidaTradePaxos.PhaseOneReply.Builder reply = DidaTradePaxos.PhaseOneReply.newBuilder ()
						.setServerid (server_id)
						.setRequestballot (request.getRequestballot ())
						.setAccepted (true)
						.setMaxballot (request.getRequestballot ());
				for (DidaTradePaxos.PhaseOneResponseLogObject entry : entries)
					reply.addResponseLogObjects (entry);
				observer.onNext (reply.build ());
				observer.onCompleted ();
			}
		};
	}

	private static DidaTradePaxos.PhaseOneResponseLogObject entry (
			int instance, int ballot, int value) {
		return DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
				.setInstance (instance)
				.setValballot (ballot)
				.setValue (value)
				.build ();
	}

	private static boolean awaitValue (
			AtomicInteger value, int expected, long timeout, TimeUnit unit) throws InterruptedException {
		long deadline = System.nanoTime () + unit.toNanos (timeout);
		while (value.get () < expected && System.nanoTime () < deadline)
			Thread.sleep (5);
		return value.get () >= expected;
	}

	@FunctionalInterface
	private interface ServiceFactory {
		DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase create (int server_id);
	}
}
