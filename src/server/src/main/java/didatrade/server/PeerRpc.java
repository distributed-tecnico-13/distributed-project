package didatrade.server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.FastRoundResponseProcessor;
import didatrade.util.GenericResponseCollector;
import didatrade.util.GenericResponseProcessor;
import didatrade.util.PhaseOneResponseProcessor;
import didatrade.util.PhaseTwoResponseProcessor;
import didatrade.util.StateTransferResponseProcessor;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

/**
 * All outgoing replica-to-replica RPCs. Owns the channels and stubs. The callers decide what to
 * send and how to judge the replies (the response processors); never call this under the state lock.
 */
final class PeerRpc {
	static final long RPC_TIMEOUT_SECONDS = 5;
	private static final long LEARN_TIMEOUT_SECONDS = 2;

	// Null when built from ready-made stubs (tests).
	final ManagedChannel[] channels;
	private final DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[] stubs;

	// One channel per participant, on port base_port + participant id.
	PeerRpc(int base_port, List<Integer> participants) {
		this.channels = new ManagedChannel[participants.size ()];
		this.stubs = new DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[participants.size ()];
		for (int i = 0; i < participants.size (); i++) {
			String target = "localhost:" + (base_port + participants.get (i));
			System.out.printf ("targets[%d] = %s%n", i, target);
			this.channels[i] = ManagedChannelBuilder.forTarget (target).usePlaintext ().build ();
			this.stubs[i] = DidaTradePaxosServiceGrpc.newStub (this.channels[i]);
		}
	}

	// Test-only construction from ready-made stubs, without channels of its own.
	PeerRpc(DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[] stubs) {
		this.channels = null;
		this.stubs = stubs;
	}

	// Each method below sends one RPC to every target and returns once the processor knows the
	// outcome, or every target has answered or failed.

	void phaseOne (List<Integer> acceptors, int ballot, PhaseOneResponseProcessor processor) {
		DidaTradePaxos.PhaseOneRequest request = DidaTradePaxos.PhaseOneRequest
				.newBuilder ().setRequestballot (ballot).build ();
		this.call (acceptors, request, processor, DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub::phaseone);
	}

	void phaseTwo (List<Integer> acceptors, int instance, int ballot, int value,
			PhaseTwoResponseProcessor processor) {
		DidaTradePaxos.PhaseTwoRequest request = DidaTradePaxos.PhaseTwoRequest
				.newBuilder ().setInstance (instance).setRequestballot (ballot)
				.setValue (value).build ();
		this.call (acceptors, request, processor, DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub::phasetwo);
	}

	void openFast (List<Integer> acceptors, int ballot, int first_fast_instance,
			FastRoundResponseProcessor processor) {
		DidaTradePaxos.OpenFastRequest request = DidaTradePaxos.OpenFastRequest.newBuilder ()
				.setFirstfastinstance (first_fast_instance)
				.setBallot (ballot)
				.build ();
		this.call (acceptors, request, processor, DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub::openfast);
	}

	void installState (List<Integer> learners, DidaTradePaxos.InstallStateRequest request,
			StateTransferResponseProcessor processor) {
		this.call (learners, request, processor, DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub::installstate);
	}

	// Fire-and-forget: tells every learner which value this acceptor accepted. Runs outside the
	// caller's gRPC context, so finishing the incoming call does not cancel these requests.
	void learn (List<Integer> learners, int server_id, int instance, int ballot, int value,
			boolean classic) {
		Context.current ().fork ().run ( () -> {
			DidaTradePaxos.LearnRequest request = DidaTradePaxos.LearnRequest.newBuilder ()
					.setInstance (instance)
					.setValue (value)
					.setBallot (ballot)
					.setServerid (server_id)
					.setClassic (classic)
					.build ();

			System.out.println ("Paxos acceptor: going to notify learners for entry "
					+ instance + " with timestamp " + ballot + " request = " + request);

			for (int learner : learners) {
				this.stubs[learner]
						.withDeadlineAfter (LEARN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
						.learn (request, new StreamObserver<DidaTradePaxos.LearnReply> () {
							@Override
							public void onNext (DidaTradePaxos.LearnReply reply) {
							}

							@Override
							public void onError (Throwable throwable) {
							}

							@Override
							public void onCompleted () {
							}
						});
			}
		});
	}

	// Sends the request to every target and waits until the processor is done.
	private <Req, Rep> void call (List<Integer> targets, Req request, GenericResponseProcessor<Rep> processor,
			Rpc<Req, Rep> rpc) {
		GenericResponseCollector<Rep> collector = new GenericResponseCollector<> (
				new ArrayList<> (), targets.size (), processor);
		for (int target : targets) {
			rpc.send (this.stubs[target].withDeadlineAfter (RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS),
					request, new CollectorStreamObserver<> (collector));
		}
		collector.waitUntilDone ();
	}

	@FunctionalInterface
	private interface Rpc<Req, Rep> {
		void send (DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub stub, Req request,
				StreamObserver<Rep> observer);
	}
}
