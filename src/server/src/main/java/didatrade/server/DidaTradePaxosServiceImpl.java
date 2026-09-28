
package didatrade.server;

import java.util.*;

import didatrade.DidaTradeMain; // ignore-unused-import
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import didatrade.util.GenericResponseCollector;
import didatrade.util.CollectorStreamObserver;

import io.grpc.ManagedChannel; // ignore-unused-import
import io.grpc.ManagedChannelBuilder; // ignore-unused-import
import io.grpc.stub.StreamObserver;
import io.grpc.Context;

public class DidaTradePaxosServiceImpl extends DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase {
	DidaTradeServerState server_state;

	public DidaTradePaxosServiceImpl(DidaTradeServerState state) {
		this.server_state = state;
	}

	@Override
	public void phaseone (DidaTradePaxos.PhaseOneRequest request, StreamObserver<DidaTradePaxos.PhaseOneReply> responseObserver) {
		// System.out.println("Receive phase1 request: \n" + request);

		int ballot = request.getRequestballot ();

		this.server_state.applyDebugState ();

		DidaTradeServerState.PhaseOneState phase_one = null;
		PaxosInstance[] entries = null;

		synchronized (this.server_state) {
			entries = this.server_state.paxos_log.getAcceptedEntries ();
			phase_one = this.server_state.processPhaseOne (ballot);
		}

		DidaTradePaxos.PhaseOneReply.Builder response_builder = DidaTradePaxos.PhaseOneReply.newBuilder ();
		response_builder.setServerid (this.server_state.my_id);
		response_builder.setRequestballot (ballot);
		response_builder.setAccepted (phase_one.accepted);
		response_builder.setMaxballot (phase_one.maxballot);
		response_builder.addAllResponseLogObjects (
				Arrays.stream (entries).map (entry -> {
					PaxosInstance.AcceptedSnapshot snapshot = entry.getAcceptedSnapshot ();
					return DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
							.setInstance (entry.getInstanceNb ())
							.setValue (snapshot.value)
							.setValballot (snapshot.valballot)
							.build ();
				})
						.toList ());

		DidaTradePaxos.PhaseOneReply response = response_builder.build ();

		// System.out.println("Sending phase1 response: " + response);

		responseObserver.onNext (response);
		responseObserver.onCompleted ();
	}

	@Override
	public void phasetwo (DidaTradePaxos.PhaseTwoRequest request, StreamObserver<DidaTradePaxos.PhaseTwoReply> responseObserver) {
		// System.out.println ("Receive phase two request: \n" + request);

		int instance = request.getInstance ();
		int ballot = request.getRequestballot ();

		this.server_state.applyDebugState ();

		int value = request.getValue ();
		PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry (instance);
		DidaTradeServerState.PhaseTwoState phase_two = this.server_state.processPhaseTwo (entry, ballot, value);

		DidaTradePaxos.PhaseTwoReply.Builder response_builder = DidaTradePaxos.PhaseTwoReply.newBuilder ();
		response_builder.setAccepted (phase_two.accepted);
		response_builder.setInstance (instance);
		response_builder.setServerid (this.server_state.my_id);
		response_builder.setRequestballot (ballot);
		response_builder.setMaxballot (phase_two.maxballot);

		DidaTradePaxos.PhaseTwoReply response = response_builder.build ();

		// System.out.println("Sending phase2 response: " + response);

		responseObserver.onNext (response);
		responseObserver.onCompleted ();

		// Notify learners
		if (phase_two.accepted) {

			Context ctx = Context.current ().fork ();
			ctx.run ( () -> {
				List<Integer> learners = this.server_state.scheduler.learners (ballot);
				int n_targets = learners.size ();

				DidaTradePaxos.LearnRequest.Builder learn_request_builder = DidaTradePaxos.LearnRequest.newBuilder ();
				learn_request_builder.setInstance (instance);
				learn_request_builder.setValue (value);
				learn_request_builder.setBallot (ballot);
				learn_request_builder.setServerid (this.server_state.my_id);

				DidaTradePaxos.LearnRequest learn_request = learn_request_builder.build ();

				// System.out.println("Sending learn request: \n" + learn_request);

				System.out.println ("Paxos acceptor: going to notify learners for entry " + instance + " with timestamp " + ballot + " request = " + learn_request);
				ArrayList<DidaTradePaxos.LearnReply> learn_responses = new ArrayList<DidaTradePaxos.LearnReply> ();
				GenericResponseCollector<DidaTradePaxos.LearnReply> learn_collector = new GenericResponseCollector<DidaTradePaxos.LearnReply> (learn_responses, n_targets);;
				for (int i = 0; i < n_targets; i++) {
					CollectorStreamObserver<DidaTradePaxos.LearnReply> learn_observer = new CollectorStreamObserver<DidaTradePaxos.LearnReply> (learn_collector);
					this.server_state.async_stubs[learners.get (i)].learn (learn_request, learn_observer);
				}
				// System.out.println("Learn request completed for instance = " + instance);
			});
		}

	}

	@Override
	public void learn (DidaTradePaxos.LearnRequest request, StreamObserver<DidaTradePaxos.LearnReply> responseObserver) {
		// System.out.println("Receive learn request: \n" + request);

		int instance = request.getInstance ();
		int ballot = request.getBallot ();
		int value = request.getValue ();
		int server_id = request.getServerid ();

		this.server_state.applyDebugState ();

		if (this.server_state.scheduler.isacceptor (server_id, ballot)) {
			PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry (instance);
			this.server_state.setCurrentBallot (ballot);
			boolean became_decided = entry.recordLearn (
					ballot, value, server_id, this.server_state.scheduler.quorum (ballot));
			System.out.println ("Paxos learner for instance " + instance + " : number of distinct accepts " + entry.getAcceptCount (ballot));
			if (became_decided) {
				System.out.println ("Paxos learner: waking up the main loop");
				this.server_state.updateCompletedBallot (ballot);
				this.server_state.main_loop.wakeup ();
			}
		}

		DidaTradePaxos.LearnReply.Builder response_builder = DidaTradePaxos.LearnReply.newBuilder ();
		response_builder.setInstance (instance);
		response_builder.setBallot (ballot);

		DidaTradePaxos.LearnReply response = response_builder.build ();

		// System.out.println("Sending learn response");

		responseObserver.onNext (response);
		responseObserver.onCompleted ();
	}

}
