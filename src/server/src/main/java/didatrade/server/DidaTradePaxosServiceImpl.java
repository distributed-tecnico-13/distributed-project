
package didatrade.server;

import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;
import java.util.Arrays;

import io.grpc.stub.StreamObserver;

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
		if (phase_two.accepted)
			PaxosLearnerNotifier.notifyAccepted (this.server_state, instance, ballot, value);

	}

	@Override
	public void openfast (DidaTradePaxos.OpenFastRequest request, StreamObserver<DidaTradePaxos.OpenFastReply> responseObserver) {
		int ballot = request.getBallot ();

		this.server_state.applyDebugState ();

		boolean accepted = this.server_state.openFastRound (ballot);
		DidaTradePaxos.OpenFastReply response = DidaTradePaxos.OpenFastReply.newBuilder ()
				.setServerid (this.server_state.my_id)
				.setBallot (ballot)
				.setAccepted (accepted)
				.setMaxballot (this.server_state.getCurrentBallot ())
				.build ();

		responseObserver.onNext (response);
		responseObserver.onCompleted ();

		if (accepted)
			FastPaxosPath.acceptPendingRequests (this.server_state);
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
			} else if (this.server_state.scheduler.fastpaxos (ballot)
					&& entry.cannotReachQuorum (ballot, this.server_state.scheduler.quorum (ballot),
							this.server_state.scheduler.acceptors (ballot).size ())) {
				if (this.server_state.advanceFromFailedFastBallot (ballot))
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
