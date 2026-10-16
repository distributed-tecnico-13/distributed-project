
package didatrade.server.service;

import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;
import java.util.Arrays;
import java.util.List;

import io.grpc.stub.StreamObserver;
import didatrade.server.DidaTradeServerState;
import didatrade.server.FastPaxosAcceptor;
import didatrade.server.PaxosInstance;
import didatrade.server.PaxosLearnerNotifier;
import didatrade.server.StateTransfer;
import didatrade.server.request.RequestRecord;

/** Acceptor and learner side of the replica-to-replica Paxos RPCs. */
public class DidaTradePaxosServiceImpl extends DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase {
	DidaTradeServerState server_state;

	public DidaTradePaxosServiceImpl(DidaTradeServerState state) {
		this.server_state = state;
	}

	// Acceptor Phase 1: promises the ballot and reports every accepted slot.
	@Override
	public void phaseone (DidaTradePaxos.PhaseOneRequest request, StreamObserver<DidaTradePaxos.PhaseOneReply> responseObserver) {
		// System.out.println("Receive phase1 request: \n" + request);

		int ballot = request.getRequestballot ();

		this.server_state.applyDebugState ();

		DidaTradeServerState.PhaseOneState phase_one;
		boolean membership_transferred;
		List<DidaTradePaxos.PhaseOneResponseLogObject> accepted_snapshots;
		synchronized (this.server_state) {
			// Snapshot *values*, not mutable instance references, atomically with the promise.
			accepted_snapshots = Arrays.stream (this.server_state.getPaxosLog ().getAcceptedEntries ())
					.map (entry -> {
						PaxosInstance.AcceptedSnapshot snapshot = entry.getAcceptedSnapshot ();
						return DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
								.setInstance (entry.getInstanceNb ())
								.setValue (snapshot.value)
								.setValballot (snapshot.valballot).build ();
					}).toList ();
			phase_one = this.server_state.processPhaseOne (ballot);
			membership_transferred = this.server_state.membershipTransferred ();
		}

		DidaTradePaxos.PhaseOneReply.Builder response_builder = DidaTradePaxos.PhaseOneReply.newBuilder ();
		response_builder.setServerid (this.server_state.getMyId ());
		response_builder.setRequestballot (ballot);
		response_builder.setAccepted (phase_one.accepted);
		response_builder.setMaxballot (phase_one.maxballot);
		response_builder.addAllResponseLogObjects (accepted_snapshots);
		response_builder.setMembershiptransferred (membership_transferred);

		DidaTradePaxos.PhaseOneReply response = response_builder.build ();

		// System.out.println("Sending phase1 response: " + response);

		responseObserver.onNext (response);
		responseObserver.onCompleted ();
	}

	// Acceptor Phase 2: accepts (ballot, value) for one slot under the state lock, then replies
	// and, if accepted, notifies the learners outside the lock.
	@Override
	public void phasetwo (DidaTradePaxos.PhaseTwoRequest request, StreamObserver<DidaTradePaxos.PhaseTwoReply> responseObserver) {
		// System.out.println ("Receive phase two request: \n" + request);

		int instance = request.getInstance ();
		int ballot = request.getRequestballot ();

		this.server_state.applyDebugState ();

		int value = request.getValue ();
		PaxosInstance entry = this.server_state.getPaxosLog ().testAndSetEntry (instance);
		DidaTradeServerState.PhaseTwoState phase_two = this.server_state.processPhaseTwo (entry, ballot, value);

		DidaTradePaxos.PhaseTwoReply.Builder response_builder = DidaTradePaxos.PhaseTwoReply.newBuilder ();
		response_builder.setAccepted (phase_two.accepted);
		response_builder.setInstance (instance);
		response_builder.setServerid (this.server_state.getMyId ());
		response_builder.setRequestballot (ballot);
		response_builder.setMaxballot (phase_two.maxballot);

		DidaTradePaxos.PhaseTwoReply response = response_builder.build ();

		// System.out.println("Sending phase2 response: " + response);

		responseObserver.onNext (response);
		responseObserver.onCompleted ();

		// Notify learners
		if (phase_two.accepted)
			PaxosLearnerNotifier.notifyAccepted (this.server_state, instance, ballot, value, true);

	}

	// Dynamic membership: installs the recovered prefix pushed by the new leader (see
	// StateTransfer).
	@Override
	public void installstate (DidaTradePaxos.InstallStateRequest request,
			StreamObserver<DidaTradePaxos.InstallStateReply> observer) {
		this.server_state.applyDebugState ();
		boolean accepted = StateTransfer.install (this.server_state, request);
		observer.onNext (DidaTradePaxos.InstallStateReply.newBuilder ()
				.setServerid (this.server_state.getMyId ()).setAccepted (accepted)
				.setMaxballot (this.server_state.getCurrentBallot ()).build ());
		observer.onCompleted ();
		if (accepted)
			this.server_state.wakeMainLoop ();
	}

	// Opens the Fast round and immediately votes for requests that are already pending.
	@Override
	public void openfast (DidaTradePaxos.OpenFastRequest request, StreamObserver<DidaTradePaxos.OpenFastReply> responseObserver) {
		int ballot = request.getBallot ();

		this.server_state.applyDebugState ();

		boolean accepted = this.server_state.openFastRound (ballot, request.getFirstfastinstance ());
		DidaTradePaxos.OpenFastReply response = DidaTradePaxos.OpenFastReply.newBuilder ()
				.setServerid (this.server_state.getMyId ())
				.setBallot (ballot)
				.setAccepted (accepted)
				.setMaxballot (this.server_state.getCurrentBallot ())
				.build ();

		responseObserver.onNext (response);
		responseObserver.onCompleted ();

		if (accepted)
			FastPaxosAcceptor.acceptPendingRequests (this.server_state);
	}

	// Learner: records one acceptor's vote. A decision completes the ballot; a Fast collision (no
	// value can reach the Fast quorum) moves on to the next ballot.
	@Override
	public void learn (DidaTradePaxos.LearnRequest request, StreamObserver<DidaTradePaxos.LearnReply> responseObserver) {
		// System.out.println("Receive learn request: \n" + request);

		int instance = request.getInstance ();
		int ballot = request.getBallot ();
		int value = request.getValue ();
		int server_id = request.getServerid ();

		this.server_state.applyDebugState ();

		if (this.server_state.getScheduler ().isacceptor (server_id, ballot)) {
			PaxosInstance entry = this.server_state.getPaxosLog ().testAndSetEntry (instance);
			this.server_state.setCurrentBallot (ballot);
			// Classic Phase 2 and direct Fast accepts have different quorum sizes.
			int decision_quorum = request.getClassic ()
					? this.server_state.getScheduler ().classicQuorum (ballot)
					: this.server_state.getScheduler ().fastQuorum (ballot);
			boolean became_decided = entry.recordLearn (
					ballot, value, server_id, decision_quorum);
			System.out.println ("Paxos learner for instance " + instance + " : number of distinct accepts " + entry.getAcceptCount (ballot));
			if (became_decided) {
				System.out.println ("Paxos learner: waking up the main loop");
				this.server_state.updateCompletedBallot ();
				this.server_state.getMainLoop ().wakeup ();
			} else if (!request.getClassic ()
					&& entry.fastRoundCollided (ballot, decision_quorum,
							this.server_state.getScheduler ().acceptors (ballot).size ())
					&& this.server_state.advanceFromCollidedFastBallot (ballot)) {
				// Learners may disagree, e.g. on a 4-1 split another learner may see the
				// four matching votes first and decide. Advancing stays safe either way.
				System.out.println ("Paxos learner: instance " + instance + " collided in Fast ballot "
						+ ballot + ", no value reached the Fast quorum; advancing to ballot " + (ballot + 1));
				this.server_state.getMainLoop ().wakeup ();
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
