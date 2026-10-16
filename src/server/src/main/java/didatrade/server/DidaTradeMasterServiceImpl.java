package didatrade.server;

import didatrade.DidaTradeMaster;
import didatrade.DidaTradeMasterServiceGrpc;

import io.grpc.stub.StreamObserver;

public class DidaTradeMasterServiceImpl extends DidaTradeMasterServiceGrpc.DidaTradeMasterServiceImplBase {
	DidaTradeServerState server_state;

	public DidaTradeMasterServiceImpl(DidaTradeServerState state) {
		this.server_state = state;
	}

	@Override
	public void newballot (DidaTradeMaster.NewBallotRequest request, StreamObserver<DidaTradeMaster.NewBallotReply> responseObserver) {
		System.out.println (request);

		int request_id = request.getReqid ();
		int new_ballot = request.getNewballot ();
		int completed_ballot = request.getCompletedballot ();;

		// for debug purposes
		System.out.println ("Current ballot = " + this.server_state.getCurrentBallot () + " new ballot = " + new_ballot + " completed ballot = " + completed_ballot);

		if (!this.server_state.scheduler.validBallot (new_ballot)
				|| this.server_state.scheduler.leader (new_ballot) != this.server_state.my_id) {
			responseObserver.onError (io.grpc.Status.INVALID_ARGUMENT.withDescription ("Send a valid ballot to its leader").asRuntimeException ());
			return;
		}
		if (!this.server_state.scheduler.dynamicMembership ())
			this.server_state.setCompletedBallot (completed_ballot);

		if (new_ballot > this.server_state.getCurrentBallot ()
				|| (this.server_state.scheduler.dynamicMembership () && new_ballot == this.server_state.getCurrentBallot ()
						&& this.server_state.getCompletedBallot () < new_ballot)) {
			this.server_state.setCurrentBallot (new_ballot);

			this.server_state.main_loop.wakeup ();

			completed_ballot = this.server_state.waitForCompletedBallot (new_ballot);
		} else {
			completed_ballot = this.server_state.getCompletedBallot ();
		}
		// A higher ballot may supersede this request while it waits for recovery.
		if (completed_ballot < new_ballot && this.server_state.getCurrentBallot () > new_ballot) {
			responseObserver.onError (io.grpc.Status.ABORTED.withDescription ("Ballot superseded before completion").asRuntimeException ());
			return;
		}

		DidaTradeMaster.NewBallotReply.Builder response_builder = DidaTradeMaster.NewBallotReply.newBuilder ();
		response_builder.setReqid (request_id);
		response_builder.setCompletedballot (completed_ballot);

		DidaTradeMaster.NewBallotReply response = response_builder.build ();
		responseObserver.onNext (response);
		responseObserver.onCompleted ();
	}

	@Override
	public void activate (DidaTradeMaster.ActivateRequest request, StreamObserver<DidaTradeMaster.ActivateReply> responseObserver) {
		System.out.println (request);

		int request_id = request.getReqid ();
		int completed_ballot = request.getCompletedballot ();;

		// for debug purposes
		System.out.println ("Current ballot = " + this.server_state.getCurrentBallot () + " activated ballot = " + completed_ballot);

		// Schedule C performs the handoff during ballot initiation; activation checks
		// readiness. Static schedules retain their original acknowledgement.

		DidaTradeMaster.ActivateReply.Builder response_builder = DidaTradeMaster.ActivateReply.newBuilder ();
		response_builder.setReqid (request_id);
		response_builder.setAck (!this.server_state.scheduler.dynamicMembership ()
				|| (this.server_state.canProposeFor (completed_ballot)
						&& completed_ballot <= this.server_state.getCompletedBallot ()));

		DidaTradeMaster.ActivateReply response = response_builder.build ();
		responseObserver.onNext (response);
		responseObserver.onCompleted ();
	}

	@Override
	public void setdebug (DidaTradeMaster.SetDebugRequest request, StreamObserver<DidaTradeMaster.SetDebugReply> responseObserver) {
		// for debug purposes
		System.out.println (request);

		boolean response_value = true;

		int request_id = request.getReqid ();
		this.server_state.setDebugMode (request.getMode ());

		// for debug purposes
		// System.out.println ("Setting debug mode to = " + this.server_state.getDebugMode ());

		DidaTradeMaster.SetDebugReply.Builder response_builder = DidaTradeMaster.SetDebugReply.newBuilder ();
		response_builder.setReqid (request_id);
		response_builder.setAck (response_value);

		DidaTradeMaster.SetDebugReply response = response_builder.build ();
		responseObserver.onNext (response);
		responseObserver.onCompleted ();
	}
}
