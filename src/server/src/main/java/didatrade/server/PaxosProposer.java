package didatrade.server;

import static didatrade.configs.ScheduleC.MEMBERSHIP_CHANGE_BALLOT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import didatrade.DidaTradePaxos;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.FastRoundResponseProcessor;
import didatrade.util.GenericResponseCollector;
import didatrade.util.PhaseOneResponseProcessor;
import didatrade.util.PhaseTwoResponseProcessor;
import didatrade.util.StateTransferResponseProcessor;

/** Phase 1 once per ballot, classic recovery, and the transition into a Fast round. */
final class PaxosProposer {
	static final long RETRY_DELAY_MILLIS = 200;
	static final long RPC_TIMEOUT_SECONDS = 5;

	private final DidaTradeServerState server_state;
	private int prepared_ballot = -1;
	private int recovery_highest_instance = -1;
	private int opened_fast_ballot = -1;

	PaxosProposer(DidaTradeServerState state) {
		this.server_state = state;
	}

	boolean prepare (int ballot) {
		if (!this.server_state.canProposeFor (ballot))
			return false;
		if (this.server_state.hasRunPhaseOne (ballot))
			return true;

		List<Integer> acceptors = this.server_state.scheduler.acceptors (ballot);
		PhaseOneResult phase_one = this.runPhaseOne (
				acceptors, acceptors.size (), ballot, ballot);
		if (!this.server_state.canProposeFor (ballot))
			return false;
		if (this.server_state.scheduler.dynamicMembership () && ballot >= MEMBERSHIP_CHANGE_BALLOT && !phase_one.membership_transferred) {
			// The new group alone may not resolve old Fast votes. Consult the old
			// group even when this first attempt could not select a safe value.
			acceptors = this.server_state.scheduler.acceptorsinrange (0, ballot);
			phase_one = this.runPhaseOne (acceptors, acceptors.size (), ballot, 0);
		}
		if (phase_one.aborted || !this.server_state.canProposeFor (ballot))
			return false;

		for (int instance : phase_one.instances ())
			this.server_state.paxos_log.adopt (instance, phase_one.commandFor (instance));
		this.fillNoops (phase_one);

		this.prepared_ballot = ballot;
		this.recovery_highest_instance = phase_one.highestInstance ();
		this.opened_fast_ballot = -1;
		this.server_state.setRanPhaseOne (ballot);
		return true;
	}

	private void fillNoops (PhaseOneResult phase_one) {
		int[] instances = phase_one.instances;
		if (instances.length == 0)
			return;
		int max_instance = Arrays.stream (instances).max ().getAsInt ();
		Set<Integer> known = Arrays.stream (instances).boxed ().collect (Collectors.toSet ());
		for (int instance = 0; instance <= max_instance; instance++) {
			if (!known.contains (instance))
				this.server_state.paxos_log.fillNoop (instance);
		}
	}

	int recoveryLimit (int ballot) {
		if (this.prepared_ballot != ballot || this.recovery_highest_instance < 0)
			return 0;
		return this.recovery_highest_instance + 1;
	}

	boolean fastRecoveryComplete (int ballot) {
		if (this.prepared_ballot != ballot)
			return false;
		for (int instance = 0; instance <= this.recovery_highest_instance; instance++) {
			PaxosInstance entry = this.server_state.paxos_log.getEntry (instance);
			if (entry == null || !entry.isDecided () || entry.getAcceptBallot () < ballot)
				return false;
		}
		return true;
	}

	// Distinct from isFastRoundOpen(), which is local acceptor state.
	boolean hasFastOpeningQuorum (int ballot) {
		return this.opened_fast_ballot == ballot
				&& this.server_state.canCoordinateFastRound (ballot);
	}

	boolean openFastRound (int ballot) {
		if (!this.server_state.canCoordinateFastRound (ballot)
				|| !this.fastRecoveryComplete (ballot))
			return false;
		if (this.opened_fast_ballot == ballot)
			return false;

		List<Integer> acceptors = this.server_state.scheduler.acceptors (ballot);
		DidaTradePaxos.OpenFastRequest request = DidaTradePaxos.OpenFastRequest.newBuilder ()
				.setFirstfastinstance (this.recoveryLimit (ballot))
				.setBallot (ballot)
				.build ();
		FastRoundResponseProcessor processor = new FastRoundResponseProcessor (
				this.server_state.scheduler.fastQuorum (ballot), acceptors);
		ArrayList<DidaTradePaxos.OpenFastReply> responses = new ArrayList<> ();
		GenericResponseCollector<DidaTradePaxos.OpenFastReply> collector = new GenericResponseCollector<> (
				responses, acceptors.size (), processor);

		for (int acceptor : acceptors) {
			CollectorStreamObserver<DidaTradePaxos.OpenFastReply> observer = new CollectorStreamObserver<> (
					collector);
			this.server_state.async_stubs[acceptor]
					.withDeadlineAfter (RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
					.openfast (request, observer);
		}
		collector.waitUntilDone ();

		if (processor.getAccepted ()) {
			this.opened_fast_ballot = ballot;
			return true;
		}
		if (processor.getMaxballot () > this.server_state.getCurrentBallot ()) {
			this.server_state.setCurrentBallot (processor.getMaxballot ());
			this.server_state.wakeMainLoop ();
		}
		return false;
	}

	boolean transferComplete (int ballot) {
		return this.server_state.getCompletedBallot () == ballot;
	}

	boolean finishTransfer (int ballot) {
		if (!this.server_state.canProposeFor (ballot) || !this.fastRecoveryComplete (ballot))
			return false;
		if (this.transferComplete (ballot))
			return true;
		// Transfer only IDs: the project guarantees eventual delivery of command
		// parameters from clients to every replica, even if debug modes delay them.
		DidaTradePaxos.InstallStateRequest.Builder builder = DidaTradePaxos.InstallStateRequest.newBuilder ()
				.setBallot (ballot);
		for (int slot = 0; slot < this.recoveryLimit (ballot); slot++) {
			int value = this.server_state.paxos_log.getEntry (slot).getCommandId ();
			builder.addEntries (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
					.setInstance (slot).setValue (value).setValballot (ballot));
		}
		DidaTradePaxos.InstallStateRequest request = builder.build ();
		List<Integer> learners = this.server_state.scheduler.learners (ballot);
		StateTransferResponseProcessor processor = new StateTransferResponseProcessor (
				ballot, this.server_state.scheduler.classicQuorum (ballot), this.server_state.scheduler.acceptors (ballot));
		GenericResponseCollector<DidaTradePaxos.InstallStateReply> collector = new GenericResponseCollector<> (
				new ArrayList<> (), learners.size (), processor);
		for (int learner : learners)
			this.server_state.async_stubs[learner].withDeadlineAfter (RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
					.installstate (request, new CollectorStreamObserver<> (collector));
		collector.waitUntilDone ();
		if (processor.getMaxballot () > ballot)
			this.server_state.setCurrentBallot (processor.getMaxballot ());
		if (!processor.getAccepted () || !this.server_state.canProposeFor (ballot))
			return false;
		this.server_state.setCompletedBallot (ballot);
		return true;
	}

	void propose (PaxosInstance entry, int ballot) {
		try {
			if (!this.server_state.canProposeFor (ballot))
				return;
			int instance = entry.getInstanceNb ();
			int phase_two_value = entry.phaseTwoValueFor (ballot, entry.getValueToPropose ());
			this.server_state.paxos_log.replaceProposedRequestId (instance, phase_two_value);
			List<Integer> acceptors = this.server_state.scheduler.acceptors (ballot);
			if (this.runPhaseTwo (instance, ballot, phase_two_value, acceptors,
					acceptors.size (), this.server_state.scheduler.classicQuorum (ballot)))
				return;

			this.finishSuccessfulPhaseTwo (entry, ballot, phase_two_value);
		} finally {
			entry.releaseClaim (ballot, TimeUnit.MILLISECONDS.toNanos (RETRY_DELAY_MILLIS));
		}
	}

	// Phase 2 reached a classic quorum. Even an already-decided slot needs its
	// higher-ballot completion recorded; otherwise newballot may wait forever.
	void finishSuccessfulPhaseTwo (PaxosInstance entry, int ballot, int value) {
		entry.decide (ballot, value);
		this.server_state.updateCompletedBallot (ballot);
	}

	private PhaseOneResult runPhaseOne (
			List<Integer> acceptors, int n_acceptors, int ballot, int low_ballot) {
		DidaTradePaxos.PhaseOneRequest request = DidaTradePaxos.PhaseOneRequest
				.newBuilder ().setRequestballot (ballot).build ();
		PhaseOneResponseProcessor processor = new PhaseOneResponseProcessor (
				this.server_state.scheduler, ballot, low_ballot);
		ArrayList<DidaTradePaxos.PhaseOneReply> responses = new ArrayList<> ();
		GenericResponseCollector<DidaTradePaxos.PhaseOneReply> collector = new GenericResponseCollector<> (
				responses, n_acceptors, processor);
		for (int acceptor : acceptors) {
			CollectorStreamObserver<DidaTradePaxos.PhaseOneReply> observer = new CollectorStreamObserver<> (
					collector);
			this.server_state.async_stubs[acceptor]
					.withDeadlineAfter (RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
					.phaseone (request, observer);
		}
		collector.waitUntilDone ();

		PhaseOneResult result = new PhaseOneResult (processor);
		if (result.aborted && result.maxballot > this.server_state.getCurrentBallot ()) {
			this.server_state.setCurrentBallot (result.maxballot);
			this.server_state.wakeMainLoop ();
		}
		return result;
	}

	private boolean runPhaseTwo (
			int instance, int ballot, int value,
			List<Integer> acceptors, int n_acceptors, int quorum) {
		if (!this.server_state.canProposeFor (ballot))
			return true;
		DidaTradePaxos.PhaseTwoRequest request = DidaTradePaxos.PhaseTwoRequest
				.newBuilder ().setInstance (instance).setRequestballot (ballot)
				.setValue (value).build ();
		PhaseTwoResponseProcessor processor = new PhaseTwoResponseProcessor (quorum, acceptors);
		ArrayList<DidaTradePaxos.PhaseTwoReply> responses = new ArrayList<> ();
		GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> collector = new GenericResponseCollector<> (
				responses, n_acceptors, processor);
		for (int acceptor : acceptors) {
			CollectorStreamObserver<DidaTradePaxos.PhaseTwoReply> observer = new CollectorStreamObserver<> (
					collector);
			this.server_state.async_stubs[acceptor]
					.withDeadlineAfter (RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
					.phasetwo (request, observer);
		}
		collector.waitUntilDone ();
		if (processor.getAccepted ())
			return false;
		if (processor.getMaxballot () > this.server_state.getCurrentBallot ()) {
			this.server_state.setCurrentBallot (processor.getMaxballot ());
			this.server_state.wakeMainLoop ();
		}
		return true;
	}

	static final class PhaseOneResult {
		final boolean aborted;
		final int maxballot;
		final boolean membership_transferred;
		private final int[] instances;
		private final Map<Integer, Integer> commands;

		PhaseOneResult(PhaseOneResponseProcessor processor) {
			this.membership_transferred = processor.membershipTransferred ();
			this.aborted = !processor.getAccepted ();
			this.maxballot = processor.getMaxballot ();
			this.instances = processor.instances ();
			this.commands = new HashMap<> ();
			for (int instance : this.instances) {
				int id = processor.commandFor (instance);
				this.commands.put (instance, id);
			}
		}

		int[] instances () {
			return this.instances.clone ();
		}

		int commandFor (int instance) {
			return this.commands.get (instance);
		}

		int highestInstance () {
			return this.instances.length == 0
					? -1
					: Arrays.stream (this.instances).max ().getAsInt ();
		}
	}
}
