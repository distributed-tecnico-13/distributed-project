package didatrade.server;

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
import didatrade.util.GenericResponseCollector;
import didatrade.util.PhaseOneResponseProcessor;
import didatrade.util.PhaseTwoResponseProcessor;

/** Phase 1 once per ballot, then one Phase 2 attempt for a slot MainLoop has claimed. */
final class PaxosProposer {
	static final long RETRY_DELAY_MILLIS = 200;
	static final long RPC_TIMEOUT_SECONDS = 5;

	private final DidaTradeServerState server_state;

	PaxosProposer(DidaTradeServerState state) {
		this.server_state = state;
	}

	// Returns whether this server may still propose. A failed prepare does not open Phase 2.
	boolean prepare (int ballot) {
		if (!this.server_state.canProposeFor (ballot))
			return false;
		if (this.server_state.hasRunPhaseOne (ballot))
			return true;

		List<Integer> acceptors = this.server_state.scheduler.acceptors (ballot);
		PhaseOneResult phase_one = this.runPhaseOne (
				acceptors, acceptors.size (), ballot);
		if (phase_one.aborted)
			return false;

		for (int instance : phase_one.instances ())
			this.server_state.paxos_log.adopt (instance, phase_one.commandFor (instance));
		this.fillNoops (phase_one);
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

	void propose (PaxosInstance entry, int ballot) {
		try {
			if (!this.server_state.canProposeFor (ballot))
				return;
			int instance = entry.getInstanceNb ();
			int phase_two_value = entry.phaseTwoValueFor (ballot, entry.getValueToPropose ());
			this.server_state.paxos_log.replaceProposedRequestId (instance, phase_two_value);
			List<Integer> acceptors = this.server_state.scheduler.acceptors (ballot);
			if (this.runPhaseTwo (instance, ballot, phase_two_value, acceptors,
					acceptors.size (), this.server_state.scheduler.quorum (ballot)))
				return;

			if (entry.decide (ballot, phase_two_value))
				this.server_state.updateCompletedBallot (ballot);
		} finally {
			entry.releaseClaim (ballot, TimeUnit.MILLISECONDS.toNanos (RETRY_DELAY_MILLIS));
		}
	}

	private PhaseOneResult runPhaseOne (
			List<Integer> acceptors, int n_acceptors, int ballot) {
		DidaTradePaxos.PhaseOneRequest request = DidaTradePaxos.PhaseOneRequest
				.newBuilder ().setRequestballot (ballot).build ();
		PhaseOneResponseProcessor processor = new PhaseOneResponseProcessor (
				this.server_state.scheduler, ballot);
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
			if (this.server_state.main_loop != null)
				this.server_state.main_loop.wakeup ();
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
			if (this.server_state.main_loop != null)
				this.server_state.main_loop.wakeup ();
		}
		return true;
	}

	static final class PhaseOneResult {
		final boolean aborted;
		final int maxballot;
		private final int[] instances;
		private final Map<Integer, Integer> commands;

		PhaseOneResult(PhaseOneResponseProcessor processor) {
			this.aborted = !processor.getAccepted ();
			this.maxballot = processor.getMaxballot ();
			this.instances = processor.instances ();
			this.commands = new HashMap<> ();
			for (int instance : this.instances)
				this.commands.put (instance, processor.commandFor (instance));
		}

		int[] instances () {
			return this.instances.clone ();
		}

		int commandFor (int instance) {
			return this.commands.get (instance);
		}
	}
}
