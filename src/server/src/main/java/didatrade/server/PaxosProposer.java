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

		int min_instance = this.server_state.paxos_log.getMinUndecidedEntry ();
		List<Integer> acceptors = this.server_state.scheduler.acceptors (ballot);
		PhaseOneResult phase_one = this.runPhaseOne (
				acceptors, acceptors.size (), ballot, min_instance);
		if (phase_one.aborted)
			return false;

		Logger trace = Logger.log (this.server_state)
				.add ("phase1 ballot=").add (ballot)
				.add (" min=").add (min_instance)
				.add (" ok");
		for (int instance : phase_one.instances ()) {
			PaxosInstance known_entry = this.server_state.paxos_log.getEntry (instance);
			if (known_entry != null && known_entry.isDecided ())
				continue;
			int command = phase_one.commandFor (instance);
			if (!this.server_state.paxos_log.adopt (instance, command))
				continue;
			trace.item ("adopted", instance + ":" + CommandIdHelper.label (command)
					+ "@" + phase_one.ballotFor (instance));
		}
		this.fillNoops (min_instance, phase_one, trace);
		trace.log ();
		this.server_state.setRanPhaseOne (ballot);
		return true;
	}

	private void fillNoops (int min_instance, PhaseOneResult phase_one, Logger trace) {
		int[] instances = phase_one.instances;
		if (instances.length == 0)
			return;
		int start = Math.max (min_instance, 0);
		int max_instance = Arrays.stream (instances).max ().getAsInt ();
		Set<Integer> known = Arrays.stream (instances).boxed ().collect (Collectors.toSet ());
		for (int instance = start; instance <= max_instance; instance++) {
			if (known.contains (instance))
				continue;
			if (this.server_state.paxos_log.adopt (instance, CommandIdHelper.NOOP_VALUE))
				trace.item ("noops", instance);
		}
	}

	void propose (PaxosInstance entry, int ballot) {
		try {
			if (entry.isDecided () || !this.server_state.canProposeFor (ballot))
				return;
			int instance = entry.getInstanceNb ();
			boolean first_attempt = entry.isNewPhaseTwoBallot (ballot);
			int phase_two_value = entry.phaseTwoValueFor (ballot, entry.getProposedRequestId ());
			this.server_state.paxos_log.replaceProposedRequestId (instance, phase_two_value);
			if (first_attempt)
				Logger.log (this.server_state, "propose instance=" + instance
						+ " ballot=" + ballot + " value=" + CommandIdHelper.label (phase_two_value));
			List<Integer> acceptors = this.server_state.scheduler.acceptors (ballot);
			if (this.runPhaseTwo (instance, ballot, phase_two_value, acceptors,
					acceptors.size (), this.server_state.scheduler.quorum (ballot)))
				return;

			if (entry.decide (ballot, phase_two_value)) {
				Logger.log (this.server_state, "decided instance=" + instance
						+ " ballot=" + ballot + " value=" + CommandIdHelper.label (phase_two_value));
				this.server_state.setCompletedBallot (ballot);
			}
		} finally {
			entry.releaseClaim (ballot, TimeUnit.MILLISECONDS.toNanos (RETRY_DELAY_MILLIS));
		}
	}

	private PhaseOneResult runPhaseOne (
			List<Integer> acceptors, int n_acceptors, int ballot, int min_instance) {
		DidaTradePaxos.PhaseOneRequest request = DidaTradePaxos.PhaseOneRequest
				.newBuilder ().setMinInstance (min_instance).setRequestballot (ballot).build ();
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
			Logger.log (this.server_state, "phase1 ballot=" + ballot
					+ " aborted maxballot=" + result.maxballot);
			this.server_state.setCurrentBallot (result.maxballot);
			if (this.server_state.main_loop != null)
				this.server_state.main_loop.wakeup ();
		} else if (result.aborted) {
			Logger.log (this.server_state, "phase1 ballot=" + ballot + " no-quorum");
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
		PhaseTwoResponseProcessor processor = new PhaseTwoResponseProcessor (quorum);
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
			Logger.log (this.server_state, "phase2 instance=" + instance
					+ " ballot=" + ballot + " rejected maxballot=" + processor.getMaxballot ());
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
		private final Map<Integer, Integer> ballots;

		PhaseOneResult(PhaseOneResponseProcessor processor) {
			this.aborted = !processor.getAccepted ();
			this.maxballot = processor.getMaxballot ();
			this.instances = processor.instances ();
			this.commands = new HashMap<> ();
			this.ballots = new HashMap<> ();
			for (int instance : this.instances) {
				this.commands.put (instance, processor.commandFor (instance));
				this.ballots.put (instance, processor.ballotFor (instance));
			}
		}

		int[] instances () {
			return this.instances.clone ();
		}

		int commandFor (int instance) {
			return this.commands.get (instance);
		}

		int ballotFor (int instance) {
			return this.ballots.get (instance);
		}
	}
}
