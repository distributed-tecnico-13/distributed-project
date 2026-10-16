package didatrade.server;

import java.util.concurrent.RejectedExecutionException;
import java.util.function.IntFunction;

/** Single coordinator: recover a ballot, open Fast rounds, and execute in order. */
public class MainLoop implements Runnable {
	DidaTradeServerState server_state;

	private boolean has_work;
	private final PaxosPipeline pipeline;
	private final PaxosProposer proposer;
	private final OrderedPaxosExecutor ordered_executor;
	// Called when the loop hits an unexpected error. The server process installs System.exit here;
	// by default (for tests) only this loop ends.
	private volatile Runnable fatal_handler = () -> {
	};

	public MainLoop(DidaTradeServerState state) {
		this.server_state = state;
		this.has_work = false;
		this.pipeline = new PaxosPipeline (this::signal);
		this.proposer = new PaxosProposer (state);
		this.ordered_executor = new OrderedPaxosExecutor (state);
	}

	public void setFatalHandler (Runnable handler) {
		this.fatal_handler = handler;
	}

	// Event loop: execute decided slots in order, schedule consensus work, then sleep until woken
	// or the next retry is due.
	public void run () {
		// Wait for the first event so startup cannot open an incomplete Fast cluster.
		this.waitForWork (-1);

		while (!Thread.currentThread ().isInterrupted ()) {
			synchronized (this) {
				this.has_work = false;
			}
			try {
				boolean progressed = this.ordered_executor.drain () > 0;
				progressed = this.scheduleConsensus () || progressed;
				if (!progressed)
					this.waitForWork (this.nextRetryDelayMillis ());
			} catch (RejectedExecutionException e) {
				// gRPC refuses new calls once its channel is shut down: this replica is going down.
				// The interrupt that should end the loop can be lost while waiting for replies.
				break;
			} catch (RuntimeException e) {
				// Unexpected, so this replica's Paxos state can no longer be trusted: stop instead of
				// carrying on. The server process exits like any crashed replica (see DidaTradeServer).
				System.err.println ("MainLoop: unexpected error, stopping: " + e);
				e.printStackTrace ();
				this.fatal_handler.run ();
				break;
			}
		}
		this.pipeline.shutdown ();
	}

	public synchronized void wakeup () {
		this.signal ();
	}

	private synchronized void signal () {
		this.has_work = true;
		notifyAll ();
	}

	private synchronized void waitForWork (long retry_delay_millis) {
		if (this.has_work)
			return;
		try {
			if (retry_delay_millis < 0)
				wait ();
			else
				wait (Math.max (1, retry_delay_millis));
		} catch (InterruptedException e) {
			Thread.currentThread ().interrupt ();
		}
	}

	// One scheduling pass for the current ballot; true if it made progress.
	private boolean scheduleConsensus () {
		int ballot = this.server_state.getCurrentBallot ();
		// Phase 1 (once per ballot, leader only); adopts the recovered slots into the log.
		if (!this.proposer.prepare (ballot))
			return false;

		// Dynamic membership: only the recovered prefix is re-proposed and transferred to the new
		// group; no new requests are assigned until the transfer is done.
		if (this.server_state.scheduler.dynamicMembership () && !this.proposer.transferComplete (ballot)) {
			if (!this.proposer.fastRecoveryComplete (ballot))
				return this.scheduleRecovery (ballot);
			return this.proposer.finishTransfer (ballot);
		}
		// Classic: the leader assigns the slots, so one loop re-proposes recovered slots and
		// proposes new requests. Fast: re-propose recovered slots, then open the Fast round; from
		// then on the acceptors assign new requests to slots themselves.
		if (this.server_state.scheduler.fastpaxos (ballot))
			return this.scheduleFastBallot (ballot);
		return this.scheduleClassicBallot (ballot);
	}

	private boolean scheduleFastBallot (int ballot) {
		// Local openfast acceptance is NOT evidence that the opening quorum replied.
		// Retry if this coordinator hasn't confirmed the Fast opening quorum.
		if (this.proposer.hasFastOpeningQuorum (ballot))
			return false;

		if (this.proposer.fastRecoveryComplete (ballot))
			return this.proposer.openFastRound (ballot);

		return this.scheduleRecovery (ballot);
	}

	// Re-proposes the slots Phase 1 recovered (below recoveryLimit) and nothing else. Used where
	// the leader must not assign new requests: before a Fast round opens (the acceptors fill the
	// slots from recoveryLimit on themselves) and during state transfer (which only covers the
	// recovered prefix).
	private boolean scheduleRecovery (int ballot) {
		// getEntry only looks slots up; it never creates a slot or assigns a request.
		return this.scheduleSlots (ballot, this.proposer.recoveryLimit (ballot),
				this.server_state.paxos_log::getEntry);
	}

	// In a classic ballot the leader alone assigns requests to slots, so recovered slots and new
	// pending requests share one loop (unlike scheduleRecovery, which must leave new slots alone).
	private boolean scheduleClassicBallot (int ballot) {
		int limit = this.ordered_executor.getNextInstanceToExecute () + PaxosPipeline.CAPACITY;
		// assignPendingRequest gives an empty slot the next pending request that no other slot
		// owns. A slot that already holds a value (recovered or assigned) is returned unchanged,
		// so it is simply re-proposed.
		return this.scheduleSlots (ballot, limit, instance -> this.server_state.paxos_log
				.assignPendingRequest (instance, this.server_state.req_history));
	}

	// Proposes every slot below limit that slotFor yields, until the pipeline is full. slotFor
	// decides whether a slot may also receive a new request, or only an existing one.
	private boolean scheduleSlots (int ballot, int limit, IntFunction<PaxosInstance> slotFor) {
		boolean submitted = false;
		for (int instance = 0; instance < limit; instance++) {
			if (!this.pipeline.hasCapacity ())
				break;
			PaxosInstance entry = slotFor.apply (instance);
			if (entry == null || !entry.tryClaim (ballot, System.nanoTime ()))
				continue;
			if (!this.pipeline.submitTask ( () -> this.proposer.propose (entry, ballot))) {
				entry.releaseClaim (ballot, 0);
				break;
			}
			submitted = true;
		}
		return submitted;
	}

	// How long to sleep: -1 waits for an event, otherwise until the earliest retry backoff expires.
	private long nextRetryDelayMillis () {
		int ballot = this.server_state.getCurrentBallot ();
		if (!this.server_state.canProposeFor (ballot) || !this.pipeline.hasCapacity ())
			return -1;
		if (!this.server_state.hasRunPhaseOne (ballot))
			return PaxosProposer.RETRY_DELAY_MILLIS;

		if (this.server_state.scheduler.dynamicMembership () && !this.proposer.transferComplete (ballot))
			return this.nextRecoveryRetryDelayMillis (ballot);
		if (this.server_state.scheduler.fastpaxos (ballot)) {
			if (this.proposer.hasFastOpeningQuorum (ballot))
				return -1;
			if (this.proposer.fastRecoveryComplete (ballot))
				return PaxosProposer.RETRY_DELAY_MILLIS;
			return this.nextRecoveryRetryDelayMillis (ballot);
		}
		return this.nextClassicRetryDelayMillis (ballot);
	}

	private long nextRecoveryRetryDelayMillis (int ballot) {
		long earliest = this.earliestRetryDelayMillis (ballot, 0, this.proposer.recoveryLimit (ballot));
		return earliest == Long.MAX_VALUE ? PaxosProposer.RETRY_DELAY_MILLIS : earliest;
	}

	private long nextClassicRetryDelayMillis (int ballot) {
		int first = this.ordered_executor.getNextInstanceToExecute ();
		long earliest = this.earliestRetryDelayMillis (ballot, first, first + PaxosPipeline.CAPACITY);
		return earliest == Long.MAX_VALUE ? -1 : earliest;
	}

	// Smallest retry delay over the slots [from, to), or Long.MAX_VALUE if none is waiting.
	private long earliestRetryDelayMillis (int ballot, int from, int to) {
		long earliest = Long.MAX_VALUE;
		long now = System.nanoTime ();
		for (int instance = from; instance < to; instance++) {
			PaxosInstance entry = this.server_state.paxos_log.getEntry (instance);
			if (entry == null)
				continue;
			long delay = entry.retryDelayMillis (ballot, now);
			if (delay >= 0)
				earliest = Math.min (earliest, delay);
		}
		return earliest;
	}
}
