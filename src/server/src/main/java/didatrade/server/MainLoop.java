package didatrade.server;

/** Single coordinator: recover a ballot, open Fast rounds, and execute in order. */
public class MainLoop implements Runnable {
	DidaTradeServerState server_state;

	private boolean has_work;
	private final PaxosPipeline pipeline;
	private final PaxosProposer proposer;
	private final OrderedPaxosExecutor ordered_executor;

	public MainLoop(DidaTradeServerState state) {
		this.server_state = state;
		this.has_work = false;
		this.pipeline = new PaxosPipeline (this::signal);
		this.proposer = new PaxosProposer (state);
		this.ordered_executor = new OrderedPaxosExecutor (state);
	}

	public void run () {
		// Do not prepare/open ballot 0 while replicas are still starting. The project
		// guarantees that all servers are running before clients start, so wait for the
		// first client/configuration event before starting consensus. This also prevents
		// a Fast round from being opened on only the first replicas that happened to bind.
		this.waitForWork (-1);

		while (!Thread.currentThread ().isInterrupted ()) {
			synchronized (this) {
				this.has_work = false;
			}
			boolean progressed = this.ordered_executor.drain () > 0;
			progressed = this.scheduleConsensus () || progressed;
			if (!progressed)
				this.waitForWork (this.nextRetryDelayMillis ());
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

	private boolean scheduleConsensus () {
		int ballot = this.server_state.getCurrentBallot ();
		if (!this.proposer.prepare (ballot))
			return false;

		if (this.server_state.scheduler.fastpaxos (ballot))
			return this.scheduleFastBallot (ballot);
		return this.scheduleClassicBallot (ballot);
	}

	private boolean scheduleFastBallot (int ballot) {
		if (this.server_state.isFastRoundOpen (ballot))
			return false;

		if (this.proposer.fastRecoveryComplete (ballot))
			return this.proposer.openFastRound (ballot);

		boolean submitted = false;
		int limit = this.proposer.recoveryLimit (ballot);
		for (int instance = 0; instance < limit; instance++) {
			if (!this.pipeline.hasCapacity ())
				break;
			PaxosInstance entry = this.server_state.paxos_log.getEntry (instance);
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

	private boolean scheduleClassicBallot (int ballot) {
		boolean submitted = false;
		int limit = this.ordered_executor.getNextInstanceToExecute () + PaxosPipeline.CAPACITY;
		for (int instance = 0; instance < limit; instance++) {
			if (!this.pipeline.hasCapacity ())
				break;
			PaxosInstance entry = this.server_state.paxos_log.assignPendingRequest (
					instance, this.server_state.req_history);
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

	private long nextRetryDelayMillis () {
		int ballot = this.server_state.getCurrentBallot ();
		if (!this.server_state.canProposeFor (ballot) || !this.pipeline.hasCapacity ())
			return -1;
		if (!this.server_state.hasRunPhaseOne (ballot))
			return PaxosProposer.RETRY_DELAY_MILLIS;

		if (this.server_state.scheduler.fastpaxos (ballot)) {
			if (this.server_state.isFastRoundOpen (ballot))
				return -1;
			if (this.proposer.fastRecoveryComplete (ballot))
				return PaxosProposer.RETRY_DELAY_MILLIS;
			return this.nextRecoveryRetryDelayMillis (ballot);
		}
		return this.nextClassicRetryDelayMillis (ballot);
	}

	private long nextRecoveryRetryDelayMillis (int ballot) {
		long earliest = Long.MAX_VALUE;
		long now = System.nanoTime ();
		for (int instance = 0; instance < this.proposer.recoveryLimit (ballot); instance++) {
			PaxosInstance entry = this.server_state.paxos_log.getEntry (instance);
			if (entry == null)
				continue;
			long delay = entry.retryDelayMillis (ballot, now);
			if (delay >= 0)
				earliest = Math.min (earliest, delay);
		}
		return earliest == Long.MAX_VALUE ? PaxosProposer.RETRY_DELAY_MILLIS : earliest;
	}

	private long nextClassicRetryDelayMillis (int ballot) {
		int first = this.ordered_executor.getNextInstanceToExecute ();
		long earliest = Long.MAX_VALUE;
		long now = System.nanoTime ();
		for (int instance = first; instance < first + PaxosPipeline.CAPACITY; instance++) {
			PaxosInstance entry = this.server_state.paxos_log.getEntry (instance);
			if (entry == null)
				continue;
			long delay = entry.retryDelayMillis (ballot, now);
			if (delay >= 0)
				earliest = Math.min (earliest, delay);
		}
		return earliest == Long.MAX_VALUE ? -1 : earliest;
	}
}
