package didatrade.server;

import java.util.*;

import didatrade.core.*;
import didatrade.configs.*;

import didatrade.DidaTradePaxosServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import didatrade.server.request.RequestHistory;

/**
 * Per-replica state shared by the gRPC handlers and the MainLoop thread. Ballot, Fast round and
 * execution state is guarded by this object's monitor.
 *
 * Convention: state transitions (promise, accept, Fast vote) are decided here under the monitor
 * and never send RPCs. The caller notifies learners or wakes the MainLoop afterwards, so no
 * network call ever holds the lock.
 */
public final class DidaTradeServerState {
	public static final int DEFAULT_POPULATION = 10;
	TradeManager trade_manager;
	ConfigurationScheduler scheduler;
	int base_port;
	int my_id;
	RequestHistory req_history;
	PaxosLog paxos_log;

	List<Integer> all_participants;
	int n_participants;
	String[] targets;
	ManagedChannel[] channels;
	DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[] async_stubs;

	private int current_ballot;
	private int last_completed_ballot = -1;
	private DebugState debug_state;
	private boolean fastpaxos_on;
	// First slot Fast proposals may use; lower slots belong to classic recovery.
	private int first_fast_instance;
	private int last_phase_one_completed_ballot = -1;
	private int last_executed_instance = -1;
	private boolean membership_transferred;

	MainLoop main_loop;
	Thread main_loop_worker;

	public DidaTradeServerState(int port, int myself, char schedule) {
		this.trade_manager = new TradeManager ();
		this.scheduler = new ConfigurationScheduler (schedule);
		this.base_port = port;
		this.my_id = myself;
		this.debug_state = new DebugState ();
		this.fastpaxos_on = false;
		this.current_ballot = 0;
		this.last_completed_ballot = -1;
		this.req_history = new RequestHistory ();
		this.paxos_log = new PaxosLog ();
		this.main_loop = new MainLoop (this);
		this.last_phase_one_completed_ballot = -1;
		this.last_executed_instance = -1;

		// populate manager
		this.trade_manager.populate (DEFAULT_POPULATION);

		// init comms
		this.all_participants = this.scheduler.allparticipants ();
		this.n_participants = all_participants.size ();

		this.targets = new String[this.n_participants];
		for (int i = 0; i < this.n_participants; i++) {
			int target_port = this.base_port + all_participants.get (i);
			this.targets[i] = new String ();
			this.targets[i] = "localhost:" + target_port;
			System.out.printf ("targets[%d] = %s%n", i, targets[i]);
		}

		this.channels = new ManagedChannel[this.n_participants];
		for (int i = 0; i < this.n_participants; i++)
			this.channels[i] = ManagedChannelBuilder.forTarget (this.targets[i]).usePlaintext ().build ();

		this.async_stubs = new DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[this.n_participants];
		for (int i = 0; i < this.n_participants; i++)
			this.async_stubs[i] = DidaTradePaxosServiceGrpc.newStub (this.channels[i]);

		// start worker
		this.main_loop_worker = new Thread (main_loop);
		this.main_loop_worker.start ();
	}

	// Test-only construction avoids starting RPC channels and the MainLoop thread;
	// the checks this makes necessary are collected in TestHelper.
	DidaTradeServerState() {
		this.current_ballot = 0;
		this.debug_state = new DebugState ();
		this.last_executed_instance = -1;
	}

	// ---- Plumbing: accessors, MainLoop wake-up and debug ----

	public int getMyId () {
		return this.my_id;
	}

	public ConfigurationScheduler getScheduler () {
		return this.scheduler;
	}

	public RequestHistory getRequestHistory () {
		return this.req_history;
	}

	public PaxosLog getPaxosLog () {
		return this.paxos_log;
	}

	public MainLoop getMainLoop () {
		return this.main_loop;
	}

	public void wakeMainLoop () {
		if (TestHelper.hasMainLoop (this))
			this.main_loop.wakeup ();
	}

	public synchronized DebugState getDebugState () {
		return this.debug_state;
	}

	public void applyDebugState () {
		this.debug_state.apply ();
	}

	public void setDebugMode (int mode) {
		this.debug_state.setDebugMode (mode);
	}

	// ---- Ballot (shared by all roles) ----

	public synchronized int getCurrentBallot () {
		return this.current_ballot;
	}

	public synchronized void setCurrentBallot (int ballot) {
		this.disableFastPaxosIfNeeded (ballot);
		this.current_ballot = Math.max (this.current_ballot, ballot);
		this.notifyAll ();
	}

	private void disableFastPaxosIfNeeded (int ballot) {
		if (ballot > this.current_ballot)
			this.fastpaxos_on = false;
	}

	// ---- Proposer (leader) ----

	// True if ballot is the current ballot and this replica is its leader.
	public synchronized boolean canProposeFor (int ballot) {
		return ballot == this.current_ballot && this.scheduler.leader (ballot) == this.my_id;
	}

	static boolean canProposeFor (
			int ballot, int current_ballot, int leader, int server_id) {
		return ballot >= 0 && ballot == current_ballot && leader == server_id;
	}

	// Leader view: this replica leads a Fast ballot and has finished Phase 1 for it.
	public synchronized boolean canCoordinateFastRound (int ballot) {
		return this.canProposeFor (ballot) && this.scheduler.fastpaxos (ballot)
				&& this.hasRunPhaseOne (ballot);
	}

	synchronized boolean hasRunPhaseOne (int ballot) {
		return this.last_phase_one_completed_ballot == ballot;
	}

	synchronized void setRanPhaseOne (int ballot) {
		this.last_phase_one_completed_ballot = ballot;
	}

	// ---- Acceptor: promise, accept and Fast votes ----

	// Phase 1 promise
	public synchronized PhaseOneState processPhaseOne (int ballot) {
		boolean accepted = ballot >= this.current_ballot;
		if (accepted)
			this.setCurrentBallot (ballot);
		return new PhaseOneState (accepted, this.current_ballot);
	}

	// Phase 2: accepts (ballot, value) in one slot iff this replica is an acceptor of the ballot
	// and has not promised a higher one.
	// Lock order: server state, then one PaxosInstance. No RPC runs under either lock; the
	// caller notifies the learners after this returns.
	public synchronized PhaseTwoState processPhaseTwo (
			PaxosInstance entry, int ballot, int value) {
		boolean accepted = false;
		if (this.scheduler.isacceptor (this.my_id, ballot) && ballot >= this.current_ballot) {
			this.setCurrentBallot (ballot);
			accepted = entry.accept (ballot, value);
			if (accepted)
				entry.noteClassicAcceptance (value);
		}
		return new PhaseTwoState (accepted, this.current_ballot);
	}

	// Acceptor view: this replica has entered the Fast round of the ballot.
	public synchronized boolean isFastRoundOpen (int ballot) {
		return this.fastpaxos_on && ballot == this.current_ballot
				&& TestHelper.hasScheduler (this) && this.scheduler.fastpaxos (ballot);
	}

	// Acceptor check for an openfast request: a Fast ballot that is not older than the current one.
	public synchronized boolean canOpenFastRound (int ballot) {
		return TestHelper.hasScheduler (this) && ballot >= this.current_ballot
				&& this.scheduler.fastpaxos (ballot)
				&& this.scheduler.isacceptor (this.my_id, ballot);
	}

	public synchronized boolean openFastRound (int ballot) {
		return this.openFastRound (ballot, 0);
	}

	// Enters Fast mode for the ballot (handles the openfast RPC).
	public synchronized boolean openFastRound (int ballot, int first_fast_instance) {
		if (!this.canOpenFastRound (ballot) || first_fast_instance < 0)
			return false;
		this.setCurrentBallot (ballot);
		// Never allow Fast proposals to reuse slots reserved for classic recovery.
		this.first_fast_instance = Math.max (this.first_fast_instance, first_fast_instance);
		this.setFastPaxosMode (true);
		return true;
	}

	// Fast acceptor: votes for a client request directly, in the first free slot at or after
	// fast_first_instance, without waiting for a leader proposal. Only decides and records the
	// vote; FastPaxosAcceptor.tryAcceptRequest notifies the learners afterwards, outside the lock.
	public synchronized FastVoteState fastVote (int request_id) {
		int ballot = this.current_ballot;
		if (!this.isFastRoundOpen (ballot)
				|| !this.scheduler.isacceptor (this.my_id, ballot))
			return new FastVoteState (false, ballot, -1);

		PaxosInstance entry = this.paxos_log.assignFastRequest (request_id, this.first_fast_instance);
		boolean accepted = entry.accept (ballot, request_id);
		return new FastVoteState (accepted, ballot, entry.getInstanceNb ());
	}

	public synchronized boolean getFastPaxosMode () {
		return this.fastpaxos_on;
	}

	public synchronized void setFastPaxosMode (boolean mode) {
		this.fastpaxos_on = mode;
	}

	// Dynamic membership: set once the recovered prefix was installed here.
	public synchronized boolean membershipTransferred () {
		return this.membership_transferred;
	}

	synchronized void noteMembershipTransferred () {
		this.membership_transferred = true;
	}

	// ---- Learner: Fast collisions and ballot completion ----

	// If the next ballot's leader has crashed, the system stays blocked until the console
	// issues a newer ballot.
	public synchronized boolean advanceFromCollidedFastBallot (int ballot) {
		if (ballot != this.current_ballot)
			return false;
		this.setCurrentBallot (ballot + 1);
		return true;
	}

	public synchronized int getCompletedBallot () {
		return this.last_completed_ballot;
	}

	public synchronized int findMaxDecidedBallot () {
		int ballot = -1;
		for (PaxosInstance entry : this.paxos_log.entries ()) {
			if (entry.isDecided () && entry.getAcceptBallot () > ballot)
				ballot = entry.getAcceptBallot ();
		}
		return ballot;
	}

	// Static schedules: a ballot is completed once slots are decided in it, so each replica
	// derives that from its own log (after a Learn decision or a successful Phase 2).
	public synchronized void updateCompletedBallot () {
		if (TestHelper.hasScheduler (this) && this.scheduler.dynamicMembership ())
			return; // Dynamic completion is acknowledged only after state transfer.

		this.setCompletedBallot (this.findMaxDecidedBallot ());
	}

	// Completion established from outside the log: the console's last known completed ballot
	// (static schedules), or a finished state transfer (dynamic membership, where decided
	// slots alone are not enough). Only raises the value; wakes waitForCompletedBallot.
	public synchronized void setCompletedBallot (int ballot) {
		if (ballot > this.last_completed_ballot)
			this.last_completed_ballot = ballot;
		this.notifyAll ();
	}

	// Blocks a newballot request until its ballot is completed or superseded.
	public synchronized int waitForCompletedBallot (int ballot) {
		// Stop waiting if a newer ballot supersedes this ballot before completion.
		while (this.last_completed_ballot < ballot && this.current_ballot <= ballot) {
			try {
				wait ();
			} catch (InterruptedException e) {
			}
		}
		return this.last_completed_ballot;
	}

	// ---- Execution and client reads ----

	public record BalanceResult(int balance, int lastExecutedInstance) {
	}

	// Waits until minInstance (the newest instance the client has seen) is executed, so a read is
	// ordered after the client's earlier commands.
	public BalanceResult getBalance (int minInstance, int userid) {
		this.waitForInstanceExecution (minInstance);
		synchronized (this) {
			return new BalanceResult (this.trade_manager.balance (userid), this.last_executed_instance);
		}
	}

	synchronized void waitForInstanceExecution (int instance) {
		// Starts at -1, so a client that has observed nothing never waits.
		while (!isExecuted (instance)) {
			try {
				wait ();
			} catch (InterruptedException e) {
				Thread.currentThread ().interrupt ();
				throw new RuntimeException ("Thread interrupted while waiting for instance execution", e);
			}
		}
	}

	public synchronized boolean isExecuted (int instance) {
		return this.last_executed_instance >= instance;
	}

	// Only the executor advances this, in log order, so every lower instance is executed too.
	public synchronized void setLastExecutedInstance (int instance) {
		this.last_executed_instance = instance;
		this.notifyAll ();
	}

	public synchronized int getLastExecutedInstance () {
		return this.last_executed_instance;
	}

	// ---- Result types ----

	public static final class FastVoteState {
		final boolean accepted;
		final int ballot;
		final int instance;

		FastVoteState(boolean accepted, int ballot, int instance) {
			this.accepted = accepted;
			this.ballot = ballot;
			this.instance = instance;
		}
	}

	public static final class PhaseOneState {
		public final boolean accepted;
		public final int maxballot;

		PhaseOneState(boolean accepted, int maxballot) {
			this.accepted = accepted;
			this.maxballot = maxballot;
		}
	}

	public static final class PhaseTwoState {
		public final boolean accepted;
		public final int maxballot;

		PhaseTwoState(boolean accepted, int maxballot) {
			this.accepted = accepted;
			this.maxballot = maxballot;
		}
	}
}
