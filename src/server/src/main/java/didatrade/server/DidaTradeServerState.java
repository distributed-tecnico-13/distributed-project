package didatrade.server;

import java.util.*;

import didatrade.core.*;
import didatrade.configs.*;
import didatrade.configs.ConfigurationScheduler; // ignore-unused-import

import didatrade.DidaTradeMain; // ignore-unused-import
import didatrade.DidaTradePaxos; // ignore-unused-import
import didatrade.DidaTradePaxosServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public final class DidaTradeServerState {
	public static final int DEFAULT_POPULATION = 10;
	int max_participants;
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
	private int completed_ballot;
	private DebugState debug_state;
	private boolean fastpaxos_on;
	private int fast_first_instance;
	private int ran_phase_one_for_ballot;
	private int last_executed_instance;

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
		this.completed_ballot = -1;
		this.req_history = new RequestHistory ();
		this.paxos_log = new PaxosLog ();
		this.main_loop = new MainLoop (this);
		this.ran_phase_one_for_ballot = -1;
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

	public synchronized int getCurrentBallot () {
		return this.current_ballot;
	}

	public synchronized void setCurrentBallot (int ballot) {
		this.disableFastPaxosIfNeeded (ballot);
		this.current_ballot = Math.max (this.current_ballot, ballot);
	}

	private void disableFastPaxosIfNeeded (int ballot) {
		if (ballot > this.current_ballot)
			this.fastpaxos_on = false;
	}

	public synchronized boolean canProposeFor (int ballot) {
		return ballot >= 0 && ballot == this.current_ballot && this.scheduler.leader (ballot) == this.my_id;
	}

	public record BalanceResult(int balance, int lastExecutedInstance) {
	}

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

	static boolean canProposeFor (
			int ballot, int current_ballot, int leader, int server_id) {
		return ballot >= 0 && ballot == current_ballot && leader == server_id;
	}

	// Test-only construction avoids starting RPC channels and the MainLoop thread.
	DidaTradeServerState() {
		this.current_ballot = 0;
		this.debug_state = new DebugState ();
		this.last_executed_instance = -1;
	}

	// Lock order: server state, then one PaxosInstance. No RPC runs under either lock.
	public synchronized PaxosInstance.PhaseOneState processPhaseOne (
			PaxosInstance entry, int ballot) {
		this.disableFastPaxosIfNeeded (ballot);
		boolean accepted = ballot >= this.current_ballot;
		if (accepted)
			this.current_ballot = ballot;
		return entry.phaseOneState (accepted, ballot, this.current_ballot);
	}

	public synchronized PhaseOneState processPhaseOne (int ballot) {
		this.disableFastPaxosIfNeeded (ballot);
		boolean accepted = ballot >= this.current_ballot;
		if (accepted)
			this.current_ballot = ballot;
		return new PhaseOneState (accepted, this.current_ballot);
	}

	public synchronized PhaseTwoState processPhaseTwo (
			PaxosInstance entry, int ballot, int value) {
		this.disableFastPaxosIfNeeded (ballot);
		boolean accepted = false;
		if (ballot >= this.current_ballot) {
			this.current_ballot = ballot;
			accepted = entry.accept (ballot, value);
			if (accepted)
				entry.noteClassicAcceptance (ballot, value);
		}
		return new PhaseTwoState (accepted, this.current_ballot);
	}

	public synchronized boolean isFastRoundOpen (int ballot) {
		return this.fastpaxos_on && ballot == this.current_ballot
				&& this.scheduler != null && this.scheduler.fastpaxos (ballot);
	}

	public synchronized boolean canCoordinateFastRound (int ballot) {
		return this.canProposeFor (ballot) && this.scheduler.fastpaxos (ballot)
				&& this.hasRunPhaseOne (ballot);
	}

	public synchronized boolean canOpenFastRound (int ballot) {
		return this.scheduler != null && ballot >= this.current_ballot
				&& this.scheduler.fastpaxos (ballot)
				&& this.scheduler.isacceptor (this.my_id, ballot);
	}

	public synchronized boolean openFastRound (int ballot) {
		return this.openFastRound (ballot, 0);
	}

	public synchronized boolean openFastRound (int ballot, int first_fast_instance) {
		if (!this.canOpenFastRound (ballot) || first_fast_instance < 0)
			return false;
		this.disableFastPaxosIfNeeded (ballot);
		this.current_ballot = ballot;
		// Never allow Fast proposals to reuse slots reserved for classic recovery.
		this.fast_first_instance = Math.max (this.fast_first_instance, first_fast_instance);
		this.setFastPaxosMode (true);
		return true;
	}

	public synchronized FastVoteState processFastVote (int request_id) {
		int ballot = this.current_ballot;
		if (!this.isFastRoundOpen (ballot)
				|| !this.scheduler.isacceptor (this.my_id, ballot))
			return new FastVoteState (false, ballot, -1);

		PaxosInstance entry = this.paxos_log.assignFastRequest (request_id, this.fast_first_instance);
		boolean accepted = entry.accept (ballot, request_id);
		return new FastVoteState (accepted, ballot, entry.getInstanceNb ());
	}

	public synchronized int getCompletedBallot () {
		return this.completed_ballot;
	}

	public synchronized int findMaxDecidedBallot () {
		int ballot = -1;
		for (PaxosInstance entry : this.paxos_log.entries ()) {
			if (entry.isDecided () && entry.getAcceptBallot () > ballot)
				ballot = entry.getAcceptBallot ();
		}
		return ballot;
	}

	public synchronized void updateCompletedBallot (int ballot) {
		// WARNING: THIS ONLY WORKS FOR CONFIGURATIONS WHERE THERE IS NO NEED FOR STATE-TRANSFER!!!!!
		// TODO: NEEDS TO BE UPDATE FOR THE PROJECT

		ballot = this.findMaxDecidedBallot ();
		if (ballot > this.completed_ballot)
			this.completed_ballot = ballot;
		this.notifyAll ();
	}

	public synchronized void setCompletedBallot (int ballot) {
		if (ballot > this.completed_ballot)
			this.completed_ballot = ballot;
		this.notifyAll ();
	}

	public synchronized int waitForCompletedBallot (int ballot) {
		while (this.completed_ballot < ballot) {
			try {
				wait ();
			} catch (InterruptedException e) {
			}
		}
		return this.completed_ballot;
	}

	synchronized boolean hasRunPhaseOne (int ballot) {
		return this.ran_phase_one_for_ballot == ballot;
	}

	synchronized void setRanPhaseOne (int ballot) {
		this.ran_phase_one_for_ballot = ballot;
	}

	public synchronized boolean getFastPaxosMode () {
		return this.fastpaxos_on;
	}

	public synchronized void setFastPaxosMode (boolean mode) {
		this.fastpaxos_on = mode;
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
		final boolean accepted;
		final int maxballot;

		PhaseOneState(boolean accepted, int maxballot) {
			this.accepted = accepted;
			this.maxballot = maxballot;
		}
	}

	public static final class PhaseTwoState {
		final boolean accepted;
		final int maxballot;

		PhaseTwoState(boolean accepted, int maxballot) {
			this.accepted = accepted;
			this.maxballot = maxballot;
		}
	}
}
