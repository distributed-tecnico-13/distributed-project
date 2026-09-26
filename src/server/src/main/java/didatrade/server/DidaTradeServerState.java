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
	private boolean wasLeaderAndRanPhaseOne;

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
		this.wasLeaderAndRanPhaseOne = false;

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
		if (ballot > this.current_ballot)
			this.current_ballot = ballot;
	}

	public synchronized boolean canProposeFor (int ballot) {
		boolean can_propose = ballot >= 0 && canProposeFor (ballot, this.current_ballot,
				this.scheduler.leader (ballot), this.my_id);
		if (!can_propose)
			this.wasLeaderAndRanPhaseOne = false;
		return can_propose;
	}

	static boolean canProposeFor (
			int ballot, int current_ballot, int leader, int server_id) {
		return ballot >= 0 && ballot == current_ballot && leader == server_id;
	}

	// Test-only construction avoids starting RPC channels and the MainLoop thread.
	DidaTradeServerState() {
		this.current_ballot = 0;
		this.debug_state = new DebugState ();
	}

	// Lock order: server state, then one PaxosInstance. No RPC runs under either lock.
	public synchronized PaxosInstance.PhaseOneState processPhaseOne (
			PaxosInstance entry, int ballot) {
		boolean accepted = ballot >= this.current_ballot;
		if (accepted)
			this.current_ballot = ballot;
		return entry.phaseOneState (accepted, ballot, this.current_ballot);
	}

	public synchronized PhaseOneState processPhaseOne (int ballot) {
		boolean accepted = ballot >= this.current_ballot;
		if (accepted)
			this.current_ballot = ballot;
		return new PhaseOneState (accepted, this.current_ballot);
	}

	public synchronized PhaseTwoState processPhaseTwo (
			PaxosInstance entry, int ballot, int value) {
		boolean accepted = false;
		if (ballot >= this.current_ballot) {
			this.current_ballot = ballot;
			accepted = entry.accept (ballot, value);
		}
		return new PhaseTwoState (accepted, this.current_ballot);
	}

	public synchronized int getCompletedBallot () {
		return this.completed_ballot;
	}

	public int findMaxDecidedBallot () {
		int ballot = -1;
		int length = this.paxos_log.length ();

		for (int i = 0; i < length; i++) {
			PaxosInstance entry = this.paxos_log.getEntry (i);
			if (entry == null)
				return ballot;
			if (!entry.isDecided ())
				return ballot;
			else if (entry.getAcceptBallot () > ballot)
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

	synchronized boolean hasRunPhaseOne () {
		return this.wasLeaderAndRanPhaseOne;
	}

	synchronized void setRanPhaseOne (boolean ran) {
		this.wasLeaderAndRanPhaseOne = ran;
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
