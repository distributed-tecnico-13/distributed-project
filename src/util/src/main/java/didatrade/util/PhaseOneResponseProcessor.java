package didatrade.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;

public class PhaseOneResponseProcessor
		extends
			GenericResponseProcessor<DidaTradePaxos.PhaseOneReply> {

	private final ConfigurationScheduler scheduler;
	private final int low_ballot;
	private final int high_ballot;

	private final int quorum;
	private final Set<Integer> acceptors;
	private final Set<Integer> responders;

	private boolean accepted;
	private int responses;

	private int value;
	private int valballot;
	private int maxballot;

	public PhaseOneResponseProcessor(ConfigurationScheduler s, int l, int h) {
		this.scheduler = s;
		this.low_ballot = l;
		this.high_ballot = h;

		this.quorum = this.scheduler.quorum (this.high_ballot);

		this.acceptors = new HashSet<> (
				this.scheduler.acceptors (this.high_ballot));
		this.responders = new HashSet<> ();

		this.accepted = false;
		this.responses = 0;

		this.value = -1;
		this.valballot = -1;
		this.maxballot = -1;
	}

	public boolean getAccepted () {
		return this.accepted;
	}

	public int getValue () {
		return this.value;
	}

	public int getValballot () {
		return this.valballot;
	}

	public int getMaxballot () {
		return this.maxballot;
	}

	public synchronized boolean onNext (
			ArrayList<DidaTradePaxos.PhaseOneReply> all_responses,
			DidaTradePaxos.PhaseOneReply last_response) {

		int server_id = last_response.getServerid ();

		if (!this.acceptors.contains (server_id))
			return false;

		if (!this.responders.add (server_id))
			return false;

		if (!last_response.getAccepted ())
			return false;

		this.responses++;

		if (this.responses >= this.quorum) {
			this.accepted = true;
			return true;
		}

		return false;
	}
}
