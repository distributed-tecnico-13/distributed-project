package didatrade.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;

public class PhaseOneResponseProcessor
		extends
			GenericResponseProcessor<DidaTradePaxos.PhaseOneReply> {

	private final ConfigurationScheduler scheduler;
	private final int high_ballot;

	private final int quorum;
	private final Set<Integer> acceptors;
	private final Set<Integer> responders;

	private boolean accepted;
	private int response_count;
	private final Map<Integer, InstanceResponse> responses_per_instance;

	private int maxballot;

	public PhaseOneResponseProcessor(ConfigurationScheduler s, int h) {
		this.scheduler = s;
		this.high_ballot = h;

		this.quorum = this.scheduler.quorum (this.high_ballot);

		this.acceptors = new HashSet<> (
				this.scheduler.acceptors (this.high_ballot));
		this.responders = new HashSet<> ();

		this.accepted = false;
		this.response_count = 0;
		this.responses_per_instance = new HashMap<Integer, InstanceResponse> ();

		this.maxballot = -1;
	}

	public boolean getAccepted () {
		return this.accepted;
	}

	public int getMaxballot () {
		return this.maxballot;
	}

	public synchronized int[] instances () {
		return this.responses_per_instance.keySet ().stream ()
				.mapToInt (Integer::intValue)
				.sorted ()
				.toArray ();
	}

	public synchronized int commandFor (int instance) {
		InstanceResponse response = this.responses_per_instance.get (instance);
		if (response == null)
			return -1;
		return response.getValue ();
	}

	public synchronized int ballotFor (int instance) {
		InstanceResponse response = this.responses_per_instance.get (instance);
		if (response == null)
			return -1;
		return response.getBallot ();
	}

	public synchronized boolean onNext (
			ArrayList<DidaTradePaxos.PhaseOneReply> all_responses,
			DidaTradePaxos.PhaseOneReply last_response) {

		int server_id = last_response.getServerid ();

		if (!this.acceptors.contains (server_id))
			return false;

		if (!this.responders.add (server_id))
			return false;

		if (!last_response.getAccepted ()) {
			this.accepted = false;

			if (last_response.getMaxballot () > this.maxballot)
				this.maxballot = last_response.getMaxballot ();

			return true;
		}

		for (DidaTradePaxos.PhaseOneResponseLogObject response : last_response.getResponseLogObjectsList ()) {
			InstanceResponse instance_response = this.responses_per_instance.get (response.getInstance ());
			if (instance_response == null) {
				instance_response = new InstanceResponse (response.getValballot (), response.getValue ());
				this.responses_per_instance.put (response.getInstance (), instance_response);
			} else if (instance_response.getBallot () < response.getValballot ()) {
				instance_response.setBallot (response.getValballot ());
				instance_response.setValue (response.getValue ());
			}
		}

		this.response_count++;

		if (this.response_count >= this.quorum) {
			this.accepted = true;
			return true;
		}

		return false;
	}
}
