package didatrade.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
	private final Map<Integer, AcceptedResponseObserver> observations_per_instance;
	private final Map<Integer, InstanceResponse> resolved_per_instance;
	private int maxballot;

	public PhaseOneResponseProcessor(ConfigurationScheduler s, int h) {
		this.scheduler = s;
		this.high_ballot = h;
		this.quorum = this.scheduler.recoveryQuorum (this.high_ballot);
		this.acceptors = new HashSet<> (this.scheduler.acceptors (this.high_ballot));
		this.responders = new HashSet<> ();
		this.accepted = false;
		this.response_count = 0;
		this.observations_per_instance = new HashMap<> ();
		this.resolved_per_instance = new HashMap<> ();
		this.maxballot = -1;
	}

	public boolean getAccepted () {
		return this.accepted;
	}

	public int getMaxballot () {
		return this.maxballot;
	}

	public synchronized int[] instances () {
		return this.resolved_per_instance.keySet ().stream ()
				.mapToInt (Integer::intValue)
				.sorted ()
				.toArray ();
	}

	public synchronized int commandFor (int instance) {
		InstanceResponse response = this.resolved_per_instance.get (instance);
		if (response == null)
			return -1;
		return response.getValue ();
	}

	public synchronized int ballotFor (int instance) {
		InstanceResponse response = this.resolved_per_instance.get (instance);
		if (response == null)
			return -1;
		return response.getBallot ();
	}

	@Override
	public synchronized boolean onNext (
			ArrayList<DidaTradePaxos.PhaseOneReply> all_responses,
			DidaTradePaxos.PhaseOneReply last_response) {
		int server_id = last_response.getServerid ();
		if (!this.acceptors.contains (server_id))
			return false;
		if (!this.responders.add (server_id))
			return false;

		this.maxballot = Math.max (this.maxballot, last_response.getMaxballot ());
		if (!last_response.getAccepted ()) {
			this.accepted = false;
			return true;
		}

		for (DidaTradePaxos.PhaseOneResponseLogObject response : last_response.getResponseLogObjectsList ()) {
			AcceptedResponseObserver current_observation = this.observations_per_instance.get (response.getInstance ());
			if (current_observation == null) {
				this.observations_per_instance.put (
						response.getInstance (),
						new AcceptedResponseObserver (response.getValballot (), response.getValue ()));
			} else {
				current_observation.observe (response.getValballot (), response.getValue ());
			}
		}

		this.response_count++;
		if (this.response_count < this.quorum)
			return false;

		if (!this.resolveInstances ())
			return false;

		this.accepted = true;
		return true;
	}

	private boolean resolveInstances () {
		Map<Integer, InstanceResponse> resolved = new HashMap<> ();
		for (Map.Entry<Integer, AcceptedResponseObserver> instance : this.observations_per_instance.entrySet ()) {
			InstanceResponse resolution = this.resolveInstance (instance.getValue ());
			if (resolution == null)
				return false;
			resolved.put (instance.getKey (), resolution);
		}
		this.resolved_per_instance.clear ();
		this.resolved_per_instance.putAll (resolved);
		return true;
	}

	private InstanceResponse resolveInstance (AcceptedResponseObserver observations) {
		if (!this.scheduler.fastpaxos (observations.ballot)) {
			if (observations.value_count.keySet ().size () != 1)
				return null;
			int value = observations.value_count.keySet ().iterator ().next ();
			return new InstanceResponse (observations.ballot, value);
		}

		int missing_acceptors = Math.max (0, this.acceptors.size () - this.responders.size ());
		List<Integer> possible_chosen_values = observations.value_count.entrySet ().stream ()
				.filter (entry -> entry.getValue () + missing_acceptors
						>= this.scheduler.fastQuorum (observations.ballot))
				.map (Map.Entry::getKey)
				.sorted ()
				.toList ();

		// If multiple reported values could have reached the Fast quorum, the
		// coordinator cannot safely recover this slot until further replies arrive.
		// With five acceptors, a Fast decision requires four matching votes;
		// Phase 1 can complete after a recovery quorum of three responses.
		if (possible_chosen_values.size () > 1)
			return null;
		if (possible_chosen_values.size () == 1)
			return new InstanceResponse (observations.ballot, possible_chosen_values.get (0));

		// No value can still have reached a quorum in this Fast round. Recovery may
		// therefore choose any value. Pick the smallest reported command deterministically.
		int value = observations.value_count.keySet ().stream ()
				.min (Comparator.naturalOrder ())
				.orElse (-1);
		return new InstanceResponse (observations.ballot, value);
	}

	private static final class AcceptedResponseObserver {
		private int ballot;
		private final Map<Integer, Integer> value_count = new HashMap<> ();

		private AcceptedResponseObserver(int ballot, int value) {
			this.ballot = ballot;
			this.value_count.put (value, 1);
		}

		private void observe (int ballot, int value) {
			if (ballot > this.ballot) {
				this.ballot = ballot;
				this.value_count.clear ();
				this.value_count.put (value, 1);
			} else if (ballot == this.ballot) {
				this.value_count.merge (value, 1, Integer::sum);
			}
		}
	}
}
