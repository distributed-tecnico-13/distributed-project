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
	private final Map<Integer, List<AcceptedObservation>> observations_per_instance;
	private final Map<Integer, InstanceResponse> responses_per_instance;
	private int maxballot;

	public PhaseOneResponseProcessor(ConfigurationScheduler s, int h) {
		this.scheduler = s;
		this.high_ballot = h;
		this.quorum = this.scheduler.quorum (this.high_ballot);
		this.acceptors = new HashSet<> (this.scheduler.acceptors (this.high_ballot));
		this.responders = new HashSet<> ();
		this.accepted = false;
		this.response_count = 0;
		this.observations_per_instance = new HashMap<> ();
		this.responses_per_instance = new HashMap<> ();
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
			this.observations_per_instance
					.computeIfAbsent (response.getInstance (), ignored -> new ArrayList<> ())
					.add (new AcceptedObservation (
							server_id, response.getValballot (), response.getValue ()));
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
		for (Map.Entry<Integer, List<AcceptedObservation>> instance : this.observations_per_instance.entrySet ()) {
			InstanceResponse resolution = this.resolveInstance (instance.getValue ());
			if (resolution == null)
				return false;
			resolved.put (instance.getKey (), resolution);
		}
		this.responses_per_instance.clear ();
		this.responses_per_instance.putAll (resolved);
		return true;
	}

	private InstanceResponse resolveInstance (List<AcceptedObservation> observations) {
		int highest_ballot = observations.stream ()
				.mapToInt (AcceptedObservation::ballot)
				.max ()
				.orElse (-1);
		if (highest_ballot < 0)
			return null;

		Map<Integer, Set<Integer>> voters_per_value = new HashMap<> ();
		for (AcceptedObservation observation : observations) {
			if (observation.ballot () != highest_ballot)
				continue;
			voters_per_value
					.computeIfAbsent (observation.value (), ignored -> new HashSet<> ())
					.add (observation.server_id ());
		}

		if (!this.scheduler.fastpaxos (highest_ballot)) {
			if (voters_per_value.size () != 1)
				return null;
			int value = voters_per_value.keySet ().iterator ().next ();
			return new InstanceResponse (highest_ballot, value);
		}

		int missing_acceptors = Math.max (0, this.acceptors.size () - this.responders.size ());
		List<Integer> possible_chosen_values = voters_per_value.entrySet ().stream ()
				.filter (entry -> entry.getValue ().size () + missing_acceptors >= this.quorum)
				.map (Map.Entry::getKey)
				.sorted ()
				.toList ();

		if (possible_chosen_values.size () > 1)
			return null;
		if (possible_chosen_values.size () == 1)
			return new InstanceResponse (highest_ballot, possible_chosen_values.get (0));

		// No value can still have reached a quorum in this Fast round. Recovery may
		// therefore choose any value. Pick the smallest reported command deterministically.
		int value = voters_per_value.keySet ().stream ()
				.min (Comparator.naturalOrder ())
				.orElse (-1);
		return new InstanceResponse (highest_ballot, value);
	}

	private static record AcceptedObservation(int server_id, int ballot, int value) {
	}
}
