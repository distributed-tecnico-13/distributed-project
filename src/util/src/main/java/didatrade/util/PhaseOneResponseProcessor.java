package didatrade.util;

import static didatrade.configs.ScheduleC.MEMBERSHIP_CHANGE_BALLOT;

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
	private final List<Integer> required_ballots;
	private boolean membership_transferred;
	private final Set<Integer> acceptors;
	private final Set<Integer> responders;

	private boolean accepted;

	private final Map<Integer, AcceptedResponseObserver> observations_per_instance;
	private final Map<Integer, InstanceResponse> resolved_per_instance;
	private int maxballot;

	public PhaseOneResponseProcessor(ConfigurationScheduler s, int h) {
		this (s, h, h);
	}

	public PhaseOneResponseProcessor(ConfigurationScheduler s, int h, int low) {
		this.scheduler = s;
		this.required_ballots = new ArrayList<> ();
		this.acceptors = new HashSet<> ();
		Set<Set<Integer>> groups = new HashSet<> ();
		for (int ballot = low; ballot <= h; ballot++) {
			Set<Integer> group = new HashSet<> (s.acceptors (ballot));
			if (groups.add (group))
				this.required_ballots.add (ballot);
			this.acceptors.addAll (group);
		}
		this.responders = new HashSet<> ();
		this.observations_per_instance = new HashMap<> ();
		this.resolved_per_instance = new HashMap<> ();
		this.maxballot = -1;
	}

	public synchronized boolean membershipTransferred () {
		return this.membership_transferred;
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

		// A new acceptor's atomic snapshot includes the entire transferred
		// prefix in the new configuration. A retired learner's marker carries no such votes.
		this.membership_transferred |= this.scheduler.dynamicMembership ()
				&& this.scheduler.isacceptor (server_id, MEMBERSHIP_CHANGE_BALLOT) && last_response.getMembershiptransferred ();
		for (DidaTradePaxos.PhaseOneResponseLogObject response : last_response.getResponseLogObjectsList ()) {
			if (!this.scheduler.isacceptor (server_id, response.getValballot ()))
				continue;
			AcceptedResponseObserver current_observation = this.observations_per_instance.get (response.getInstance ());
			if (current_observation == null) {
				this.observations_per_instance.put (
						response.getInstance (),
						new AcceptedResponseObserver (response.getValballot (), response.getValue ()));
			} else {
				current_observation.observe (response.getValballot (), response.getValue ());
			}
		}

		for (int ballot : this.required_ballots) {
			long count = this.scheduler.acceptors (ballot).stream ().filter (this.responders::contains).count ();
			if (count < this.scheduler.recoveryQuorum (ballot))
				return false;
		}

		if (!this.resolveInstances ())
			return false;

		this.accepted = true;
		return true;
	}

	private boolean resolveInstances () {
		Map<Integer, InstanceResponse> resolved = new HashMap<> ();
		for (Map.Entry<Integer, AcceptedResponseObserver> instance : this.observations_per_instance.entrySet ()) {
			// With that certificate, any slot seen ONLY in the old configuration
			// is an unchosen tail; every old decision is also present in the new configuration.
			if (this.membership_transferred && instance.getValue ().ballot < MEMBERSHIP_CHANGE_BALLOT)
				continue;
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

		int missing_acceptors = (int) this.scheduler.acceptors (observations.ballot).stream ()
				.filter (id -> !this.responders.contains (id)).count ();
		List<Integer> possible_chosen_values = observations.value_count.entrySet ().stream ()
				.filter (entry -> entry.getValue () + missing_acceptors >= this.scheduler.fastQuorum (observations.ballot))
				.map (Map.Entry::getKey)
				.sorted ()
				.toList ();

		// If multiple reported values could have reached the Fast quorum, the
		// coordinator cannot safely recover this slot until further replies arrive.
		// Use the origin schedule's Fast threshold. Majority-only schedules may
		// need additional replies beyond their initial recovery quorum.
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
