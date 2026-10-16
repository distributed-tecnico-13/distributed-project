package didatrade.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import didatrade.DidaTradePaxos;

/**
 * Done once a classic quorum accepted, or as soon as one acceptor rejects (its reply carries the
 * higher ballot).
 */
public class PhaseTwoResponseProcessor extends GenericResponseProcessor<DidaTradePaxos.PhaseTwoReply> {
	private boolean accepted;
	private int maxballot;
	private final int quorum;
	private int accepted_count;
	private final Set<Integer> acceptors;
	private final Set<Integer> responders;

	public PhaseTwoResponseProcessor(int q, List<Integer> acceptor_ids) {
		this.accepted = false;
		this.maxballot = 0;
		this.quorum = q;
		this.accepted_count = 0;
		this.acceptors = new HashSet<> (acceptor_ids);
		this.responders = new HashSet<> ();
	}

	public boolean getAccepted () {
		return this.accepted;
	}

	public int getMaxballot () {
		return this.maxballot;
	}

	public synchronized boolean onNext (ArrayList<DidaTradePaxos.PhaseTwoReply> all_responses, DidaTradePaxos.PhaseTwoReply last_response) {
		int server_id = last_response.getServerid ();
		if (!this.acceptors.contains (server_id) || !this.responders.add (server_id))
			return false;
		if (last_response.getAccepted () == false) {
			this.accepted = false;
			if (last_response.getMaxballot () > this.maxballot)
				this.maxballot = last_response.getMaxballot ();
			return true;
		}
		this.accepted_count++;
		if (this.accepted_count >= this.quorum) {
			this.accepted = true;
			return true;
		}
		return false;
	}
}
