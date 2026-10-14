package didatrade.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import didatrade.DidaTradePaxos;

public class FastRoundResponseProcessor extends GenericResponseProcessor<DidaTradePaxos.OpenFastReply> {
	private final int quorum;
	private final Set<Integer> acceptors;
	private final Set<Integer> responders;
	private int accepted_count;
	private int maxballot;
	private boolean accepted;

	public FastRoundResponseProcessor(int quorum, List<Integer> acceptor_ids) {
		this.quorum = quorum;
		this.acceptors = new HashSet<> (acceptor_ids);
		this.responders = new HashSet<> ();
		this.accepted_count = 0;
		this.maxballot = -1;
		this.accepted = false;
	}

	public boolean getAccepted () {
		return this.accepted;
	}

	public int getMaxballot () {
		return this.maxballot;
	}

	@Override
	public synchronized boolean onNext (
			ArrayList<DidaTradePaxos.OpenFastReply> all_responses,
			DidaTradePaxos.OpenFastReply last_response) {
		int server_id = last_response.getServerid ();
		if (!this.acceptors.contains (server_id) || !this.responders.add (server_id))
			return false;

		this.maxballot = Math.max (this.maxballot, last_response.getMaxballot ());
		if (!last_response.getAccepted ()) {
			if (this.maxballot > last_response.getBallot ()) {
				this.accepted = false;
				return true;
			}
			return false;
		}

		this.accepted_count++;
		if (this.accepted_count >= this.quorum)
			this.accepted = true;

		// All openfast RPCs are sent, but a Fast quorum is sufficient to proceed.
		return this.accepted;
	}
}
