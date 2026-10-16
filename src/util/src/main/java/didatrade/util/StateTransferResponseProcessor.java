package didatrade.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import didatrade.DidaTradePaxos;

/**
 * Done once a classic quorum of acceptors installed the state, or any replica reports a newer
 * ballot.
 */
public class StateTransferResponseProcessor extends GenericResponseProcessor<DidaTradePaxos.InstallStateReply> {
	private final int ballot;
	private final int quorum;
	private final Set<Integer> acceptors;
	private final Set<Integer> acknowledgements = new HashSet<> ();
	private int maxballot = -1;

	public StateTransferResponseProcessor(int ballot, int quorum, List<Integer> acceptor_ids) {
		this.ballot = ballot;
		this.quorum = quorum;
		this.acceptors = new HashSet<> (acceptor_ids);
	}

	public synchronized boolean getAccepted () {
		return this.maxballot <= this.ballot && this.acknowledgements.size () >= this.quorum;
	}

	public synchronized int getMaxballot () {
		return this.maxballot;
	}

	@Override
	public synchronized boolean onNext (
			ArrayList<DidaTradePaxos.InstallStateReply> all_responses,
			DidaTradePaxos.InstallStateReply last_response) {
		// Transfers reach every learner; even a retired acceptor can report
		// a newer ballot, but only current acceptors count towards the quorum.
		this.maxballot = Math.max (this.maxballot, last_response.getMaxballot ());
		if (this.maxballot > this.ballot)
			return true;
		if (last_response.getAccepted () && this.acceptors.contains (last_response.getServerid ()))
			this.acknowledgements.add (last_response.getServerid ());
		return this.getAccepted ();
	}
}
