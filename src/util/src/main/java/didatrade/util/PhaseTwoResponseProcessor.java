package didatrade.util;

import java.util.ArrayList;

import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc; // ignore-unused-import

public class PhaseTwoResponseProcessor extends GenericResponseProcessor<DidaTradePaxos.PhaseTwoReply> {
	private boolean accepted;
	private int maxballot;
	private int quorum;
	private int responses;

	public PhaseTwoResponseProcessor(int q) {
		this.accepted = false;
		this.maxballot = 0;
		this.quorum = q;
		this.responses = 0;
	}

	public boolean getAccepted () {
		return this.accepted;
	}

	public int getMaxballot () {
		return this.maxballot;
	}

	public synchronized boolean onNext (ArrayList<DidaTradePaxos.PhaseTwoReply> all_responses, DidaTradePaxos.PhaseTwoReply last_response) {
		if (last_response.getAccepted () == false) {
			this.accepted = false;
			if (last_response.getMaxballot () > this.maxballot)
				this.maxballot = last_response.getMaxballot ();
			return true;
		}
		this.responses++;
		if (this.responses >= this.quorum) {
			this.accepted = true;
			return true;
		}
		return false;
	}
}
