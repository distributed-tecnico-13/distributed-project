package didatrade.server;

/**
 * Fast Paxos acceptor path: votes for client requests directly, without a leader proposal. The
 * vote itself is decided under the state lock (fastVote); this class does the follow-up outside it.
 */
public final class FastPaxosAcceptor {
	private FastPaxosAcceptor() {
	}

	// Votes for the request in a free slot (fastVote, under the state lock), then tells the learners
	// and wakes the MainLoop outside the lock. False if no Fast round is open.
	public static boolean tryAcceptRequest (DidaTradeServerState state, int request_id) {
		DidaTradeServerState.FastVoteState vote = state.fastVote (request_id);
		if (!vote.accepted)
			return false;
		PaxosLearnerNotifier.notifyAccepted (
				state, vote.instance, vote.ballot, request_id);
		state.wakeMainLoop ();
		return true;
	}

	// Called when this replica opens a Fast round (openfast). Requests can arrive
	// before the fast round is actually opened, so must be accepted now.
	public static int acceptPendingRequests (DidaTradeServerState state) {
		int accepted = 0;
		for (int request_id : state.req_history.getPendingRequestIds ()) {
			if (tryAcceptRequest (state, request_id))
				accepted++;
		}
		return accepted;
	}
}
