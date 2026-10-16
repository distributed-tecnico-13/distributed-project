package didatrade.server;

public final class FastPaxosAcceptor {
	private FastPaxosAcceptor() {
	}

	public static boolean tryAcceptRequest (DidaTradeServerState state, int request_id) {
		DidaTradeServerState.FastVoteState vote = state.processFastVote (request_id);
		if (!vote.accepted)
			return false;
		PaxosLearnerNotifier.notifyAccepted (
				state, vote.instance, vote.ballot, request_id);
		if (state.main_loop != null)
			state.main_loop.wakeup ();
		return true;
	}

	public static int acceptPendingRequests (DidaTradeServerState state) {
		int accepted = 0;
		for (int request_id : state.req_history.getPendingRequestIds ()) {
			if (tryAcceptRequest (state, request_id))
				accepted++;
		}
		return accepted;
	}
}
