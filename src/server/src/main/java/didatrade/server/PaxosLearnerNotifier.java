package didatrade.server;

/** Acceptors announce their votes to the learners (Learn RPC, sent by PeerRpc). */
public final class PaxosLearnerNotifier {
	private PaxosLearnerNotifier() {
	}

	public static void notifyAccepted (
			DidaTradeServerState state, int instance, int ballot, int value) {
		notifyAccepted (state, instance, ballot, value, false);
	}

	// Fire-and-forget, called after the locked state change. classic marks
	// a Phase 2 accept (classic quorum); otherwise it is a Fast vote (Fast quorum).
	public static void notifyAccepted (
			DidaTradeServerState state, int instance, int ballot, int value, boolean classic) {
		state.peer_rpc.learn (state.scheduler.learners (ballot), state.my_id, instance, ballot, value, classic);
	}
}
