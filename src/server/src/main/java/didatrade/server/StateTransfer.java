package didatrade.server;

import static didatrade.configs.ScheduleC.MEMBERSHIP_CHANGE_BALLOT;

import didatrade.DidaTradePaxos;

/** Installs a prefix already confirmed by the non-Byzantine ballot leader. */
final class StateTransfer {
	private StateTransfer() {
	}

	static boolean install (DidaTradeServerState state, DidaTradePaxos.InstallStateRequest request) {
		int ballot = request.getBallot ();
		synchronized (state) {
			if (ballot < state.getCurrentBallot ())
				return false;
			state.setCurrentBallot (ballot);
			// Installation can finish before execution; the executor waits for delayed client commands.
			for (DidaTradePaxos.PhaseOneResponseLogObject e : request.getEntriesList ()) {
				PaxosInstance local = state.paxos_log.testAndSetEntry (e.getInstance ());
				if (state.scheduler.isacceptor (state.my_id, ballot)) {
					local.accept (ballot, e.getValue ());
					local.noteClassicAcceptance (ballot, e.getValue ());
				}
				local.decide (ballot, e.getValue ());
			}
			// The leader sends this only after recovery; it waits for a NEW quorum
			// to store the marker before acknowledging completion to the console.
			if (state.scheduler.dynamicMembership () && ballot >= MEMBERSHIP_CHANGE_BALLOT)
				state.noteMembershipTransferred ();
			return true;
		}
	}
}
