package didatrade.server;

import static didatrade.configs.ScheduleC.MEMBERSHIP_CHANGE_BALLOT;

import didatrade.DidaTradePaxos;
import didatrade.server.request.DidaTradeCommand;

/** Installs a prefix already confirmed by the non-Byzantine ballot leader. */
public final class StateTransfer {
	private StateTransfer() {
	}

	// Acceptor side of PaxosProposer.finishTransfer: installs commands and decided entries
	// atomically. Rejects older ballots and conflicting commands.
	public static boolean install (DidaTradeServerState state, DidaTradePaxos.InstallStateRequest request) {
		int ballot = request.getBallot ();
		synchronized (state) {
			if (ballot < state.getCurrentBallot ())
				return false;
			state.setCurrentBallot (ballot);
			// The leader already recovered these slots, so they are accepted and decided right
			// away.
			// Installation can finish before execution; the executor waits for delayed client commands.
			for (DidaTradePaxos.PhaseOneResponseLogObject e : request.getEntriesList ()) {
				PaxosInstance local = state.paxos_log.testAndSetEntry (e.getInstance ());
				if (state.scheduler.isacceptor (state.my_id, ballot)) {
					local.accept (ballot, e.getValue ());
					local.noteClassicAcceptance (e.getValue ());
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
