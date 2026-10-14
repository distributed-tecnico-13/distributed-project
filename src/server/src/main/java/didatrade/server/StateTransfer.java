package didatrade.server;

import java.util.HashMap;
import java.util.Map;
import didatrade.DidaTradePaxos;

/** Installs a prefix already confirmed by the non-Byzantine ballot leader. */
final class StateTransfer {
	private StateTransfer() {
	}

	static boolean install (DidaTradeServerState state, DidaTradePaxos.InstallStateRequest request) {
		int ballot = request.getBallot ();
		synchronized (state) {
			if (!state.scheduler.validBallot (ballot) || ballot < state.getCurrentBallot ()
					|| request.getSenderid () != state.scheduler.leader (ballot)
					|| request.getThroughinstance () < -1
					|| request.getEntriesCount () != (long) request.getThroughinstance () + 1)
				return false;
			Map<Integer, DidaTradeCommand> commands = new HashMap<> ();
			try {
				for (DidaTradePaxos.CommandPayload p : request.getCommandsList ()) {
					DidaTradeCommand c = CommandTransfer.decode (p);
					DidaTradeCommand previous = commands.putIfAbsent (p.getRequestid (), c);
					if (previous != null && !CommandTransfer.same (previous, c))
						return false;
					RequestRecord existing = state.req_history.getIfExists (p.getRequestid ());
					if (existing != null && !CommandTransfer.same (existing.getRequest (), c))
						return false;
				}
			} catch (IllegalArgumentException e) {
				return false;
			}
			for (int i = 0; i < request.getEntriesCount (); i++) {
				DidaTradePaxos.PhaseOneResponseLogObject e = request.getEntries (i);
				if (e.getInstance () != i || e.getValballot () != ballot
						|| (!CommandIdHelper.isNoop (e.getValue ()) && !commands.containsKey (e.getValue ())))
					return false;
				PaxosInstance local = state.paxos_log.getEntry (i);
				if (local != null && ((local.isDecided () && local.getCommandId () != e.getValue ())
						|| (local.getWriteBallot () == ballot && local.getAcceptedValue () != e.getValue ())))
					return false;
			}
			state.setCurrentBallot (ballot);
			commands.forEach ( (id, c) -> state.req_history.installIfAbsent (id, c));
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
			if (state.scheduler.dynamicMembership () && ballot >= 2)
				state.noteMembershipTransferred ();
			return true;
		}
	}
}
