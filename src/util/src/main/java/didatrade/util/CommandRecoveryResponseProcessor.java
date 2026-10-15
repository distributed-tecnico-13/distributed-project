package didatrade.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import didatrade.DidaTradePaxos;

/** Command availability is independent of the quorum that chose their IDs. */
public class CommandRecoveryResponseProcessor extends GenericResponseProcessor<DidaTradePaxos.FetchCommandsReply> {
	private final Set<Integer> required;
	private final Map<Integer, DidaTradePaxos.CommandPayload> commands = new HashMap<> ();
	private boolean conflicting;

	public CommandRecoveryResponseProcessor(Set<Integer> required) {
		this.required = new HashSet<> (required);
	}

	public synchronized boolean getAccepted () {
		return !this.conflicting && this.commands.keySet ().containsAll (this.required);
	}

	public synchronized List<DidaTradePaxos.CommandPayload> getCommands () {
		return List.copyOf (this.commands.values ());
	}

	@Override
	public synchronized boolean onNext (ArrayList<DidaTradePaxos.FetchCommandsReply> all,
			DidaTradePaxos.FetchCommandsReply last) {
		for (DidaTradePaxos.CommandPayload command : last.getCommandsList ()) {
			if (!this.required.contains (command.getRequestid ()))
				continue;
			DidaTradePaxos.CommandPayload previous = this.commands.putIfAbsent (command.getRequestid (), command);
			if (previous != null && !previous.equals (command))
				this.conflicting = true;
		}
		return this.conflicting || this.getAccepted ();
	}
}
