package didatrade.server;

import didatrade.server.request.DidaTradeAction;
import didatrade.server.request.DidaTradeCommand;
import didatrade.server.request.RequestRecord;

/** Only MainLoop calls this executor; PaxosInstance still synchronizes with RPC threads. */
final class OrderedPaxosExecutor {
	private final DidaTradeServerState server_state;
	private int next_instance_to_execute;

	OrderedPaxosExecutor(DidaTradeServerState state) {
		this.server_state = state;
		this.next_instance_to_execute = 0;
	}

	int getNextInstanceToExecute () {
		return this.next_instance_to_execute;
	}

	int drain () {
		int executed_count = 0;
		while (true) {
			PaxosInstance entry = this.server_state.paxos_log.getEntry (this.next_instance_to_execute);
			if (entry == null || !entry.isDecided ())
				return executed_count;

			synchronized (this.server_state) {
				if (!entry.isExecuted () && !this.executeCommand (entry))
					return executed_count;
				entry.markExecuted ();
				this.server_state.setLastExecutedInstance (this.next_instance_to_execute);
			}

			this.next_instance_to_execute++;
			executed_count++;
		}
	}

	private boolean executeCommand (PaxosInstance entry) {
		if (entry.isNoop ())
			return true;
		int command_id = entry.getCommandId ();
		RequestRecord record = this.server_state.req_history.getIfExists (command_id);
		if (record == null)
			return false; // A later client arrival wakes MainLoop; never skip this slot.
		if (this.server_state.req_history.getIfProcessed (command_id) != null)
			return true;

		boolean result = this.executeAction (record.getRequest (), entry.getInstanceNb ());

		record.setResponse (result);
		this.server_state.req_history.moveToProcessed (command_id);
		return true;
	}

	private boolean executeAction (DidaTradeCommand command, int instance) {
		boolean result = false;
		command.setInstance (instance);
		switch (command.getAction ()) {
			case DidaTradeAction.POPULATE :
				result = this.server_state.trade_manager.populate (command.getQuantity ());
				break;
			case DidaTradeAction.ADDUSER :
				result = this.server_state.trade_manager.add_user (
						command.getUserId (), command.getQuantity (), command.getStock ());
				break;
			case DidaTradeAction.SELL :
				result = this.server_state.trade_manager.sell (
						command.getUserId (), command.getQuantity ());
				break;
			case DidaTradeAction.BUY :
				result = this.server_state.trade_manager.acquire (
						command.getUserId (), command.getQuantity ());
				break;
			case DidaTradeAction.DUMP :
				this.server_state.trade_manager.dump ();
				result = true;
				break;
			default :
				System.err.println ("*** Unknown command ****");
				break;
		}
		return result;
	}
}
