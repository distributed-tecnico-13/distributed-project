package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import didatrade.core.TradeManager;

class OrderedPaxosExecutorTest {

	@Test
	void stopsAtFirstUndecidedSlot () {
		DidaTradeServerState state = newState ();
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (state);
		addRequest (state, 201);
		decide (state, 1, 201);

		assertEquals (0, executor.drain ());
		assertEquals (0, executor.getNextInstanceToExecute ());
		assertFalse (state.paxos_log.getEntry (1).isExecuted ());
	}

	@Test
	void drainsOutOfOrderDecisionsInSlotOrder () {
		DidaTradeServerState state = newState ();
		RecordingTradeManager manager = new RecordingTradeManager ();
		state.trade_manager = manager;
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (state);
		for (int id : List.of (101, 201, 301))
			addRequest (state, id);

		decide (state, 2, 301);
		decide (state, 1, 201);
		assertEquals (0, executor.drain ());
		decide (state, 0, 101);
		assertEquals (3, executor.drain ());
		assertEquals (List.of (101, 201, 301), manager.executed_users);
		assertEquals (3, executor.getNextInstanceToExecute ());
	}

	@Test
	void duplicateCommandExecutesOnlyOnce () {
		DidaTradeServerState state = newState ();
		RecordingTradeManager manager = new RecordingTradeManager ();
		state.trade_manager = manager;
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (state);
		addRequest (state, 101);
		decide (state, 0, 101);
		decide (state, 1, 101);

		assertEquals (2, executor.drain ());
		assertEquals (0, executor.drain ());
		assertEquals (List.of (101), manager.executed_users);
		assertTrue (state.paxos_log.getEntry (0).isExecuted ());
		assertTrue (state.paxos_log.getEntry (1).isExecuted ());
	}

	@Test
	void missingPayloadBlocksUntilItArrives () {
		DidaTradeServerState state = newState ();
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (state);
		addRequest (state, 201);
		decide (state, 0, 101);
		decide (state, 1, 201);
		assertEquals (0, executor.drain ());
		assertEquals (0, executor.getNextInstanceToExecute ());

		// DidaTradeMainServiceImpl performs these two calls on late arrival.
		addRequest (state, 101);
		assertEquals (2, executor.drain ());
		assertEquals (2, executor.getNextInstanceToExecute ());
	}

	@Test
	void responseIsSetOnlyAfterExecution () {
		DidaTradeServerState state = newState ();
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (state);
		RequestRecord record = addRequest (state, 101);
		assertFalse (record.getResponseValue ());
		decide (state, 0, 101);
		assertEquals (1, executor.drain ());
		assertTrue (record.getResponseValue ());
		assertTrue (state.req_history.getIfProcessed (101) == record);
	}

	private static DidaTradeServerState newState () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.paxos_log = new PaxosLog ();
		state.req_history = new RequestHistory ();
		state.trade_manager = new TradeManager ();
		return state;
	}

	private static RequestRecord addRequest (DidaTradeServerState state, int id) {
		RequestRecord record = new RequestRecord (id,
				new DidaTradeCommand (DidaTradeAction.ADDUSER, id, 10, 10));
		state.req_history.addToPending (id, record);
		return record;
	}

	private static void decide (DidaTradeServerState state, int slot, int id) {
		state.paxos_log.testAndSetEntry (slot).decide (0, id);
	}

	private static final class RecordingTradeManager extends TradeManager {
		final List<Integer> executed_users = new ArrayList<> ();

		@Override
		public synchronized boolean add_user (int user_id, int cash, int stock) {
			this.executed_users.add (user_id);
			return super.add_user (user_id, cash, stock);
		}
	}
}
