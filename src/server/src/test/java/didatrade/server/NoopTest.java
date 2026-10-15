package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import didatrade.core.TradeManager;
import didatrade.server.request.CommandIdHelper;
import didatrade.server.request.DidaTradeAction;
import didatrade.server.request.DidaTradeCommand;
import didatrade.server.request.RequestHistory;
import didatrade.server.request.RequestRecord;

class NoopTest {

	@Test
	void severalHolesHoldTheSameNoop () {
		PaxosLog log = new PaxosLog ();

		assertTrue (log.fillNoop (0));
		assertTrue (log.fillNoop (1));
		assertTrue (log.fillNoop (3));
		assertTrue (log.fillNoop (1));

		assertEquals (CommandIdHelper.NOOP_VALUE, log.getEntry (0).getProposedRequestId ());
		assertEquals (CommandIdHelper.NOOP_VALUE, log.getEntry (1).getProposedRequestId ());
		assertNull (log.getEntry (2));
		assertEquals (CommandIdHelper.NOOP_VALUE, log.getEntry (3).getProposedRequestId ());
	}

	@Test
	void fillNoopDoesNotReplaceAClientRequest () {
		PaxosLog log = new PaxosLog ();
		assertTrue (log.adopt (2, 301));

		assertFalse (log.fillNoop (2));
		assertEquals (301, log.getEntry (2).getProposedRequestId ());
		assertTrue (log.fillNoop (0));
		assertTrue (log.fillNoop (1));
		assertTrue (log.adopt (4, 301));
		assertEquals (301, log.getEntry (4).getProposedRequestId ());
		assertEquals (301, log.getEntry (2).getProposedRequestId ());
	}

	@Test
	void phaseOneClientRequestReplacesAFilledNoop () {
		PaxosLog log = new PaxosLog ();
		assertTrue (log.fillNoop (2));

		assertTrue (log.adopt (2, 301));
		assertEquals (301, log.getEntry (2).getProposedRequestId ());
	}

	@Test
	void adoptOverridesALocalClientCandidateWithARecoveredNoop () {
		PaxosLog log = new PaxosLog ();
		assertTrue (log.adopt (2, 301));

		// Phase 1 for this ballot discovered noop as the highest-accepted value at slot 2 -
		// that is a Paxos-mandated recovery, unlike fillNoop's speculative fill, so it must
		// override the local candidate.
		assertTrue (log.adopt (2, CommandIdHelper.NOOP_VALUE));
		assertEquals (CommandIdHelper.NOOP_VALUE, log.getEntry (2).getProposedRequestId ());
	}

	@Test
	void pendingRequestSkipsNoopSlots () {
		PaxosLog log = new PaxosLog ();
		RequestHistory history = new RequestHistory ();
		log.fillNoop (0);
		log.fillNoop (1);
		history.addToPending (100, new RequestRecord (100));

		assertEquals (CommandIdHelper.NOOP_VALUE,
				log.assignPendingRequest (0, history).getProposedRequestId ());
		assertEquals (CommandIdHelper.NOOP_VALUE,
				log.assignPendingRequest (1, history).getProposedRequestId ());
		assertEquals (100, log.assignPendingRequest (2, history).getProposedRequestId ());
		history.addToPending (200, new RequestRecord (200));
		assertEquals (200, log.assignPendingRequest (3, history).getProposedRequestId ());
	}

	@Test
	void noopSlotCanBeClaimedAndRetried () {
		PaxosInstance empty = new PaxosInstance (0);
		assertFalse (empty.tryClaim (5, 0));

		PaxosInstance noop = new PaxosInstance (1);
		noop.setProposedRequestId (CommandIdHelper.NOOP_VALUE);
		assertTrue (noop.tryClaim (5, 1000));
		long deadline = noop.releaseClaim (5, TimeUnit.MILLISECONDS.toNanos (200));

		assertFalse (noop.tryClaim (5, deadline - 1));
		assertEquals (1, noop.retryDelayMillis (5, deadline - 1));
		assertTrue (noop.tryClaim (5, deadline));
	}

	@Test
	void historyIgnoresANoopId () {
		RequestHistory history = new RequestHistory ();
		history.addToPending (CommandIdHelper.NOOP_VALUE, new RequestRecord (CommandIdHelper.NOOP_VALUE));
		history.addToPending (100, new RequestRecord (100));

		assertNull (history.getIfPending (CommandIdHelper.NOOP_VALUE));
		assertEquals (100, history.getIfPending (100).getId ());
	}

	@Test
	void decidedNoopIsTheCommandId () {
		PaxosInstance accepted = new PaxosInstance (0);
		accepted.accept (5, CommandIdHelper.NOOP_VALUE);
		assertEquals (CommandIdHelper.NOOP_VALUE, accepted.getAcceptedValue ());
		assertFalse (accepted.isNoop ());
		accepted.decide (5, CommandIdHelper.NOOP_VALUE);
		assertTrue (accepted.isNoop ());

		PaxosInstance chosen = new PaxosInstance (1);
		chosen.decide (5, CommandIdHelper.NOOP_VALUE);
		assertTrue (chosen.isNoop ());
		assertEquals (0, chosen.getAcceptedValue ());
	}

	@Test
	void learnedNoopNeedsNoLocalAccept () {
		PaxosInstance entry = new PaxosInstance (0);

		assertFalse (entry.recordLearn (5, CommandIdHelper.NOOP_VALUE, 0, 2));
		assertTrue (entry.recordLearn (5, CommandIdHelper.NOOP_VALUE, 1, 2));
		assertTrue (entry.isNoop ());
		assertEquals (CommandIdHelper.NOOP_VALUE, entry.getCommandId ());
		assertEquals (0, entry.getAcceptedValue ());
	}

	@Test
	void decidedNoopUnblocksTheFollowingCommand () {
		DidaTradeServerState state = newState ();
		RecordingTradeManager manager = new RecordingTradeManager ();
		state.trade_manager = manager;
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (state);
		addRequest (state, 101);
		state.paxos_log.testAndSetEntry (0).decide (5, CommandIdHelper.NOOP_VALUE);
		state.paxos_log.testAndSetEntry (1).decide (5, 101);

		assertEquals (2, executor.drain ());
		assertEquals (List.of (101), manager.executed_users);
		assertTrue (state.paxos_log.getEntry (0).isExecuted ());
		assertTrue (state.paxos_log.getEntry (1).isExecuted ());
		assertEquals (2, executor.getNextInstanceToExecute ());
	}

	@Test
	void learnedNoopUnblocksTheFollowingCommand () {
		DidaTradeServerState state = newState ();
		RecordingTradeManager manager = new RecordingTradeManager ();
		state.trade_manager = manager;
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (state);
		addRequest (state, 101);
		PaxosInstance hole = state.paxos_log.testAndSetEntry (0);
		hole.recordLearn (5, CommandIdHelper.NOOP_VALUE, 0, 2);
		hole.recordLearn (5, CommandIdHelper.NOOP_VALUE, 1, 2);
		state.paxos_log.testAndSetEntry (1).decide (5, 101);

		assertEquals (2, executor.drain ());
		assertEquals (List.of (101), manager.executed_users);
		assertTrue (hole.isExecuted ());
	}

	@Test
	void missingPayloadStillBlocksAfterANoop () {
		DidaTradeServerState state = newState ();
		OrderedPaxosExecutor executor = new OrderedPaxosExecutor (state);
		state.paxos_log.testAndSetEntry (0).decide (5, CommandIdHelper.NOOP_VALUE);
		state.paxos_log.testAndSetEntry (1).decide (5, 101);

		assertEquals (1, executor.drain ());
		assertEquals (1, executor.getNextInstanceToExecute ());
		assertFalse (state.paxos_log.getEntry (1).isExecuted ());

		addRequest (state, 101);
		assertEquals (1, executor.drain ());
		assertEquals (2, executor.getNextInstanceToExecute ());
	}

	private static DidaTradeServerState newState () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.paxos_log = new PaxosLog ();
		state.req_history = new RequestHistory ();
		state.trade_manager = new TradeManager ();
		return state;
	}

	private static void addRequest (DidaTradeServerState state, int id) {
		state.req_history.addToPending (id,
				new RequestRecord (id, new DidaTradeCommand (DidaTradeAction.ADDUSER, id, 10, 10)));
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
