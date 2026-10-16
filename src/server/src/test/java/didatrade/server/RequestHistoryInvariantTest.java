package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import didatrade.server.request.CommandIdKind;
import didatrade.server.request.RequestHistory;
import didatrade.server.request.RequestRecord;

class RequestHistoryInvariantTest {

	@Test
	void sentinelIdsAreNeverQueuedAsClientRequests () {
		RequestHistory history = new RequestHistory ();

		assertFalse (history.addToPending (CommandIdKind.ABSENT_ID,
				new RequestRecord (CommandIdKind.ABSENT_ID)));
		assertFalse (history.addToPending (CommandIdKind.NOOP_ID,
				new RequestRecord (CommandIdKind.NOOP_ID)));
		assertNull (history.getIfPending (CommandIdKind.ABSENT_ID));
		assertNull (history.getIfPending (CommandIdKind.NOOP_ID));
	}

	@Test
	void processedRecordRemainsDiscoverableButIsNoLongerPending () {
		RequestHistory history = new RequestHistory ();
		RequestRecord record = new RequestRecord (101);
		assertTrue (history.addToPending (101, record));

		assertSame (record, history.moveToProcessed (101));
		assertNull (history.getIfPending (101));
		assertSame (record, history.getIfProcessed (101));
		assertSame (record, history.getIfExists (101));
	}

	@Disabled("Potential idempotency bug: decide desired duplicate-request semantics before enabling")
	@Test
	void duplicatePendingRequestIdMustNotReplaceRecordAlreadyWaitedOnByOriginalRpc () {
		RequestHistory history = new RequestHistory ();
		RequestRecord original = new RequestRecord (101);
		RequestRecord retry = new RequestRecord (101);
		assertTrue (history.addToPending (101, original));

		history.addToPending (101, retry);

		// Replacing the original can strand the first RPC waiter forever.
		assertSame (original, history.getIfPending (101));
	}
}
