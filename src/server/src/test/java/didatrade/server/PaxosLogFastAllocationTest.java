package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class PaxosLogFastAllocationTest {

	@Test
	void duplicateUnexecutedRequestReusesSameEntry () {
		PaxosLog log = new PaxosLog ();

		PaxosInstance first = log.assignFastRequest (100);
		PaxosInstance second = log.assignFastRequest (100);

		assertSame (first, second);
		assertEquals (0, first.getInstanceNb ());
		assertEquals (1, log.length ());
	}

	@Test
	void acceptedEntryIsNeverOverwrittenByAnotherFastRequest () {
		PaxosLog log = new PaxosLog ();
		PaxosInstance first = log.assignFastRequest (100);
		first.accept (0, 100);

		PaxosInstance second = log.assignFastRequest (200);

		assertNotSame (first, second);
		assertEquals (1, second.getInstanceNb ());
		assertEquals (100, first.getAcceptedSnapshot ().value);
	}

	@Test
	void decidedEntryIsNeverReused () {
		PaxosLog log = new PaxosLog ();
		PaxosInstance first = log.testAndSetEntry (0);
		first.decide (0, 100);

		PaxosInstance second = log.assignFastRequest (200);

		assertEquals (1, second.getInstanceNb ());
		assertEquals (100, first.getDecidedValue ());
	}

	@Test
	void reservedButNotYetAcceptedEntryIsNotStolen () {
		PaxosLog log = new PaxosLog ();
		PaxosInstance first = log.assignFastRequest (100);

		PaxosInstance second = log.assignFastRequest (200);

		assertEquals (0, first.getInstanceNb ());
		assertEquals (1, second.getInstanceNb ());
		assertEquals (100, first.getProposedRequestId ());
		assertEquals (200, second.getProposedRequestId ());
	}

	@Test
	void allocatorUsesFirstAvailableHole () {
		PaxosLog log = new PaxosLog ();
		log.testAndSetEntry (0).decide (0, 100);
		log.testAndSetEntry (2).decide (0, 300);

		PaxosInstance allocated = log.assignFastRequest (200);

		assertEquals (1, allocated.getInstanceNb ());
		assertEquals (200, allocated.getProposedRequestId ());
	}
}
