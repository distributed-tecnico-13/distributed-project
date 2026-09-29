package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PhaseOneRecoveryCollisionTest {

	@Test
	void phaseOneRecoveredValueCannotBeDiscardedBecauseSameRequestIsLocallyAssignedElsewhere () {
		PaxosLog log = new PaxosLog ();
		assertTrue (log.adopt (6, 301)); // stale local candidate in a different slot

		// If Phase 1 says slot 5 must carry 301, local allocation policy must not veto Paxos.
		assertTrue (log.adopt (5, 301));
		assertEquals (301, log.getEntry (5).getProposedRequestId ());
	}
}
