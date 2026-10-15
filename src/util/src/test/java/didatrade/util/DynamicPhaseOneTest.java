package didatrade.util;

import static org.junit.jupiter.api.Assertions.*;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;

class DynamicPhaseOneTest {
	@Test
	void newAcceptorMarkerDisqualifiesOnlyObsoleteOldVotes () {
		PhaseOneResponseProcessor p = new PhaseOneResponseProcessor (new ConfigurationScheduler ('C'), 3);
		for (int id : new int[]{3, 4}) {
			int value = id * 100 + 1;
			assertFalse (p.onNext (new ArrayList<> (), DidaTradePaxos.PhaseOneReply.newBuilder ()
					.setServerid (id).setAccepted (true).setRequestballot (3).setMaxballot (3)
					.addResponseLogObjects (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
							.setInstance (1).setValue (value).setValballot (0))
					.build ()));
		}
		// This new acceptor certifies that slot 0 was transferred, and that
		// the old votes at slot 1 were not needed to preserve any decision.
		assertTrue (p.onNext (new ArrayList<> (), DidaTradePaxos.PhaseOneReply.newBuilder ()
				.setServerid (6).setAccepted (true).setRequestballot (3).setMaxballot (3).setMembershiptransferred (true)
				.addResponseLogObjects (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
						.setInstance (0).setValue (101).setValballot (2))
				.build ()));
		assertArrayEquals (new int[]{0}, p.instances ());
		assertEquals (101, p.commandFor (0));
	}

	@Test
	void retiredLearnerMarkerCannotCertifyTheNewAcceptorsHistory () {
		PhaseOneResponseProcessor p = new PhaseOneResponseProcessor (new ConfigurationScheduler ('C'), 3, 0);
		for (int id : new int[]{0, 1, 2, 5, 6}) {
			DidaTradePaxos.PhaseOneReply.Builder r = DidaTradePaxos.PhaseOneReply.newBuilder ()
					.setServerid (id).setAccepted (true).setRequestballot (3).setMaxballot (3)
					.setMembershiptransferred (id == 0);
			if (id < 3) {
				int value = id == 2 ? 201 : 101;
				r.addResponseLogObjects (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
						.setInstance (0).setValue (value).setValballot (0));
			}
			assertEquals (id == 6, p.onNext (new ArrayList<> (), r.build ()));
		}
		assertFalse (p.membershipTransferred ());
		assertTrue (p.getAccepted ());
		assertArrayEquals (new int[]{0}, p.instances ());
		assertEquals (101, p.commandFor (0));
	}

	@Test
	void transferMarkerCannotDiscardNewFastVotes () {
		PhaseOneResponseProcessor p = new PhaseOneResponseProcessor (new ConfigurationScheduler ('C'), 3);
		for (int id : new int[]{2, 3, 4}) {
			DidaTradePaxos.PhaseOneReply.Builder r = DidaTradePaxos.PhaseOneReply.newBuilder ()
					.setServerid (id).setAccepted (true).setRequestballot (3).setMaxballot (3)
					.setMembershiptransferred (id == 4);
			if (id != 4) {
				int value = id == 3 ? 201 : 101;
				r.addResponseLogObjects (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
						.setInstance (0).setValue (value).setValballot (2));
			}
			assertEquals (id == 4, p.onNext (new ArrayList<> (), r.build ()));
		}
		assertEquals (101, p.commandFor (0));
	}

	@Test
	void majorityOfUnionCannotReplaceAnOldMajority () {
		PhaseOneResponseProcessor p = new PhaseOneResponseProcessor (new ConfigurationScheduler ('C'), 2, 0);
		assertFalse (send (p, 3, false));
		assertFalse (send (p, 4, false));
		assertFalse (send (p, 5, false));
		assertFalse (send (p, 6, false));
		assertTrue (send (p, 0, true));
		assertEquals (101, p.commandFor (0));
	}
	@Test
	void newExclusiveMembersDoNotCountAsOldAcceptors () {
		PhaseOneResponseProcessor p = new PhaseOneResponseProcessor (new ConfigurationScheduler ('C'), 2, 0);
		assertFalse (send (p, 5, false));
		assertFalse (send (p, 6, false));
		assertFalse (send (p, 0, true));
		assertFalse (send (p, 1, true));
		assertTrue (send (p, 2, true));
		assertEquals (101, p.commandFor (0));
	}
	@Test
	void recoveredIdsDoNotRequireClientCommandsForPreparation () {
		PhaseOneResponseProcessor p = new PhaseOneResponseProcessor (new ConfigurationScheduler ('C'), 2, 0);
		assertFalse (send (p, 0, true));
		assertFalse (send (p, 1, true));
		assertFalse (send (p, 2, true));
		assertFalse (send (p, 5, false));
		assertTrue (send (p, 6, false));
		assertEquals (101, p.commandFor (0));
	}
	@Test
	void splitOldFastHistoryRecoversWithThreeOldAndThreeNewResponders () {
		PhaseOneResponseProcessor p = new PhaseOneResponseProcessor (new ConfigurationScheduler ('C'), 2, 0);
		for (int id : new int[]{5, 6, 0, 1, 2}) {
			DidaTradePaxos.PhaseOneReply.Builder r = DidaTradePaxos.PhaseOneReply.newBuilder ()
					.setServerid (id).setAccepted (true).setRequestballot (2).setMaxballot (2);
			if (id < 3) {
				int value = id == 1 ? 201 : 101;
				r.addResponseLogObjects (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
						.setInstance (0).setValue (value).setValballot (0));
			}
			assertEquals (id == 2, p.onNext (new ArrayList<> (), r.build ()));
		}
		assertEquals (101, p.commandFor (0));
	}
	private static boolean send (PhaseOneResponseProcessor p, int id, boolean history) {
		DidaTradePaxos.PhaseOneReply.Builder r = DidaTradePaxos.PhaseOneReply.newBuilder ()
				.setServerid (id).setRequestballot (2).setAccepted (true).setMaxballot (2);
		if (history)
			r.addResponseLogObjects (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
					.setInstance (0).setValue (101).setValballot (1));
		return p.onNext (new ArrayList<> (), r.build ());
	}
}
