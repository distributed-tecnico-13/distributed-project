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
					.addCommands (DidaTradePaxos.CommandPayload.newBuilder ().setRequestid (value).setAction ("ADDUSER")).build ()));
		}
		// This new acceptor certifies that slot 0 was transferred, and that
		// the old votes at slot 1 were not needed to preserve any decision.
		assertTrue (p.onNext (new ArrayList<> (), DidaTradePaxos.PhaseOneReply.newBuilder ()
				.setServerid (6).setAccepted (true).setRequestballot (3).setMaxballot (3).setMembershiptransferred (true)
				.addResponseLogObjects (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
						.setInstance (0).setValue (101).setValballot (2).setClassic (true))
				.addCommands (DidaTradePaxos.CommandPayload.newBuilder ().setRequestid (101).setAction ("ADDUSER")).build ()));
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
			if (id == 1 || id == 2) {
				int value = id * 100 + 1;
				r.addResponseLogObjects (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
						.setInstance (0).setValue (value).setValballot (0))
						.addCommands (DidaTradePaxos.CommandPayload.newBuilder ().setRequestid (value).setAction ("ADDUSER"));
			}
			assertFalse (p.onNext (new ArrayList<> (), r.build ()));
		}
		assertFalse (p.membershipTransferred ());
		assertFalse (p.getAccepted ());
	}

	@Test
	void transferMarkerCannotDiscardAmbiguousNewFastVotes () {
		PhaseOneResponseProcessor p = new PhaseOneResponseProcessor (new ConfigurationScheduler ('C'), 3);
		for (int id : new int[]{2, 3, 4, 5}) {
			DidaTradePaxos.PhaseOneReply.Builder r = DidaTradePaxos.PhaseOneReply.newBuilder ()
					.setServerid (id).setAccepted (true).setRequestballot (3).setMaxballot (3)
					.setMembershiptransferred (id == 4);
			if (id != 4) {
				int value = id == 3 ? 201 : 101;
				r.addResponseLogObjects (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
						.setInstance (0).setValue (value).setValballot (2))
						.addCommands (DidaTradePaxos.CommandPayload.newBuilder ().setRequestid (value).setAction ("ADDUSER"));
			}
			assertEquals (id == 5, p.onNext (new ArrayList<> (), r.build ()));
		}
		assertEquals (101, p.commandFor (0));
	}

	@Test
	void majorityOfUnionCannotReplaceAnOldMajority () {
		PhaseOneResponseProcessor p = new PhaseOneResponseProcessor (new ConfigurationScheduler ('C'), 2, 0);
		assertFalse (send (p, 3, false, false));
		assertFalse (send (p, 4, false, false));
		assertFalse (send (p, 5, false, false));
		assertFalse (send (p, 6, false, false));
		assertTrue (send (p, 0, true, true));
		assertEquals (101, p.commandFor (0));
		assertEquals (101, p.payloadFor (101).getRequestid ());
	}
	@Test
	void newExclusiveMembersDoNotCountAsOldAcceptors () {
		PhaseOneResponseProcessor p = new PhaseOneResponseProcessor (new ConfigurationScheduler ('C'), 2, 0);
		assertFalse (send (p, 5, false, false));
		assertFalse (send (p, 6, false, false));
		assertFalse (send (p, 0, true, true));
		assertFalse (send (p, 1, true, true));
		assertTrue (send (p, 2, true, true));
		assertEquals (101, p.commandFor (0));
	}
	@Test
	void aRecoveredIdWithoutItsPayloadCannotFinishPreparation () {
		PhaseOneResponseProcessor p = new PhaseOneResponseProcessor (new ConfigurationScheduler ('C'), 2, 0);
		for (int id : new int[]{0, 1, 2, 3, 4, 5, 6})
			assertFalse (send (p, id, id < 3, false));
		assertFalse (p.getAccepted ());
	}
	@Test
	void ambiguousFastHistoryWaitsForTheRemainingOldAcceptor () {
		PhaseOneResponseProcessor p = new PhaseOneResponseProcessor (new ConfigurationScheduler ('C'), 2, 0);
		for (int id : new int[]{5, 0, 1, 2, 3}) {
			DidaTradePaxos.PhaseOneReply.Builder r = DidaTradePaxos.PhaseOneReply.newBuilder ()
					.setServerid (id).setAccepted (true).setRequestballot (2).setMaxballot (2);
			if (id != 5) {
				int value = id % 2 == 0 ? 101 : 201;
				r.addResponseLogObjects (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
						.setInstance (0).setValue (value).setValballot (0))
						.addCommands (DidaTradePaxos.CommandPayload.newBuilder ().setRequestid (value).setAction ("ADDUSER"));
			}
			assertFalse (p.onNext (new ArrayList<> (), r.build ()));
		}
		DidaTradePaxos.PhaseOneReply last = DidaTradePaxos.PhaseOneReply.newBuilder ()
				.setServerid (4).setAccepted (true).setRequestballot (2).setMaxballot (2)
				.addResponseLogObjects (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ().setInstance (0).setValue (101).setValballot (0))
				.addCommands (DidaTradePaxos.CommandPayload.newBuilder ().setRequestid (101).setAction ("ADDUSER")).build ();
		assertTrue (p.onNext (new ArrayList<> (), last));
		assertEquals (101, p.commandFor (0));
	}
	private static boolean send (PhaseOneResponseProcessor p, int id, boolean history, boolean payload) {
		DidaTradePaxos.PhaseOneReply.Builder r = DidaTradePaxos.PhaseOneReply.newBuilder ()
				.setServerid (id).setRequestballot (2).setAccepted (true).setMaxballot (2);
		if (history)
			r.addResponseLogObjects (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
					.setInstance (0).setValue (101).setValballot (1).setClassic (true));
		if (payload)
			r.addCommands (DidaTradePaxos.CommandPayload.newBuilder ().setRequestid (101)
					.setAction ("ADDUSER").setUserid (77).setQuantity (100).setStock (10));
		return p.onNext (new ArrayList<> (), r.build ());
	}
}
