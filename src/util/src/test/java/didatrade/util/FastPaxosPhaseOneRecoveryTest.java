package didatrade.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;

class FastPaxosPhaseOneRecoveryTest {

	private static final int REQUEST_BALLOT = 6;

	@Test
	void splitFastVotesRemainAmbiguousUntilThirdAcceptorReplies () {
		PhaseOneResponseProcessor processor = fastProcessor ();

		assertFalse (process (processor, accepted (0, entry (0, 5, 100))));
		assertFalse (process (processor, accepted (1, entry (0, 5, 200))));
		assertFalse (processor.getAccepted ());

		assertTrue (process (processor, accepted (2, entry (0, 5, 100))));
		assertTrue (processor.getAccepted ());
		assertEquals (5, processor.ballotFor (0));
		assertEquals (100, processor.commandFor (0));
	}

	@Test
	void matchingFastQuorumCanBeRecoveredWithoutWaitingForThirdReply () {
		PhaseOneResponseProcessor processor = fastProcessor ();

		assertFalse (process (processor, accepted (0, entry (0, 5, 100))));
		assertTrue (process (processor, accepted (1, entry (0, 5, 100))));

		assertTrue (processor.getAccepted ());
		assertEquals (100, processor.commandFor (0));
	}

	@Test
	void threeWayFastSplitProvesNoValueWasChosenAndUsesDeterministicChoice () {
		PhaseOneResponseProcessor processor = fastProcessor ();

		assertFalse (process (processor, accepted (0, entry (0, 5, 300))));
		assertFalse (process (processor, accepted (1, entry (0, 5, 100))));
		assertTrue (process (processor, accepted (2, entry (0, 5, 200))));

		assertTrue (processor.getAccepted ());
		assertEquals (5, processor.ballotFor (0));
		assertEquals (100, processor.commandFor (0));
	}

	@Test
	void highestFastBallotDominatesLowerAcceptedHistory () {
		PhaseOneResponseProcessor processor = fastProcessor ();

		assertFalse (process (processor, accepted (0, entry (0, 3, 100))));
		assertTrue (process (processor, accepted (1, entry (0, 5, 200))));

		assertTrue (processor.getAccepted ());
		assertEquals (5, processor.ballotFor (0));
		assertEquals (200, processor.commandFor (0));
	}

	@Test
	void classicHighestBallotWithConflictingValuesIsRejectedAsInvalidHistory () {
		PhaseOneResponseProcessor processor = new PhaseOneResponseProcessor (
				new ConfigurationScheduler ('A'), 4);

		assertFalse (process (processor, acceptedFor (4, 0, entry (0, 1, 100))));
		assertFalse (process (processor, acceptedFor (4, 1, entry (0, 1, 200))));
		assertFalse (processor.getAccepted ());
		assertFalse (process (processor, acceptedFor (4, 2, entry (0, 1, 100))));
		assertFalse (processor.getAccepted ());
	}

	@Test
	void recoveryResolvesEachInstanceIndependently () {
		PhaseOneResponseProcessor processor = fastProcessor ();

		assertFalse (process (processor, accepted (0,
				entry (0, 5, 100), entry (1, 5, 400))));
		assertFalse (process (processor, accepted (1,
				entry (0, 5, 200), entry (1, 5, 400))));
		assertTrue (process (processor, accepted (2,
				entry (0, 5, 100), entry (1, 5, 500))));

		assertTrue (processor.getAccepted ());
		assertEquals (100, processor.commandFor (0));
		assertEquals (400, processor.commandFor (1));
	}

	private PhaseOneResponseProcessor fastProcessor () {
		return new PhaseOneResponseProcessor (
				new ConfigurationScheduler ('B'), REQUEST_BALLOT);
	}

	private boolean process (
			PhaseOneResponseProcessor processor,
			DidaTradePaxos.PhaseOneReply reply) {
		return processor.onNext (new ArrayList<> (), reply);
	}

	private DidaTradePaxos.PhaseOneReply accepted (
			int server_id,
			DidaTradePaxos.PhaseOneResponseLogObject... entries) {
		return acceptedFor (REQUEST_BALLOT, server_id, entries);
	}

	private DidaTradePaxos.PhaseOneReply acceptedFor (
			int request_ballot,
			int server_id,
			DidaTradePaxos.PhaseOneResponseLogObject... entries) {
		DidaTradePaxos.PhaseOneReply.Builder reply = DidaTradePaxos.PhaseOneReply.newBuilder ()
				.setServerid (server_id)
				.setRequestballot (request_ballot)
				.setAccepted (true)
				.setMaxballot (request_ballot);
		for (DidaTradePaxos.PhaseOneResponseLogObject entry : entries)
			reply.addResponseLogObjects (entry);
		return reply.build ();
	}

	private DidaTradePaxos.PhaseOneResponseLogObject entry (
			int instance, int value_ballot, int value) {
		return DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
				.setInstance (instance)
				.setValballot (value_ballot)
				.setValue (value)
				.build ();
	}
}
