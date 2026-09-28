package didatrade.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;

class PhaseOneResponseProcessorTest {

	private static final int REQUEST_BALLOT = 6;

	@Test
	void emptyLogsStillCompleteOnceAQuorumPromises () {
		PhaseOneResponseProcessor processor = newProcessor ();

		assertFalse (process (processor, promise (0)));
		assertTrue (process (processor, promise (1)));
		assertTrue (processor.getAccepted ());
		assertEquals (0, processor.instances ().length);
	}

	@Test
	void fewerThanQuorumResponsesDoesNotCompletePhaseOne () {
		PhaseOneResponseProcessor processor = newProcessor ();

		assertFalse (process (processor, promise (0)));
		assertFalse (processor.getAccepted ());
	}

	@Test
	void higherBallotResponseAbortsPhaseOne () {
		PhaseOneResponseProcessor processor = newProcessor ();

		assertTrue (process (processor, rejected (0, 9)));
		assertFalse (processor.getAccepted ());
		assertEquals (9, processor.getMaxballot ());
	}

	@Test
	void keepsEachInstanceAndTheHighestAcceptedBallot () {
		PhaseOneResponseProcessor processor = newProcessor ();

		process (processor, accepted (0, entry (0, 2, 201), entry (1, 4, 301)));
		process (processor, accepted (1, entry (0, 5, 401), entry (1, 3, 101)));

		assertArrayEquals (new int[]{0, 1}, processor.instances ());
		assertEquals (5, processor.ballotFor (0));
		assertEquals (401, processor.commandFor (0));
		assertEquals (4, processor.ballotFor (1));
		assertEquals (301, processor.commandFor (1));
	}

	@Test
	void duplicateAcceptorDoesNotCountTwice () {
		PhaseOneResponseProcessor processor = newProcessor ();

		assertFalse (process (processor, promise (0)));
		assertFalse (process (processor, promise (0)));
		assertFalse (processor.getAccepted ());
	}

	@Test
	void replyFromOutsideTheAcceptorSetIsIgnored () {
		PhaseOneResponseProcessor processor = newProcessor ();

		assertFalse (process (processor, promise (9)));
		assertFalse (process (processor, promise (0)));
		assertFalse (processor.getAccepted ());
	}

	private PhaseOneResponseProcessor newProcessor () {
		return new PhaseOneResponseProcessor (
				new ConfigurationScheduler ('A'), REQUEST_BALLOT);
	}

	private boolean process (
			PhaseOneResponseProcessor processor,
			DidaTradePaxos.PhaseOneReply reply) {
		return processor.onNext (new ArrayList<> (), reply);
	}

	private DidaTradePaxos.PhaseOneReply promise (int server_id) {
		return accepted (server_id);
	}

	private DidaTradePaxos.PhaseOneReply rejected (int server_id, int max_ballot) {
		return DidaTradePaxos.PhaseOneReply.newBuilder ()
				.setServerid (server_id)
				.setRequestballot (REQUEST_BALLOT)
				.setAccepted (false)
				.setMaxballot (max_ballot)
				.build ();
	}

	private DidaTradePaxos.PhaseOneReply accepted (
			int server_id,
			DidaTradePaxos.PhaseOneResponseLogObject... entries) {
		DidaTradePaxos.PhaseOneReply.Builder reply = DidaTradePaxos.PhaseOneReply.newBuilder ()
				.setServerid (server_id)
				.setRequestballot (REQUEST_BALLOT)
				.setAccepted (true)
				.setMaxballot (REQUEST_BALLOT);
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
