package didatrade.util;

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
	void noPreviouslyAcceptedValuesLeavesSelectionEmpty () {
		PhaseOneResponseProcessor processor = newProcessor ();

		assertFalse (process (processor, acceptedReply (0, -1, 0)));
		assertTrue (process (processor, acceptedReply (1, -1, 0)));
		assertTrue (processor.getAccepted ());
		assertEquals (-1, processor.getValballot ());
		assertEquals (-1, processor.getValue ());
	}

	@Test
	void onePreviouslyAcceptedValueIsSelected () {
		PhaseOneResponseProcessor processor = newProcessor ();

		process (processor, acceptedReply (0, -1, 0));
		process (processor, acceptedReply (1, 4, 301));

		assertEquals (4, processor.getValballot ());
		assertEquals (301, processor.getValue ());
	}

	@Test
	void severalAcceptedValuesSelectHighestBallot () {
		PhaseOneResponseProcessor processor = newProcessor ();

		process (processor, acceptedReply (0, 2, 201));
		process (processor, acceptedReply (1, 4, 301));

		assertEquals (4, processor.getValballot ());
		assertEquals (301, processor.getValue ());
	}

	@Test
	void highestAcceptedBallotNeedNotBeLastResponse () {
		PhaseOneResponseProcessor processor = newProcessor ();

		process (processor, acceptedReply (0, 4, 301));
		process (processor, acceptedReply (1, 2, 201));

		assertEquals (4, processor.getValballot ());
		assertEquals (301, processor.getValue ());
	}

	@Test
	void fewerThanQuorumResponsesDoesNotCompletePhaseOne () {
		PhaseOneResponseProcessor processor = newProcessor ();

		assertFalse (process (processor, acceptedReply (0, -1, 0)));
		assertFalse (processor.getAccepted ());
	}

	@Test
	void quorumOfDistinctAcceptorsCompletesPhaseOne () {
		PhaseOneResponseProcessor processor = newProcessor ();

		assertFalse (process (processor, acceptedReply (0, -1, 0)));
		assertTrue (process (processor, acceptedReply (1, -1, 0)));
		assertTrue (processor.getAccepted ());
	}

	@Test
	void higherBallotResponseAbortsPhaseOne () {
		PhaseOneResponseProcessor processor = newProcessor ();

		assertTrue (process (processor, rejectedReply (0, 9)));
		assertFalse (processor.getAccepted ());
		assertEquals (9, processor.getMaxballot ());
	}

	private PhaseOneResponseProcessor newProcessor () {
		return new PhaseOneResponseProcessor (
				new ConfigurationScheduler ('A'), 0, REQUEST_BALLOT);
	}

	private boolean process (
			PhaseOneResponseProcessor processor,
			DidaTradePaxos.PhaseOneReply reply) {
		return processor.onNext (new ArrayList<> (), reply);
	}

	private DidaTradePaxos.PhaseOneReply acceptedReply (
			int server_id, int value_ballot, int value) {
		return reply (server_id, true, value_ballot, value, REQUEST_BALLOT);
	}

	private DidaTradePaxos.PhaseOneReply rejectedReply (
			int server_id, int max_ballot) {
		return reply (server_id, false, -1, 0, max_ballot);
	}

	private DidaTradePaxos.PhaseOneReply reply (
			int server_id,
			boolean accepted,
			int value_ballot,
			int value,
			int max_ballot) {
		return DidaTradePaxos.PhaseOneReply.newBuilder ()
				.setServerid (server_id)
				.setRequestballot (REQUEST_BALLOT)
				.setAccepted (accepted)
				.setValballot (value_ballot)
				.setValue (value)
				.setMaxballot (max_ballot)
				.build ();
	}
}
