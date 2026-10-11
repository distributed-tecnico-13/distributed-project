package didatrade.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import didatrade.DidaTradePaxos;

class FastRoundResponseProcessorTest {

	@Test
	void quorumMarksRoundAcceptedButDoesNotStopRemainingOpenRequests () {
		FastRoundResponseProcessor processor = new FastRoundResponseProcessor (2, List.of (0, 1, 2));
		ArrayList<DidaTradePaxos.OpenFastReply> responses = new ArrayList<> ();

		assertFalse (processor.onNext (responses, acceptedReply (0)));
		assertFalse (processor.onNext (responses, acceptedReply (1)));
		assertTrue (processor.getAccepted ());
	}

	@Test
	void higherBallotRejectionAbortsEvenAfterAnOpeningQuorum () {
		FastRoundResponseProcessor processor = new FastRoundResponseProcessor (2, List.of (0, 1, 2));
		ArrayList<DidaTradePaxos.OpenFastReply> responses = new ArrayList<> ();

		assertFalse (processor.onNext (responses, acceptedReply (0)));
		assertFalse (processor.onNext (responses, acceptedReply (1)));
		assertTrue (processor.getAccepted ());

		DidaTradePaxos.OpenFastReply rejection = DidaTradePaxos.OpenFastReply.newBuilder ()
				.setServerid (2)
				.setBallot (0)
				.setAccepted (false)
				.setMaxballot (1)
				.build ();
		assertTrue (processor.onNext (responses, rejection));
		assertFalse (processor.getAccepted ());
	}

	@Test
	void duplicateAcceptorCannotCreateOpeningQuorum () {
		FastRoundResponseProcessor processor = new FastRoundResponseProcessor (2, List.of (0, 1, 2));
		ArrayList<DidaTradePaxos.OpenFastReply> responses = new ArrayList<> ();

		assertFalse (processor.onNext (responses, acceptedReply (0)));
		assertFalse (processor.onNext (responses, acceptedReply (0)));
		assertFalse (processor.getAccepted ());
	}

	@Test
	void nonAcceptorReplyIsIgnored () {
		FastRoundResponseProcessor processor = new FastRoundResponseProcessor (2, List.of (0, 1, 2));
		ArrayList<DidaTradePaxos.OpenFastReply> responses = new ArrayList<> ();

		assertFalse (processor.onNext (responses, acceptedReply (99)));
		assertFalse (processor.onNext (responses, acceptedReply (0)));
		assertFalse (processor.getAccepted ());
	}

	@Test
	void sameBallotRejectionDoesNotDestroyExistingOpeningQuorum () {
		FastRoundResponseProcessor processor = new FastRoundResponseProcessor (2, List.of (0, 1, 2));
		ArrayList<DidaTradePaxos.OpenFastReply> responses = new ArrayList<> ();

		assertFalse (processor.onNext (responses, acceptedReply (0)));
		assertFalse (processor.onNext (responses, acceptedReply (1)));
		assertTrue (processor.getAccepted ());

		DidaTradePaxos.OpenFastReply rejection = DidaTradePaxos.OpenFastReply.newBuilder ()
				.setServerid (2).setBallot (0).setAccepted (false).setMaxballot (0).build ();
		assertFalse (processor.onNext (responses, rejection));
		assertTrue (processor.getAccepted ());
	}

	@Test
	void openingQuorumSurvivesOneRpcFailure () {
		FastRoundResponseProcessor processor = new FastRoundResponseProcessor (2, List.of (0, 1, 2));
		GenericResponseCollector<DidaTradePaxos.OpenFastReply> collector = new GenericResponseCollector<> (new ArrayList<> (), 3, processor);

		collector.addResponse (acceptedReply (0));
		collector.addResponse (acceptedReply (2));
		collector.addNoResponse ();
		collector.waitUntilDone ();

		assertTrue (processor.getAccepted ());
	}

	@Test
	void oneReplyAndTwoRpcFailuresCannotOpenRound () {
		FastRoundResponseProcessor processor = new FastRoundResponseProcessor (2, List.of (0, 1, 2));
		GenericResponseCollector<DidaTradePaxos.OpenFastReply> collector = new GenericResponseCollector<> (new ArrayList<> (), 3, processor);

		collector.addResponse (acceptedReply (0));
		collector.addNoResponse ();
		collector.addNoResponse ();
		collector.waitUntilDone ();

		assertFalse (processor.getAccepted ());
	}

	private DidaTradePaxos.OpenFastReply acceptedReply (int server_id) {
		return DidaTradePaxos.OpenFastReply.newBuilder ()
				.setServerid (server_id)
				.setBallot (0)
				.setAccepted (true)
				.setMaxballot (0)
				.build ();
	}
}
