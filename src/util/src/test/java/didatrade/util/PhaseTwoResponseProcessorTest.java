package didatrade.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import didatrade.DidaTradePaxos;

class PhaseTwoResponseProcessorTest {

	@Test
	void rpcErrorsCannotCompleteAnAcceptedQuorum () {
		PhaseTwoResponseProcessor processor = new PhaseTwoResponseProcessor (2);
		GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> collector = collector (processor);

		collector.addResponse (acceptedReply (0));
		collector.addNoResponse ();
		collector.addNoResponse ();
		collector.waitUntilDone ();

		assertFalse (processor.getAccepted ());
	}

	@Test
	void quorumOfAcceptedRepliesCompletesPhaseTwo () {
		PhaseTwoResponseProcessor processor = new PhaseTwoResponseProcessor (2);
		GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> collector = collector (processor);

		collector.addResponse (acceptedReply (0));
		collector.addResponse (acceptedReply (1));
		collector.waitUntilDone ();

		assertTrue (processor.getAccepted ());
	}

	@Test
	void rejectionAbortsAndPreservesHigherBallot () {
		PhaseTwoResponseProcessor processor = new PhaseTwoResponseProcessor (2);
		GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> collector = collector (processor);

		collector.addResponse (acceptedReply (0));
		collector.addResponse (DidaTradePaxos.PhaseTwoReply.newBuilder ()
				.setServerid (1)
				.setAccepted (false)
				.setMaxballot (8)
				.build ());
		collector.waitUntilDone ();

		assertFalse (processor.getAccepted ());
		assertEquals (8, processor.getMaxballot ());
	}

	private GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> collector (
			PhaseTwoResponseProcessor processor) {
		return new GenericResponseCollector<> (new ArrayList<> (), 3, processor);
	}

	private DidaTradePaxos.PhaseTwoReply acceptedReply (int server_id) {
		return DidaTradePaxos.PhaseTwoReply.newBuilder ()
				.setServerid (server_id)
				.setAccepted (true)
				.build ();
	}
}
