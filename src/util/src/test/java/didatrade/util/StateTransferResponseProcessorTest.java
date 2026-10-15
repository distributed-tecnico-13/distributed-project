package didatrade.util;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import didatrade.DidaTradePaxos;

class StateTransferResponseProcessorTest {
	@Test
	void onlyDistinctCurrentAcceptorsCompleteTransfer () {
		StateTransferResponseProcessor processor = processor ();
		assertFalse (reply (processor, 0, true, 2)); // retired acceptor
		assertFalse (reply (processor, 1, true, 2)); // retired acceptor
		assertFalse (reply (processor, 2, true, 2));
		assertFalse (reply (processor, 2, true, 2)); // duplicate
		assertFalse (reply (processor, 3, false, 2));
		assertFalse (reply (processor, 4, true, 2));
		assertFalse (processor.getAccepted ());
		assertTrue (reply (processor, 5, true, 2));
		assertTrue (processor.getAccepted ());
	}

	@Test
	void rejectionAtCurrentBallotDoesNotAbortOtherAcceptors () {
		StateTransferResponseProcessor processor = processor ();
		assertFalse (reply (processor, 2, false, 2));
		assertFalse (reply (processor, 3, true, 2));
		assertFalse (reply (processor, 4, true, 2));
		assertTrue (reply (processor, 5, true, 2));
		assertTrue (processor.getAccepted ());
	}

	@Test
	void higherBallotFromAcceptorOrRetiredLearnerAbortsTransfer () {
		for (int id : new int[]{0, 2}) {
			StateTransferResponseProcessor processor = processor ();
			assertFalse (reply (processor, 3, true, 2));
			assertTrue (reply (processor, id, false, 3));
			assertEquals (3, processor.getMaxballot ());
			assertFalse (processor.getAccepted ());
		}
	}

	@Test
	void higherBallotInAnAcceptedReplyStillAbortsTransfer () {
		StateTransferResponseProcessor processor = processor ();
		assertFalse (reply (processor, 2, true, 2));
		assertFalse (reply (processor, 3, true, 2));
		assertTrue (reply (processor, 4, true, 3));
		assertFalse (processor.getAccepted ());
	}

	@Test
	void collectorCanCompleteWithQuorumDespiteRpcFailures () {
		StateTransferResponseProcessor processor = processor ();
		GenericResponseCollector<DidaTradePaxos.InstallStateReply> collector = new GenericResponseCollector<> (
				new ArrayList<> (), 7, processor);
		collector.addNoResponse ();
		collector.addNoResponse ();
		for (int id : new int[]{2, 3, 4})
			collector.addResponse (response (id, true, 2));
		collector.waitUntilDone ();
		assertTrue (processor.getAccepted ());
	}

	@Test
	void exhaustedRepliesWithoutQuorumLeaveTransferIncomplete () {
		StateTransferResponseProcessor processor = processor ();
		GenericResponseCollector<DidaTradePaxos.InstallStateReply> collector = new GenericResponseCollector<> (
				new ArrayList<> (), 3, processor);
		collector.addResponse (response (2, true, 2));
		collector.addResponse (response (3, true, 2));
		collector.addNoResponse ();
		collector.waitUntilDone ();
		assertFalse (processor.getAccepted ());
	}

	private StateTransferResponseProcessor processor () {
		return new StateTransferResponseProcessor (2, 3, List.of (2, 3, 4, 5, 6));
	}

	private boolean reply (StateTransferResponseProcessor processor, int id, boolean accepted, int maxballot) {
		return processor.onNext (new ArrayList<> (), response (id, accepted, maxballot));
	}

	private DidaTradePaxos.InstallStateReply response (int id, boolean accepted, int maxballot) {
		return DidaTradePaxos.InstallStateReply.newBuilder ()
				.setServerid (id).setAccepted (accepted).setMaxballot (maxballot).build ();
	}
}
