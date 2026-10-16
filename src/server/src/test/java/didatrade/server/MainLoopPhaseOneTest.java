package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;
import didatrade.util.GenericResponseCollector;
import didatrade.util.PhaseOneResponseProcessor;

class MainLoopPhaseOneTest {

	private static final int REQUEST_BALLOT = 6;

	@Test
	void staleBallotCannotProceedToPhaseTwo () {
		assertFalse (DidaTradeServerState.canProposeFor (5, 8, 2, 2));
		assertTrue (DidaTradeServerState.canProposeFor (8, 8, 2, 2));
	}

	@Test
	void failedRpcCallbacksEndPhaseOneWithoutQuorum () {
		PhaseOneResponseProcessor processor = new PhaseOneResponseProcessor (
				new ConfigurationScheduler ('A'), REQUEST_BALLOT);
		GenericResponseCollector<DidaTradePaxos.PhaseOneReply> collector = new GenericResponseCollector<> (
				new ArrayList<> (), 3, processor);
		collector.addResponse (reply (0, 0, -1, 0));
		collector.addNoResponse ();
		collector.addNoResponse ();
		collector.waitUntilDone ();
		assertFalse (processor.getAccepted ());
	}

	@Test
	void sameBallotRetryCannotChangeAnAlreadySentPhaseTwoValue () {
		PaxosInstance entry = new PaxosInstance (0);
		assertEquals (101, entry.pinPhaseTwoValue (REQUEST_BALLOT, 101));
		assertEquals (101, entry.pinPhaseTwoValue (REQUEST_BALLOT, 301));
		assertEquals (301, entry.pinPhaseTwoValue (REQUEST_BALLOT + 1, 301));
	}

	@Test
	void phaseOneResultKeepsCommandsByInstance () {
		PhaseOneResponseProcessor processor = new PhaseOneResponseProcessor (
				new ConfigurationScheduler ('A'), REQUEST_BALLOT);
		processor.onNext (new ArrayList<> (), reply (0, 0, 4, 301));
		processor.onNext (new ArrayList<> (), reply (1, 1, 2, 201));

		PaxosProposer.PhaseOneResult result = new PaxosProposer.PhaseOneResult (processor);

		assertFalse (result.aborted);
		assertArrayEquals (new int[]{0, 1}, result.instances ());
		assertEquals (301, result.commandFor (0));
		assertEquals (201, result.commandFor (1));
	}

	private DidaTradePaxos.PhaseOneReply reply (
			int server_id, int instance, int value_ballot, int value) {
		return DidaTradePaxos.PhaseOneReply.newBuilder ()
				.setServerid (server_id)
				.setRequestballot (REQUEST_BALLOT)
				.setAccepted (true)
				.setMaxballot (REQUEST_BALLOT)
				.addResponseLogObjects (DidaTradePaxos.PhaseOneResponseLogObject.newBuilder ()
						.setInstance (instance)
						.setValballot (value_ballot)
						.setValue (value)
						.build ())
				.build ();
	}
}
