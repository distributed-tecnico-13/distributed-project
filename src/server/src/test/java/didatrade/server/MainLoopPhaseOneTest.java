package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;
import didatrade.util.GenericResponseCollector;
import didatrade.util.PhaseOneResponseProcessor;

class MainLoopPhaseOneTest {

	private static final int PROPOSED_VALUE = 101;
	private static final int REQUEST_BALLOT = 6;

	@Test
	void keepsProposedValueWhenNoPreviousValueWasAccepted () {
		PhaseOneResponseProcessor processor = completedProcessor (
				reply (0, -1, 0),
				reply (1, -1, 0));

		MainLoop.PhaseOneResult result = MainLoop.evaluatePhaseOneResult (
				processor, PROPOSED_VALUE);

		assertFalse (result.aborted);
		assertEquals (PROPOSED_VALUE, result.value);
	}

	@Test
	void carriesForwardValueWithHighestAcceptedBallot () {
		PhaseOneResponseProcessor processor = completedProcessor (
				reply (0, 4, 301),
				reply (1, 2, 201));

		MainLoop.PhaseOneResult result = MainLoop.evaluatePhaseOneResult (
				processor, PROPOSED_VALUE);

		assertFalse (result.aborted);
		assertEquals (301, result.value);
	}

	private PhaseOneResponseProcessor completedProcessor (
			DidaTradePaxos.PhaseOneReply first,
			DidaTradePaxos.PhaseOneReply second) {
		PhaseOneResponseProcessor processor = new PhaseOneResponseProcessor (
				new ConfigurationScheduler ('A'), 0, REQUEST_BALLOT);
		ArrayList<DidaTradePaxos.PhaseOneReply> responses = new ArrayList<> ();
		GenericResponseCollector<DidaTradePaxos.PhaseOneReply> collector = new GenericResponseCollector<> (responses, 3, processor);

		collector.addResponse (first);
		collector.addResponse (second);
		return processor;
	}

	private DidaTradePaxos.PhaseOneReply reply (
			int server_id, int value_ballot, int value) {
		return DidaTradePaxos.PhaseOneReply.newBuilder ()
				.setServerid (server_id)
				.setRequestballot (REQUEST_BALLOT)
				.setAccepted (true)
				.setValballot (value_ballot)
				.setValue (value)
				.setMaxballot (REQUEST_BALLOT)
				.build ();
	}
}
