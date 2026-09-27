package didatrade.server;

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

	private static final int PROPOSED_VALUE = 101;
	private static final int REQUEST_BALLOT = 6;

	@Test
	void keepsProposedValueWhenNoPreviousValueWasAccepted () {
		PhaseOneResponseProcessor processor = completedProcessor (
				reply (0, -1, 0),
				reply (1, -1, 0));

		PaxosProposer.PhaseOneResult result = PaxosProposer.evaluatePhaseOneResult (
				processor, PROPOSED_VALUE);

		assertFalse (result.aborted);
		assertEquals (PROPOSED_VALUE, result.value);
	}

	@Test
	void carriesForwardValueWithHighestAcceptedBallot () {
		PhaseOneResponseProcessor processor = completedProcessor (
				reply (0, 4, 301),
				reply (1, 2, 201));

		PaxosProposer.PhaseOneResult result = PaxosProposer.evaluatePhaseOneResult (
				processor, PROPOSED_VALUE);

		assertFalse (result.aborted);
		assertEquals (301, result.value);
	}

	@Test
	void staleBallotCannotProceedToPhaseTwo () {
		assertFalse (DidaTradeServerState.canProposeFor (5, 8, 2, 2));
		assertTrue (DidaTradeServerState.canProposeFor (8, 8, 2, 2));
	}

	@Test
	void failedRpcCallbacksEndPhaseOneWithoutQuorum () {
		PhaseOneResponseProcessor processor = new PhaseOneResponseProcessor (
				new ConfigurationScheduler ('A'), 0, REQUEST_BALLOT);
		GenericResponseCollector<DidaTradePaxos.PhaseOneReply> collector = new GenericResponseCollector<> (
				new ArrayList<> (), 3, processor);
		collector.addResponse (reply (0, -1, 0));
		collector.addNoResponse ();
		collector.addNoResponse ();
		collector.waitUntilDone ();
		assertFalse (processor.getAccepted ());
	}

	@Test
	void sameBallotRetryCannotChangeAnAlreadySentPhaseTwoValue () {
		PaxosInstance entry = new PaxosInstance (0);
		assertEquals (101, entry.phaseTwoValueFor (REQUEST_BALLOT, 101));

		// A different Phase 1 quorum could now report an older accepted value.
		PhaseOneResponseProcessor retry = completedProcessor (
				reply (0, 4, 301), reply (1, -1, 0));
		PaxosProposer.PhaseOneResult selected = PaxosProposer.evaluatePhaseOneResult (
				retry, PROPOSED_VALUE);
		assertEquals (301, selected.value);
		assertEquals (101, entry.phaseTwoValueFor (REQUEST_BALLOT, selected.value));
		assertEquals (301, entry.phaseTwoValueFor (REQUEST_BALLOT + 1, selected.value));
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
