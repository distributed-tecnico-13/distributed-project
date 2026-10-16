package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import didatrade.configs.ConfigurationScheduler;

/** Regression checks for the Fast Paxos PR review discussion. */
class FastPaxosReviewRegressionTest {

	@Test
	void acceptorLocalFastOpenDoesNotImplyLeaderOpeningQuorum () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.scheduler = new ConfigurationScheduler ('B');
		state.paxos_log = new PaxosLog ();
		state.my_id = 0;
		state.setRanPhaseOne (0);
		PaxosProposer proposer = new PaxosProposer (state);

		// A partial openfast exchange may open the coordinator's own acceptor,
		// but fewer than four acknowledgements cannot establish the Fast round.
		assertTrue (state.openFastRound (0));
		assertTrue (state.isFastRoundOpen (0));
		assertFalse (proposer.hasFastOpeningQuorum (0));
	}

	@Test
	void failedReproposalOfDecidedSlotCanRetryInSameBallot () {
		PaxosInstance slot = new PaxosInstance (0);
		assertTrue (slot.decide (0, 100));
		assertTrue (slot.tryClaim (1, System.nanoTime ()));
		assertEquals (100, slot.pinPhaseTwoValue (1, 100));
		// RPC failure: Phase 2 did not obtain a quorum.
		slot.releaseClaim (1, 0);
		assertTrue (slot.tryClaim (1, System.nanoTime ()));
		assertEquals (100, slot.pinPhaseTwoValue (1, 999)); // cannot change within ballot
	}

	@Test
	void confirmedHigherBallotReproposalDoesNotDependOnLearns () {
		PaxosInstance slot = new PaxosInstance (0);
		assertTrue (slot.decide (0, 100));
		assertFalse (slot.decide (1, 100));
		assertEquals (1, slot.getAcceptBallot ());
		assertFalse (slot.tryClaim (1, System.nanoTime ()));
		assertTrue (slot.tryClaim (2, System.nanoTime ()));
	}

	@Test
	void acceptedClassicRecoveryReleasesOldFastRequestFromItsSlot () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.scheduler = new ConfigurationScheduler ('B');
		state.paxos_log = new PaxosLog ();
		state.my_id = 0;
		PaxosInstance slot = state.paxos_log.assignFastRequest (100);
		assertTrue (slot.accept (0, 100));
		assertTrue (state.processPhaseTwo (slot, 1, 200).accepted);
		assertEquals (200, slot.getUnexecutedRequestId ());
		PaxosInstance next = state.paxos_log.assignFastRequest (100);
		assertEquals (1, next.getInstanceNb ());
		assertEquals (100, next.getProposedRequestId ());
	}

	@Test
	void recoveredPrefixIsNotUsedForNewFastRequests () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.scheduler = new ConfigurationScheduler ('B');
		state.paxos_log = new PaxosLog ();
		state.my_id = 0;
		assertTrue (state.openFastRound (1, 4));
		DidaTradeServerState.FastVoteState vote = state.fastVote (123);
		assertTrue (vote.accepted);
		assertEquals (4, vote.instance);
	}

	@Test
	void higherOpenFastBallotClosesOldFastRoundAndAdvancesPromise () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.scheduler = new ConfigurationScheduler ('B');
		state.paxos_log = new PaxosLog ();
		state.my_id = 0;
		assertTrue (state.openFastRound (0));
		assertTrue (state.openFastRound (1, 2));
		assertFalse (state.isFastRoundOpen (0));
		assertEquals (1, state.getCurrentBallot ());
		assertEquals (2, state.fastVote (100).instance);
		assertFalse (state.openFastRound (0));
	}

	@Test
	void unchosenFastSlotIsPreservedUntilExplicitBallotChange () {
		DidaTradeServerState state = new DidaTradeServerState ();
		state.scheduler = new ConfigurationScheduler ('B');
		state.paxos_log = new PaxosLog ();
		state.my_id = 0;
		assertTrue (state.openFastRound (0));
		state.fastVote (100);
		assertEquals (0, state.getCurrentBallot ());
		assertEquals (100, state.paxos_log.getEntry (0).getAcceptedValue ());
		state.setCurrentBallot (1);
		assertEquals (1, state.getCurrentBallot ());
		assertEquals (100, state.paxos_log.getEntry (0).getAcceptedValue ());
	}

	@Test
	void adoptNeverFabricatesLocalAcceptedHistory () {
		PaxosLog log = new PaxosLog ();
		assertTrue (log.adopt (4, 123));
		assertEquals (0, log.getAcceptedEntries ().length);
		PaxosInstance entry = log.testAndSetEntry (4);
		assertTrue (entry.accept (0, 234));
		assertTrue (log.adopt (4, 345));
		assertEquals (234, entry.getAcceptedValue ());
		assertEquals (0, entry.getWriteBallot ());
	}
}
