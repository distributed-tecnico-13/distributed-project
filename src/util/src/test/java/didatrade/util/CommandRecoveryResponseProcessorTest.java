package didatrade.util;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import didatrade.DidaTradePaxos;

@Timeout(5)
class CommandRecoveryResponseProcessorTest {
	@Test
	void requiresEveryRequestedCommandRatherThanAMajorityOfReplies () {
		CommandRecoveryResponseProcessor p = new CommandRecoveryResponseProcessor (Set.of (101, 201));
		assertFalse (send (p, command (101)));
		assertFalse (send (p, command (101))); // duplicate contributes no new command
		assertFalse (send (p, command (301))); // unrequested
		assertFalse (p.getAccepted ());
		assertTrue (send (p, command (201)));
		assertTrue (p.getAccepted ());
		assertEquals (2, p.getCommands ().size ());
	}

	@Test
	void conflictingParametersAbortRecovery () {
		CommandRecoveryResponseProcessor p = new CommandRecoveryResponseProcessor (Set.of (101, 201));
		assertFalse (send (p, command (101)));
		assertTrue (send (p, command (101).toBuilder ().setQuantity (999).build ()));
		assertFalse (p.getAccepted ());
	}

	@Test
	void missingCommandAfterAllRepliesLeavesRecoveryIncomplete () {
		CommandRecoveryResponseProcessor p = new CommandRecoveryResponseProcessor (Set.of (101, 201));
		GenericResponseCollector<DidaTradePaxos.FetchCommandsReply> collector = new GenericResponseCollector<> (new ArrayList<> (), 2, p);
		collector.addResponse (DidaTradePaxos.FetchCommandsReply.newBuilder ().addCommands (command (101)).build ());
		collector.addNoResponse ();
		collector.waitUntilDone ();
		assertFalse (p.getAccepted ());
	}

	private boolean send (CommandRecoveryResponseProcessor p, DidaTradePaxos.CommandPayload command) {
		return p.onNext (new ArrayList<> (), DidaTradePaxos.FetchCommandsReply.newBuilder ().addCommands (command).build ());
	}

	private DidaTradePaxos.CommandPayload command (int id) {
		return DidaTradePaxos.CommandPayload.newBuilder ().setRequestid (id).setAction ("BUY").setUserid (77).setQuantity (50).build ();
	}
}
