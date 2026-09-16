package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class DidaTradeCommandTest {

	@Test
	void addUserCommandKeepsItsArguments () {
		DidaTradeCommand command = new DidaTradeCommand (DidaTradeAction.ADDUSER, 3, 100, 50);

		assertEquals (DidaTradeAction.ADDUSER, command.getAction ());
		assertEquals (3, command.getUserId ());
		assertEquals (100, command.getQuantity ());
		assertEquals (50, command.getStock ());
	}
}
