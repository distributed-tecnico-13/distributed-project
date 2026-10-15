package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import didatrade.server.request.DidaTradeAction;
import didatrade.server.request.DidaTradeCommand;

class DidaTradeCommandTest {

	@Test
	void addUserCommandKeepsItsArguments () {
		DidaTradeCommand command = new DidaTradeCommand (DidaTradeAction.ADDUSER, 3, 100, 50);

		assertEquals (DidaTradeAction.ADDUSER, command.getAction ());
		assertEquals (3, command.getUserId ());
		assertEquals (100, command.getQuantity ());
		assertEquals (50, command.getStock ());
		assertEquals (-1, command.getInstance ());
	}
}
