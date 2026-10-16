package didatrade.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import didatrade.server.request.CommandIdKind;

class CommandIdKindTest {
	@Test
	void classifiesSentinelsClientRequestsAndInvalidIds () {
		assertEquals (CommandIdKind.ABSENT, CommandIdKind.of (CommandIdKind.ABSENT_ID));
		assertEquals (CommandIdKind.NOOP, CommandIdKind.of (CommandIdKind.NOOP_ID));
		assertEquals (CommandIdKind.CLIENT_REQUEST, CommandIdKind.of (0));
		assertEquals (CommandIdKind.CLIENT_REQUEST, CommandIdKind.of (Integer.MAX_VALUE));
		assertEquals (CommandIdKind.INVALID, CommandIdKind.of (-3));
		assertEquals (CommandIdKind.INVALID, CommandIdKind.of (Integer.MIN_VALUE));
	}
}
