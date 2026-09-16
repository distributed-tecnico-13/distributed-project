package didatrade.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;

class GenericResponseCollectorTest {

	@Test
	void collectsResponsesUntilQuorum () {
		ArrayList<String> responses = new ArrayList<String> ();
		GenericResponseCollector<String> collector = new GenericResponseCollector<String> (responses, 3);

		collector.addResponse ("a");
		collector.addResponse ("b");
		collector.waitForQuorum (2);

		assertEquals (2, responses.size ());
	}
}
