package didatrade.configs;

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import org.junit.jupiter.api.Test;

class ScheduleCTest {
	@Test
	void selectsSevenLearnersAndFiveAcceptorsWithMajorityThree () {
		ConfigurationScheduler s = new ConfigurationScheduler ('C');
		assertTrue (s.dynamicMembership ());
		assertEquals (7, s.allparticipants ().size ());
		assertEquals (List.of (0, 1, 2, 3, 4), s.acceptors (1));
		assertEquals (List.of (2, 3, 4, 5, 6), s.acceptors (2));
		for (int b = 0; b < 5; b++) {
			assertEquals (3, s.classicQuorum (b));
			assertEquals (3, s.recoveryQuorum (b));
			assertEquals (4, s.fastQuorum (b));
		}
		assertEquals (List.of (0, 1, 3, 4, 5), java.util.stream.IntStream.range (0, 5).map (s::leader).boxed ().toList ());
		assertFalse (s.validBallot (-1));
		assertFalse (s.validBallot (5));
	}
}
