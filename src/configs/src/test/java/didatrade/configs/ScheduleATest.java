package didatrade.configs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ScheduleATest {

	@Test
	void scheduleAHasThreeParticipants () {
		ScheduleA schedule = new ScheduleA ();

		assertEquals (3, schedule.allparticipants ().size ());
		assertEquals (2, schedule.quorum (0));
	}

	@Test
	void everyParticipantIsAnAcceptor () {
		ScheduleA schedule = new ScheduleA ();

		for (Integer node : schedule.allparticipants ()) {
			assertTrue (schedule.isacceptor (node, 0));
		}
	}
}
