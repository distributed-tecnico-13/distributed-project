package didatrade.configs;

import java.util.*;

public class ScheduleA extends Schedule {
	private List<Integer> all_participants = Arrays.asList (0, 1, 2);
	private List<Integer> all_learners = Arrays.asList (0, 1, 2);
	private List<Integer> all_acceptors = Arrays.asList (0, 1, 2);

	public ScheduleA() {
	}

	public List<Integer> learners (int ballot) {
		return this.all_learners;
	}

	public List<Integer> acceptors (int ballot) {
		return this.all_acceptors;
	}

	public boolean isacceptor (int node, int ballot) {
		return this.all_acceptors.contains (node);
	}

	public List<Integer> acceptorsinrange (int low_ballot, int high_ballot) {
		return this.all_acceptors;
	}

	public List<Integer> allparticipantsinballot (int ballot) {
		return this.all_participants;
	}

	public List<Integer> allparticipants () {
		return this.all_participants;
	}

	public Integer leader (int ballot) {
		int leader = ballot % 3;

		return leader;
	}

	public Integer quorum (int ballot) {
		return 2;
	}

	public boolean fastpaxos (int ballot) {
		// Schedule A always runs classic Multi-Paxos (quorum 2 of 3).
		return false;
	}
}
