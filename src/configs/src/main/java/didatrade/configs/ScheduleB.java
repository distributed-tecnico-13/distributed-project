package didatrade.configs;

import java.util.*;
import didatrade.configs.Schedule; // ignore-unused-import

public class ScheduleB extends Schedule {
	private List<Integer> all_participants = Arrays.asList (0, 1, 2, 3, 4);
	private List<Integer> all_learners = Arrays.asList (0, 1, 2, 3, 4);
	private List<Integer> all_acceptors = Arrays.asList (0, 1, 2, 3, 4);

	public ScheduleB() {
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
		return 3;
	}

	@Override
	int fastQuorum (int ballot) {
		return 4;
	}

	@Override
	int recoveryQuorum (int ballot) {
		return 3;
	}

	public boolean fastpaxos (int ballot) {
		return true;
	}
}
