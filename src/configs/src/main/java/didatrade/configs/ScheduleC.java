package didatrade.configs;

import java.util.*;

/**
 * Dynamic membership: acceptors 0-4 before MEMBERSHIP_CHANGE_BALLOT and 2-6 from it on. Fast Paxos
 * only in ballot 0 and the membership change ballot.
 */
public class ScheduleC extends Schedule {
	public static final int MEMBERSHIP_CHANGE_BALLOT = 2;

	private List<Integer> all_participants = Arrays.asList (0, 1, 2, 3, 4, 5, 6);
	private List<Integer> all_learners = Arrays.asList (0, 1, 2, 3, 4, 5, 6);
	private List<Integer> acceptors_start = Arrays.asList (0, 1, 2, 3, 4);
	private List<Integer> acceptors_end = Arrays.asList (2, 3, 4, 5, 6);
	private List<Integer> acceptors_all = Arrays.asList (0, 1, 2, 3, 4, 5, 6);

	public ScheduleC() {
	}

	public List<Integer> learners (int ballot) {
		return this.all_learners;
	}

	public List<Integer> acceptors (int ballot) {
		if (ballot < MEMBERSHIP_CHANGE_BALLOT)
			return this.acceptors_start;
		else
			return this.acceptors_end;
	}

	public List<Integer> acceptorsinrange (int low_ballot, int high_ballot) {
		if (high_ballot < MEMBERSHIP_CHANGE_BALLOT)
			return this.acceptors_start;
		else if (low_ballot >= MEMBERSHIP_CHANGE_BALLOT)
			return this.acceptors_end;
		else
			return this.acceptors_all;
	}

	public List<Integer> allparticipantsinballot (int ballot) {
		return this.all_participants;
	}

	public List<Integer> allparticipants () {
		return this.all_participants;
	}

	public boolean isacceptor (int node, int ballot) {
		if (ballot < MEMBERSHIP_CHANGE_BALLOT)
			return this.acceptors_start.contains (node);
		else
			return this.acceptors_end.contains (node);
	}

	// Ballots 0-1 are led by replicas 0-1, ballots 2-4 by replicas 3-5, later ballots by replica 5.
	public Integer leader (int ballot) {
		if (ballot < MEMBERSHIP_CHANGE_BALLOT)
			return ballot;
		else if (ballot < 5)
			return ballot + 1;
		else
			return 5;
	}

	public Integer quorum (int ballot) {
		return 3;
	}

	@Override
	int fastQuorum (int ballot) {
		return 4;
	}

	public boolean fastpaxos (int ballot) {
		return ((ballot == 0) || (ballot == MEMBERSHIP_CHANGE_BALLOT));
	}
}
