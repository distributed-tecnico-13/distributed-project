package didatrade.configs;

import java.util.*;

/** Cluster configuration: who leads, accepts and learns in each ballot, and the quorum sizes. */
public abstract class Schedule {

	abstract List<Integer> learners (int ballot);

	abstract List<Integer> acceptors (int ballot);

	// Acceptors across the ballots low..high; the union if the group changes in between.
	abstract List<Integer> acceptorsinrange (int low_ballot, int high_ballot);

	abstract boolean isacceptor (int node, int ballot);

	abstract List<Integer> allparticipants ();

	abstract List<Integer> allparticipantsinballot (int ballot);

	abstract Integer leader (int ballot);

	abstract Integer quorum (int ballot);

	// The existing quorum remains the classic Phase-2 majority.
	int classicQuorum (int ballot) {
		return this.quorum (ballot);
	}
	// Fast and Phase-1 recovery thresholds may differ from the classic quorum.
	int fastQuorum (int ballot) {
		return this.quorum (ballot);
	}
	int recoveryQuorum (int ballot) {
		return this.quorum (ballot);
	}

	abstract boolean fastpaxos (int ballot);
}
