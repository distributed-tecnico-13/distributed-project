package didatrade.configs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

class ScheduleBFastPaxosTest {

	@Test
	void everyConfigurationIsFast () {
		ConfigurationScheduler scheduler = new ConfigurationScheduler ('B');
		for (int ballot = 0; ballot < 12; ballot++)
			assertTrue (scheduler.fastpaxos (ballot));
	}

	@Test
	void leaderRotatesAcrossAllThreeReplicas () {
		ConfigurationScheduler scheduler = new ConfigurationScheduler ('B');
		for (int ballot = 0; ballot < 12; ballot++)
			assertEquals (ballot % 3, scheduler.leader (ballot));
	}

	@Test
	void everyBallotUsesSeparateQuorums () {
		ConfigurationScheduler scheduler = new ConfigurationScheduler ('B');
		for (int ballot = 0; ballot < 12; ballot++) {
			assertEquals (3, scheduler.quorum (ballot));
			assertEquals (3, scheduler.classicQuorum (ballot));
			assertEquals (3, scheduler.recoveryQuorum (ballot));
			assertEquals (4, scheduler.fastQuorum (ballot));
		}
	}

	@Test
	void everyReplicaIsAnAcceptorAndLearner () {
		ConfigurationScheduler scheduler = new ConfigurationScheduler ('B');
		List<Integer> replicas = List.of (0, 1, 2, 3, 4);
		for (int ballot = 0; ballot < 6; ballot++) {
			assertEquals (replicas, scheduler.acceptors (ballot));
			assertEquals (replicas, scheduler.learners (ballot));
			for (int replica : replicas)
				assertTrue (scheduler.isacceptor (replica, ballot));
		}
	}
}
