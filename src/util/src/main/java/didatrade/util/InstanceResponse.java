package didatrade.util;

/** Highest accepted ballot and command reported for one Paxos instance. */
public final class InstanceResponse {
	private int ballot;
	private int value;

	public InstanceResponse(int ballot, int value) {
		this.ballot = ballot;
		this.value = value;
	}

	public int getBallot () {
		return this.ballot;
	}

	public void setBallot (int ballot) {
		this.ballot = ballot;
	}

	public int getValue () {
		return this.value;
	}

	public void setValue (int value) {
		this.value = value;
	}
}
