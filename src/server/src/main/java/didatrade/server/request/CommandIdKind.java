package didatrade.server.request;

/** What a command id denotes: no value yet, a noop, a client request, or an invalid id. */
public enum CommandIdKind {
	ABSENT, NOOP, CLIENT_REQUEST, INVALID;

	public static final int ABSENT_ID = -1;
	public static final int NOOP_ID = -2;

	public static CommandIdKind of (int command_id) {
		if (command_id == ABSENT_ID)
			return ABSENT;
		if (command_id == NOOP_ID)
			return NOOP;
		return command_id >= 0 ? CLIENT_REQUEST : INVALID;
	}

	public static boolean isAbsent (int command_id) {
		return of (command_id) == ABSENT;
	}

	public static boolean isNoop (int command_id) {
		return of (command_id) == NOOP;
	}

	public static boolean isClientRequest (int command_id) {
		return of (command_id) == CLIENT_REQUEST;
	}
}
