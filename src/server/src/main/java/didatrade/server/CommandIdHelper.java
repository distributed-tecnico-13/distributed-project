package didatrade.server;

/** A command id is absent (-1), a noop (-2), or a client request id. */
final class CommandIdHelper {
	static final int ABSENT_VALUE = -1;
	static final int NOOP_VALUE = -2;

	private CommandIdHelper() {
	}

	static boolean isAbsent (int command_id) {
		return command_id == ABSENT_VALUE;
	}

	static boolean isNoop (int command_id) {
		return command_id == NOOP_VALUE;
	}

	static boolean isClientRequest (int command_id) {
		return command_id >= 0;
	}
}
