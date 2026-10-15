package didatrade.server.request;

/** A command id is absent (-1), a noop (-2), or a client request id. */
public final class CommandIdHelper {
	public static final int ABSENT_VALUE = -1;
	public static final int NOOP_VALUE = -2;

	private CommandIdHelper() {
	}

	public static boolean isAbsent (int command_id) {
		return command_id == ABSENT_VALUE;
	}

	public static boolean isNoop (int command_id) {
		return command_id == NOOP_VALUE;
	}

	public static boolean isClientRequest (int command_id) {
		return command_id >= 0;
	}
}
