package didatrade.server;

/**
 * Checks that only matter because unit tests build a partially initialised
 * {@link DidaTradeServerState} (see its package-private test constructor).
 * A production state always has a scheduler and a running MainLoop, so
 * these never change what the real logic does. The checks are moved in here
 * so reading the code is not confusing, else reader might wonder why
 * mainLoop etc could ever be null
 */
public final class TestHelper {
	private TestHelper() {
	}

	// Test states have no MainLoop; production states always have one.
	public static boolean hasMainLoop (DidaTradeServerState state) {
		return state.main_loop != null;
	}

	// Test states may have no scheduler; production states always have one.
	public static boolean hasScheduler (DidaTradeServerState state) {
		return state.scheduler != null;
	}
}
