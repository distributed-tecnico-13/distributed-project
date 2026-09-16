package didatrade.app;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class DidaTradeAppTest {

	/*
	 * Placeholder. The app module is a gRPC client driven from main() and needs a
	 * running replica group to exercise, so there is nothing to assert yet.
	 */
	@Test
	void appClassLoads () {
		assertNotNull (DidaTradeApp.class);
	}
}
