package didatrade.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class UserTest {

	@Test
	void newUserKeepsCashAndStock () {
		User user = new User (7, 100, 50);

		assertEquals (7, user.getId ());
		assertEquals (100, user.getBalance ());
		assertEquals (50, user.getStock ());
	}

	@Test
	void withdrawFailsWhenBalanceIsTooLow () {
		User user = new User (7, 10, 0);

		assertTrue (user.withdraw (10));
		assertEquals (0, user.getBalance ());
		assertFalse (user.withdraw (1));
	}
}
