package didatrade.core;

import java.util.*;

public class TradeManager {
	private Hashtable<Integer, User> users;
	private ArrayList<SaleOrder> sales_orders;
	private ArrayList<BuyOrder> buy_orders;

	public TradeManager() {
		this.users = new Hashtable<Integer, User> ();
		this.sales_orders = new ArrayList<SaleOrder> ();
		this.buy_orders = new ArrayList<BuyOrder> ();
	}

	public synchronized boolean populate (int quantity) {
		for (int i = 0; i < quantity; i++)
			this.add_user (i, quantity, quantity);

		return true;
	}

	public synchronized boolean add_user (int user_id, int cash, int stock) {
		if (this.users.get (user_id) != null) {
			return false;
		}
		User newUser = new User (user_id, cash, stock);
		this.users.put (user_id, newUser);
		return true;
	}

	// Matches the oldest sell order with the oldest buy order until one side is empty.
	private void match_all () {
		while ((this.sales_orders.size () > 0) && (this.buy_orders.size () > 0)) {
			SaleOrder sale_order = this.sales_orders.getFirst ();
			BuyOrder buy_order = this.buy_orders.getFirst ();
			int n_items = Math.min (sale_order.available (), buy_order.pending ());
			sale_order.sell (n_items);
			buy_order.acquire (n_items);
			if (sale_order.available () == 0)
				this.sales_orders.removeFirst ();
			if (buy_order.pending () == 0)
				this.buy_orders.removeFirst ();
		}
	}

	// Sells fraction percent of the user's stock. False for an unknown user or if that rounds down
	// to zero items.
	public synchronized boolean sell (int user_id, int fraction) {
		User user = this.users.get (user_id);
		int n_items;

		if (user == null)
			return false;

		n_items = user.getStock () * fraction / 100;

		System.out.println ("\n\t in sell for user " + user_id + " with stock " + user.getStock () + " selling " + fraction + "%  = " + n_items + " items.\n");

		if (n_items > 0) {
			user.removeStock (n_items);
			SaleOrder order = new SaleOrder (user, n_items);
			this.sales_orders.add (order);
			this.match_all ();
			return true;
		} else
			return false;
	}

	// Spends fraction percent of the user's cash on items (1 cash per item). False like sell.
	public synchronized boolean acquire (int user_id, int fraction) {
		User user = this.users.get (user_id);
		int n_items;

		if (user == null)
			return false;

		n_items = user.getBalance () * fraction / 100;

		System.out.println ("\n\t in acquire for user " + user_id + " with balance " + user.getBalance () + " acquiring " + fraction + "%  = " + n_items + " items.\n");

		if (n_items > 0) {
			user.withdraw (n_items);
			BuyOrder order = new BuyOrder (user, n_items);
			this.buy_orders.add (order);
			this.match_all ();
			return true;
		} else
			return false;
	}

	// Cash balance, or -1 for an unknown user.
	public synchronized int balance (int user_id) {
		User user = this.users.get (user_id);

		if (user == null)
			return -1;
		else
			return user.getBalance ();
	}

	public synchronized void dump () {
		Enumeration<Integer> ids;

		System.out.println ("\n ----------- Users ----------- \n");

		ids = this.users.keys ();
		while (ids.hasMoreElements ()) {
			Integer uid = ids.nextElement ();
			User user = this.users.get (uid);
			System.out.println ("\n\t user " + uid + " balance = " + user.getBalance () + " stock = " + user.getStock ());
		}

		System.out.println ("\n ----------- Sell Orders ----------- \n");

		for (int i = 0; i < this.sales_orders.size (); i++) {
			SaleOrder order = this.sales_orders.get (i);
			User user = order.getUser ();
			System.out.println ("\n\t order from user " + user.getId () + " with " + order.available () + " available");
		}

		System.out.println ("\n ----------- Buy Orders ----------- \n");

		for (int i = 0; i < this.buy_orders.size (); i++) {
			BuyOrder order = this.buy_orders.get (i);
			User user = order.getUser ();
			System.out.println ("\n\t order from user " + user.getId () + " with " + order.pending () + " pending");
		}

		System.out.println ("\n -----------    done     ----------- \n");
	}
}
