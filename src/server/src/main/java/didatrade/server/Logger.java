package didatrade.server;

/** One-line server traces. State changes only, so retries do not flood the log. */
final class Logger {
	private final StringBuilder message = new StringBuilder ();
	private String open_list;

	private Logger(DidaTradeServerState state) {
		this.message.append ("[s").append (state.my_id).append ("] ");
	}

	static Logger log (DidaTradeServerState state) {
		return new Logger (state);
	}

	static void log (DidaTradeServerState state, String message) {
		log (state).add (message).log ();
	}

	Logger add (String text) {
		this.open_list = null;
		this.message.append (text);
		return this;
	}

	Logger add (int value) {
		return this.add (Integer.toString (value));
	}

	Logger item (String name, String value) {
		if (name.equals (this.open_list))
			this.message.append (',');
		else {
			this.message.append (' ').append (name).append ('=');
			this.open_list = name;
		}
		this.message.append (value);
		return this;
	}

	Logger item (String name, int value) {
		return this.item (name, Integer.toString (value));
	}

	void log () {
		System.out.println (this.message);
	}
}
