package didatrade.server;

import didatrade.DidaTradePaxos;

/** Encodes immutable parameters; execution state and responses stay local. */
final class CommandTransfer {
	private CommandTransfer() {
	}

	static DidaTradePaxos.CommandPayload encode (RequestRecord record) {
		DidaTradeCommand c = record.getRequest ();
		return DidaTradePaxos.CommandPayload.newBuilder ().setRequestid (record.getId ())
				.setAction (c.getAction ().name ()).setUserid (c.getUserId ())
				.setQuantity (c.getQuantity ()).setStock (c.getStock ()).build ();
	}

	static DidaTradeCommand decode (DidaTradePaxos.CommandPayload payload) {
		if (!CommandIdHelper.isClientRequest (payload.getRequestid ()))
			throw new IllegalArgumentException ("Invalid client request id");
		return new DidaTradeCommand (DidaTradeAction.valueOf (payload.getAction ()),
				payload.getUserid (), payload.getQuantity (), payload.getStock ());
	}

	static boolean same (DidaTradeCommand a, DidaTradeCommand b) {
		return a.getAction () == b.getAction () && a.getUserId () == b.getUserId ()
				&& a.getQuantity () == b.getQuantity () && a.getStock () == b.getStock ();
	}
}
