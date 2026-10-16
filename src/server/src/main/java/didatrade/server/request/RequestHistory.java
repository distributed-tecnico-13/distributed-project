
package didatrade.server.request;

import java.util.Hashtable;
import java.util.List;

/**
 * Client requests by id: pending until executed, then processed. Executed ones are kept so
 * duplicates are recognised.
 */
public class RequestHistory {
	private Hashtable<Integer, RequestRecord> pending;
	private Hashtable<Integer, RequestRecord> processed;

	public RequestHistory() {
		this.pending = new Hashtable<Integer, RequestRecord> ();
		this.processed = new Hashtable<Integer, RequestRecord> ();
	}

	public synchronized RequestRecord getIfPending (int requestid) {
		return this.pending.get (requestid);
	}

	public synchronized List<Integer> getPendingRequestIds () {
		return this.pending.keySet ().stream ().sorted ().toList ();
	}

	public synchronized RequestRecord getIfProcessed (int requestid) {
		return this.processed.get (requestid);
	}

	public synchronized RequestRecord getIfExists (int requestid) {
		RequestRecord record;

		record = this.pending.get (requestid);
		if (record == null)
			record = this.processed.get (requestid);
		return record;
	}

	public synchronized boolean addToPending (int requestid, RequestRecord record) {
		if (!CommandIdKind.isClientRequest (requestid))
			return false;
		this.pending.put (requestid, record);
		return true;
	}

	public synchronized RequestRecord moveToProcessed (int requestid) {
		RequestRecord record = this.pending.remove (requestid);
		if (record != null)
			this.processed.put (requestid, record);
		return record;
	}

}
