
package didatrade.server;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

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

	public synchronized RequestRecord getFirstPending () {
		return this.getFirstPendingExcluding (new HashSet<> ());
	}

	public synchronized RequestRecord getFirstPendingExcluding (
			Set<Integer> excluded_request_ids) {
		Enumeration<Integer> pendingids = this.pending.keys ();
		while (pendingids.hasMoreElements ()) {
			int request_id = pendingids.nextElement ();
			if (!excluded_request_ids.contains (request_id))
				return this.pending.get (request_id);
		}
		return null;
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

	public synchronized void addToPending (int requestid, RequestRecord record) {
		this.pending.put (requestid, record);
	}

	public synchronized RequestRecord moveToProcessed (int requestid) {
		RequestRecord record = this.pending.remove (requestid);
		if (record != null)
			this.processed.put (requestid, record);
		return record;
	}

}
