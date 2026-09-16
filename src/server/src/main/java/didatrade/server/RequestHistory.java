
package didatrade.server;

import java.util.Enumeration;
import java.util.Hashtable;

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
		Enumeration<Integer> pendingids = this.pending.keys ();
		if (pendingids.hasMoreElements ())
			return this.pending.get (pendingids.nextElement ());
		else
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
		this.processed.put (requestid, record);
		return record;
	}

}
