package didatrade.server;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

public class PaxosLog {
	private Hashtable<Integer, PaxosInstance> log;

	public PaxosLog() {
		this.log = new Hashtable<Integer, PaxosInstance> ();
	}

	public synchronized int length () {
		return this.log.size ();
	}

	public synchronized PaxosInstance getEntry (int position) {
		return this.log.get (position);
	}

	public synchronized PaxosInstance testAndSetEntry (int position) {
		PaxosInstance entry = this.log.get (position);

		if (entry == null) {
			entry = new PaxosInstance (position);
			this.log.put (position, entry);
		}
		return entry;
	}

	public synchronized PaxosInstance testAndSetEntry (int position, int ballot) {
		PaxosInstance entry = this.log.get (position);

		if (entry == null) {
			entry = new PaxosInstance (position, ballot);
			this.log.put (position, entry);
		}
		return entry;
	}

	// MainLoop is the sole allocator. Keep the request/slot choice with the log.
	public synchronized PaxosInstance assignPendingRequest (int position, RequestHistory history) {
		PaxosInstance entry = this.log.get (position);
		if (entry != null && (entry.isDecided () || entry.getProposedRequestId () >= 0))
			return entry;

		Set<Integer> assigned = new HashSet<> ();
		for (PaxosInstance candidate : this.log.values ()) {
			int owned_request = candidate.getUnexecutedRequestId ();
			if (owned_request >= 0)
				assigned.add (owned_request);
		}
		RequestRecord pending = history.getFirstPendingExcluding (assigned);
		if (pending == null)
			return entry;
		if (entry == null)
			entry = this.testAndSetEntry (position);
		entry.setProposedRequestId (pending.getId ());
		return entry;
	}

	// Binds a command Phase 1 already accepted. A later fresh request cannot reuse the slot.
	public synchronized boolean adopt (int instance, int request_id) {
		PaxosInstance entry = this.log.get (instance);
		if (entry != null && entry.getProposedRequestId () >= 0)
			return entry.getProposedRequestId () == request_id;
		if (this.isRequestAssigned (request_id))
			return false;
		if (entry == null)
			entry = this.testAndSetEntry (instance);
		entry.setProposedRequestId (request_id);
		return true;
	}

	public synchronized void replaceProposedRequestId (int position, int request_id) {
		PaxosInstance entry = this.log.get (position);
		if (entry != null)
			entry.setProposedRequestId (request_id);
	}

	public synchronized PaxosInstance[] getAcceptedEntries (int minInstance) {
		return this.log.entrySet ().stream ()
				.filter (e -> e.getKey () >= minInstance && e.getValue ().isAccepted ())
				.map (entry -> entry.getValue ())
				.toArray (PaxosInstance[]::new);
	}

	public synchronized int getMinUndecidedInstance () {
		return this.log.entrySet ().stream ()
				.filter (e -> !e.getValue ().isDecided ())
				.map (entry -> entry.getKey ())
				.min (Integer::compareTo)
				.orElse (-1);
	}

	private boolean isRequestAssigned (int request_id) {
		for (PaxosInstance candidate : this.log.values ()) {
			if (candidate.getUnexecutedRequestId () == request_id)
				return true;
		}
		return false;
	}
}
