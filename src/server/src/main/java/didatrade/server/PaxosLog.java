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
		if (entry != null && (entry.isDecided ()
				|| !CommandIdHelper.isAbsent (entry.getProposedRequestId ())))
			return entry;

		Set<Integer> assigned = new HashSet<> ();
		for (PaxosInstance candidate : this.log.values ()) {
			int owned_request = candidate.getUnexecutedRequestId ();
			if (CommandIdHelper.isClientRequest (owned_request))
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

	// Binds a command Phase 1 discovered as already accepted for this instance. A fresh
	// discovery always overrides a stale, not-yet-decided local candidate; a noop fill
	// (fillNoops) must never clobber a real candidate; a decided slot never changes.
	public synchronized boolean adopt (int instance, int request_id) {
		PaxosInstance entry = this.log.get (instance);
		if (entry != null) {
			int existing = entry.getProposedRequestId ();
			if (existing == request_id)
				return true;
			if (entry.isDecided ())
				return entry.getCommandId () == request_id;
			if (CommandIdHelper.isNoop (request_id) && CommandIdHelper.isClientRequest (existing))
				return false;
		}
		// A noop is not a client request, so several holes may hold the same value.
		if (!CommandIdHelper.isNoop (request_id) && this.isRequestAssigned (request_id))
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

	public synchronized int getMinUndecidedEntry () {
		int instance = 0;
		while (true) {
			PaxosInstance entry = this.log.get (instance);
			if (entry == null || !entry.isDecided ())
				return instance;
			instance++;
		}
	}

	private boolean isRequestAssigned (int request_id) {
		for (PaxosInstance candidate : this.log.values ()) {
			if (candidate.getUnexecutedRequestId () == request_id)
				return true;
		}
		return false;
	}
}
