package didatrade.server;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import didatrade.server.request.CommandIdKind;
import didatrade.server.request.RequestHistory;
import didatrade.server.request.RequestRecord;

/** The replica's log of Paxos instances, created on demand by slot number. */
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

	public synchronized PaxosInstance[] entries () {
		return this.log.values ().toArray (PaxosInstance[]::new);
	}

	// Returns the entry of the slot, creating it if it does not exist yet.
	public synchronized PaxosInstance testAndSetEntry (int position) {
		PaxosInstance entry = this.log.get (position);

		if (entry == null) {
			entry = new PaxosInstance (position);
			this.log.put (position, entry);
		}
		return entry;
	}

	// Classic path: gives the slot the first pending client request that no other slot owns yet.
	// Fast ballots allocate per acceptor in assignFastRequest instead.
	public synchronized PaxosInstance assignPendingRequest (int position, RequestHistory history) {
		PaxosInstance entry = this.log.get (position);
		if (entry != null && (entry.isDecided ()
				|| !CommandIdKind.isAbsent (entry.getProposedRequestId ())))
			return entry;

		Set<Integer> assigned = new HashSet<> ();
		for (PaxosInstance candidate : this.log.values ()) {
			int owned_request = candidate.getUnexecutedRequestId ();
			if (CommandIdKind.isClientRequest (owned_request))
				assigned.add (owned_request);
		}
		// This is optional, the executor executes each command id only once.
		// In Phase one proposing the same command id in multiple slots is possible.
		// But because its cheap to prevent it here for the proposer, we do it.
		RequestRecord pending = history.getFirstPendingExcluding (assigned);
		if (pending == null)
			return entry;
		if (entry == null)
			entry = this.testAndSetEntry (position);
		entry.setProposedRequestId (pending.getId ());
		return entry;
	}

	public synchronized PaxosInstance assignFastRequest (int request_id) {
		return this.assignFastRequest (request_id, 0);
	}

	// Fast path: the request's own slot if it already has one, otherwise the first free slot at or
	// after first_fast_instance.
	public synchronized PaxosInstance assignFastRequest (int request_id, int first_fast_instance) {
		for (PaxosInstance candidate : this.log.values ()) {
			if (candidate.getInstanceNb () >= first_fast_instance
					&& candidate.getUnexecutedRequestId () == request_id)
				return candidate;
		}

		int position = first_fast_instance;
		while (true) {
			PaxosInstance entry = this.log.get (position);
			if (entry == null) {
				entry = this.testAndSetEntry (position);
				entry.setProposedRequestId (request_id);
				return entry;
			}
			if (!entry.isDecided () && !entry.isAccepted ()
					&& CommandIdKind.isAbsent (entry.getProposedRequestId ())) {
				entry.setProposedRequestId (request_id);
				return entry;
			}
			position++;
		}
	}

	// Phase 1 recovery: makes the slot propose the recovered value. False if the slot is already
	// decided on another value.
	public synchronized boolean adopt (int instance, int request_id) {
		PaxosInstance entry = this.log.get (instance);
		if (entry != null) {
			if (entry.getProposedRequestId () == request_id)
				return true;
			if (entry.isDecided ())
				return entry.getCommandId () == request_id;
		}
		if (entry == null)
			entry = this.testAndSetEntry (instance);
		entry.setProposedRequestId (request_id);
		return true;
	}

	// Speculatively fills an instance Phase 1 said nothing about.
	public synchronized boolean fillNoop (int instance) {
		PaxosInstance entry = this.log.get (instance);
		if (entry != null && !entry.isDecided ()
				&& CommandIdKind.isClientRequest (entry.getProposedRequestId ()))
			return false;
		return this.adopt (instance, CommandIdKind.NOOP_ID);
	}

	// Slots this acceptor accepted a value in; the content of a Phase 1 reply.
	public synchronized PaxosInstance[] getAcceptedEntries () {
		return this.log.entrySet ().stream ()
				.filter (e -> e.getValue ().isAccepted ())
				.map (entry -> entry.getValue ())
				.toArray (PaxosInstance[]::new);
	}
}
