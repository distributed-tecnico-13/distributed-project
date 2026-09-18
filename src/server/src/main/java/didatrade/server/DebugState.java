package didatrade.server;
public class DebugState {
	private boolean throttled;
	private boolean blocking;
	private boolean shouldExit;

	public static final int MAX_THROTTLE_DELAY = 3000;

	public DebugState() {
		this.throttled = false;
		this.blocking = false;
		this.shouldExit = false;
	}

	public synchronized void setDebugMode (int mode) {
		switch (mode) {
			case 0 :
				System.out.println ("DebugState: Received mode 0 - Disabling debugging");
				blocking = false;
				throttled = false;
				shouldExit = false;
				break;
			case 1 :
				System.out.println ("DebugState: Received mode 1 - Setting shouldExit = true");
				shouldExit = true;
				break;
			case 2 :
				System.out.println ("DebugState: Received mode 2 - Setting blocking = true");
				blocking = true;
				break;
			case 3 :
				System.out.println ("DebugState: Received mode 3 - Setting blocking = false");
				blocking = false;
				break;
			case 4 :
				System.out.println ("DebugState: Received mode 4 - Setting throttled = true");
				throttled = true;
				break;
			case 5 :
				System.out.println ("DebugState: Received mode 5 - Setting throttled = false");
				throttled = false;
				break;
			default :
				System.err.println ("Unknown debug mode: " + mode);
				return;
		}
		this.notifyAll ();
	}

	public void apply () {
		applyExit ();
		applyThrottle ();
		applyBlocking ();
	}

	public void applyBlocking () {
		synchronized (this) {
			while (isBlocking () && !shouldExit ()) {
				try {
					System.out.println ("Waiting in debug mode 2");
					this.wait ();
				} catch (InterruptedException e) {
				}
			}
		}
	}

	public void applyThrottle () {
		if (isThrottled ()) {
			try {
				Thread.sleep ((long) (Math.random () * DebugState.MAX_THROTTLE_DELAY));
			} catch (InterruptedException e) {
			}
		}
	}

	public void applyExit () {
		if (shouldExit ()) {
			System.out.println ("Exiting because of debug mode");
			System.exit (0);
		}
	}

	public boolean isThrottled () {
		return throttled;
	}
	public void setThrottled (boolean throttled) {
		this.throttled = throttled;
	}

	public boolean isBlocking () {
		return blocking;
	}
	public void setBlocking (boolean blocking) {
		this.blocking = blocking;
	}

	public boolean shouldExit () {
		return shouldExit;
	}
	public void setShouldExit (boolean shouldExit) {
		this.shouldExit = shouldExit;
	}
}
