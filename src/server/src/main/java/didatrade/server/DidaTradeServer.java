package didatrade.server;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import didatrade.server.service.DebugExitInterceptor;
import didatrade.server.service.DidaTradeMainServiceImpl;
import didatrade.server.service.DidaTradeMasterServiceImpl;
import didatrade.server.service.DidaTradePaxosServiceImpl;

public class DidaTradeServer {

	static DidaTradeServerState server_state;

	/** Server host port. */
	private static int port;

	public static void main (String[] args) throws Exception {

		System.out.println (DidaTradeServer.class.getSimpleName ());

		// Print received arguments.
		System.out.printf ("Received %d arguments%n", args.length);
		for (int i = 0; i < args.length; i++) {
			System.out.printf ("arg[%d] = %s%n", i, args[i]);
		}

		// Check arguments.
		if (args.length < 3) {
			System.err.println ("Argument(s) missing!");
			System.err.printf ("Usage: java %s baseport replica-id scheduler\n", Server.class.getName ());
			return;
		}

		int base_port = Integer.valueOf (args[0]);
		int my_id = Integer.valueOf (args[1]);
		char scheduler = args[2].charAt (0);

		server_state = new DidaTradeServerState (base_port, my_id, scheduler);
		// An unexpected error in the MainLoop means this replica's Paxos state can no longer be
		// trusted. Crash like any failed replica; the others carry on. Should not happen.
		server_state.getMainLoop ().setFatalHandler ( () -> System.exit (1));

		port = base_port + my_id;

		final BindableService service_impl = new DidaTradeMainServiceImpl (server_state);
		final BindableService master_impl = new DidaTradeMasterServiceImpl (server_state);
		final BindableService paxos_impl = new DidaTradePaxosServiceImpl (server_state);

		// Create a new server to listen on port.
		Server server = ServerBuilder.forPort (port).addService (service_impl).addService (master_impl).addService (paxos_impl)
				.intercept (new DebugExitInterceptor (server_state)).build ();
		// Start the server.
		server.start ();
		// Server threads are running in the background.
		System.out.println ("Server started");

		// Do not exit the main thread. Wait until server is terminated.
		server.awaitTermination ();
	}
}
