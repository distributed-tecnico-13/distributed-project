package didatrade.server;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

public class DebugExitInterceptor implements ServerInterceptor {
	DidaTradeServerState server_state;

	public DebugExitInterceptor(DidaTradeServerState state) {
		this.server_state = state;
	}

	// a pending exit is only applied once the call is closed, otherwise System.exit
	// races with the transport still writing the reply
	@Override
	public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall (ServerCall<ReqT, RespT> call, Metadata headers,
			ServerCallHandler<ReqT, RespT> next) {
		ServerCall.Listener<ReqT> listener = next.startCall (call, headers);

		return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT> (listener) {
			@Override
			public void onComplete () {
				super.onComplete ();
				server_state.getDebugState ().applyExit ();
			}
		};
	}
}
