package moe.ku6.yukinetbridge.impl.websocket;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.ByteBuffer;

public class BridgeWebsocketClient extends WebSocketClient {
    private final String secret;

    public BridgeWebsocketClient(URI serverUri, String secret) {
        super(serverUri);

        this.secret = secret;

        addHeader("X-YukiNetBridge-Secret", secret);

        connect();
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {

    }

    @Override
    public void onMessage(String s) {
        throw new UnsupportedOperationException("String messages are not supported.");
    }

    @Override
    public void onMessage(ByteBuffer buf) {
        super.onMessage(buf);

        var stream = new ByteArrayInputStream(buf.array());
    }

    @Override
    public void onClose(int i, String s, boolean b) {

    }

    @Override
    public void onError(Exception e) {

    }
}
