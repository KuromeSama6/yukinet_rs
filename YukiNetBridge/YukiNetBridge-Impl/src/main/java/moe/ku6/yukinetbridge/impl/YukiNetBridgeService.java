package moe.ku6.yukinetbridge.impl;

import lombok.Getter;
import moe.ku6.yukinetbridge.impl.config.YukiNetBridgeConfig;
import moe.ku6.yukinetbridge.impl.websocket.BridgeWebsocketClient;

import java.net.URI;

public class YukiNetBridgeService {
    @Getter
    private static YukiNetBridgeService instance;
    private final YukiNetBridgeConfig config;
    private final BridgeWebsocketClient websocketClient;

    public YukiNetBridgeService(YukiNetBridgeConfig config) {
        if (instance != null)
            throw new IllegalStateException("YukiNetBridgeService is already initialized");

        instance = this;

        this.config = config;

        websocketClient = new BridgeWebsocketClient(config.getMasterUri(), config.getSecret());
    }
}
