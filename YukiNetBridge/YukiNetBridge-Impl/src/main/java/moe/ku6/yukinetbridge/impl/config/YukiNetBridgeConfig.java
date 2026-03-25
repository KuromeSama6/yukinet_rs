package moe.ku6.yukinetbridge.impl.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.net.URI;

@AllArgsConstructor
@Getter
public class YukiNetBridgeConfig {
    private final URI masterUri;
    private final String secret;
}
