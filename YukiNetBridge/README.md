# YukiNetBridge

The YukiNetBridge plugin connects your proxy and your backend servers to YukiNet, allowing YukiNet control to your network, adding and removing backend servers at needed.

YukiNetBridge is required to use YukiNet with your network.

YukiNetBridge must be used in conjuction with YukiNet (yukinet_rs); this plugin alone will not provide any functionality.

## Installation

1. Download (or build yourself) the latest version of YukiNetBridge from the [releases page](https://github.com/KuromeSama6/yukinet_rs). We currently support the following proxy software:
    - BungeeCord and its forks;
    - Velocity and its forks.
2. Place the downloaded JAR file into the `plugins` folder of your proxy server.
3. Run your server once to generate configuration files.
4. Configure connection details (host IP, port, secrets, etc.) in `config.json` as needed to connect to YukiNet. These need to match the ones in your `master_config.json` file.
5. Start your network with YukiNet, which will also start your proxy server. YukiNetBridge should now connect to YukiNet automatically.

Note - For YukiNetBridge to initialize correctly, YukiNet must be running before starting your proxy server. We recommend starting your network through YukiNet, which will handle this for you. YukiNetBridge will block the proxy server startup process until it successfully connects to YukiNet.

## API

YukiNetBridge provides an API for your plugin to interact with YukiNet, such as adding or removing backend servers dynamically.