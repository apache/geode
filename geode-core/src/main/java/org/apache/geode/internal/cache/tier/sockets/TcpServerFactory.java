package org.apache.geode.internal.cache.tier.sockets;

import java.net.InetAddress;
import java.util.Properties;

import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.distributed.internal.tcpserver.TcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;

public class TcpServerFactory {
  private ClientProtocolMessageHandler protocolHandler;

  public TcpServerFactory() {
    initializeMessageHandler();
  }

  public TcpServer makeTcpServer(int port, InetAddress bind_address, Properties sslConfig,
      DistributionConfigImpl cfg, TcpHandler handler, PoolStatHelper poolHelper,
      ThreadGroup threadGroup, String threadName, InternalLocator internalLocator) {

    return new TcpServer(port, bind_address, sslConfig, cfg, handler, poolHelper, threadGroup,
        threadName, internalLocator, protocolHandler);
  }

  public synchronized ClientProtocolMessageHandler initializeMessageHandler() {
    if (!Boolean.getBoolean("geode.feature-protobuf-protocol")) {
      return null;
    }
    if (protocolHandler != null) {
      return protocolHandler;
    }

    protocolHandler = new MessageHandlerFactory().makeMessageHandler();

    return protocolHandler;
  }
}
