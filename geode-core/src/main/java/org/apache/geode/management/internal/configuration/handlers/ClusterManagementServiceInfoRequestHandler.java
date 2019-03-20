package org.apache.geode.management.internal.configuration.handlers;

import java.io.IOException;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.tcpserver.TcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.management.internal.configuration.messages.ClusterManagementServiceInfo;
import org.apache.geode.management.internal.configuration.messages.ClusterManagementServiceInfoRequest;

public class ClusterManagementServiceInfoRequestHandler implements TcpHandler {
  @Override
  public Object processRequest(Object request) throws IOException {
    if (!(request instanceof ClusterManagementServiceInfoRequest)) {
      throw new IllegalArgumentException("invalid request type");
    }

    ClusterManagementServiceInfo info = new ClusterManagementServiceInfo();
    InternalLocator locator = InternalLocator.getLocator();
    if (locator.getClusterManagementService() == null) {
      return info;
    }

    DistributionConfigImpl config = locator.getConfig();
    info.setHttpPort(config.getHttpServicePort());
    info.setHostName(config.getHttpServiceBindAddress());

    return info;
  }

  @Override
  public void endRequest(Object request, long startTime) {

  }

  @Override
  public void endResponse(Object request, long startTime) {

  }

  @Override
  public void shutDown() {

  }

  @Override
  public void init(TcpServer tcpServer) {

  }

  @Override
  public void restarting(DistributedSystem system, GemFireCache cache,
      InternalConfigurationPersistenceService sharedConfig) {

  }
}
