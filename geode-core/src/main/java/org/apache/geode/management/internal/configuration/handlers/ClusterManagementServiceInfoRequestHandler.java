/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.management.internal.configuration.handlers;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.RestartableTcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.internal.configuration.messages.ClusterManagementServiceInfo;
import org.apache.geode.management.internal.configuration.messages.ClusterManagementServiceInfoRequest;

public class ClusterManagementServiceInfoRequestHandler implements RestartableTcpHandler {
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

    String hostName = null;
    if (StringUtils.isNotBlank(config.getHttpServiceBindAddress())) {
      hostName = config.getHttpServiceBindAddress();
    } else if (StringUtils.isNotBlank(config.getServerBindAddress())) {
      hostName = config.getServerBindAddress();
    } else if (StringUtils.isNotBlank(locator.getHostnameForClients())) {
      hostName = locator.getHostnameForClients();
    } else {
      hostName = locator.getBindAddress().getHostName();
    }

    info.setHostName(hostName);

    if (StringUtils.isNotBlank(config.getSecurityManager())) {
      info.setSecured(true);
    }

    SSLConfig sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(config, SecurableCommunicationChannel.WEB);
    info.setSSL(sslConfig.isEnabled());

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
