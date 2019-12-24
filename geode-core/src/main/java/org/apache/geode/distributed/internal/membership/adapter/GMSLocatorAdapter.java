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
package org.apache.geode.distributed.internal.membership.adapter;

import static org.apache.geode.distributed.internal.membership.adapter.TcpSocketCreatorAdapter.asTcpSocketCreator;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.Distribution;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.LocatorStats;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.NetLocator;
import org.apache.geode.distributed.internal.membership.gms.api.Membership;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Locator;
import org.apache.geode.distributed.internal.membership.gms.locator.GMSLocator;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

public class GMSLocatorAdapter implements NetLocator {

  private final GMSLocator<InternalDistributedMember> gmsLocator;

  /**
   * @param bindAddress network address that TcpServer will bind to
   * @param locatorString location of other locators (bootstrapping, failover)
   * @param usePreferredCoordinators true if the membership coordinator should be a Locator
   * @param networkPartitionDetectionEnabled true if network partition detection is enabled
   * @param locatorStats the locator statistics object
   * @param securityUDPDHAlgo DF algorithm
   * @param workingDirectory directory to use for view file (defaults to "user.dir")
   */
  public GMSLocatorAdapter(InetAddress bindAddress, String locatorString,
      boolean usePreferredCoordinators,
      boolean networkPartitionDetectionEnabled, LocatorStats locatorStats,
      String securityUDPDHAlgo, Path workingDirectory) {
    final TcpClient locatorClient = new TcpClient(
        asTcpSocketCreator(
            SocketCreatorFactory
                .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR)),
        InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
        InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer());
    try {
      gmsLocator =
          new GMSLocator<>(bindAddress, locatorString, usePreferredCoordinators,
              networkPartitionDetectionEnabled,
              locatorStats, securityUDPDHAlgo, workingDirectory, locatorClient,
              InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
              InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer());
    } catch (MembershipConfigurationException e) {
      throw new GemFireConfigException(e.getMessage());
    }
  }

  @Override
  public Object processRequest(Object request) throws IOException {
    return gmsLocator.processRequest(request);
  }

  @Override
  public void endRequest(Object request, long startTime) {
    gmsLocator.endRequest(request, startTime);
  }

  @Override
  public void endResponse(Object request, long startTime) {
    gmsLocator.endResponse(request, startTime);
  }

  @Override
  public void shutDown() {
    gmsLocator.shutDown();
  }

  @Override
  public void restarting(DistributedSystem ds, GemFireCache cache,
      InternalConfigurationPersistenceService sharedConfig) {
    InternalDistributedSystem ids = (InternalDistributedSystem) ds;
    Distribution distribution = ids.getDM().getDistribution();
    gmsLocator.setMembership(distribution.getMembership());
  }

  @Override
  public void init(TcpServer tcpServer) {
    gmsLocator.init("" + tcpServer.getPort());
  }

  @Override
  public boolean setMembership(Membership<InternalDistributedMember> membership) {
    return gmsLocator.setMembership(membership);
  }

  public Locator getGMSLocator() {
    return gmsLocator;
  }
}
