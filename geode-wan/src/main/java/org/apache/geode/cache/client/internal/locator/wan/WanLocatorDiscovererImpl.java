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
package org.apache.geode.cache.client.internal.locator.wan;

import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.WanLocatorDiscoverer;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingExecutors;

public class WanLocatorDiscovererImpl implements WanLocatorDiscoverer {

  private static final Logger logger = LogService.getLogger();

  private volatile boolean stopped = false;

  private ExecutorService _executor;

  public WanLocatorDiscovererImpl() {

  }

  @Override
  public void discover(int port, DistributionConfigImpl config,
      LocatorMembershipListener locatorListener, final String hostnameForClients) {
    this._executor = LoggingExecutors.newCachedThreadPool("WAN Locator Discovery Thread", true);
    exchangeLocalLocators(port, config, locatorListener, hostnameForClients);
    exchangeRemoteLocators(port, config, locatorListener, hostnameForClients);
    this._executor.shutdown();
  }

  @Override
  public void stop() {
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  /**
   * For WAN 70 Exchange the locator information within the distributed system
   *
   */
  private void exchangeLocalLocators(int port, DistributionConfigImpl config,
      LocatorMembershipListener locatorListener, final String hostnameForClients) {
    String localLocator = config.getStartLocator();
    DistributionLocatorId locatorId = null;
    if (localLocator.equals(DistributionConfig.DEFAULT_START_LOCATOR)) {
      locatorId = new DistributionLocatorId(port, config.getBindAddress(), hostnameForClients);
    } else {
      locatorId = new DistributionLocatorId(localLocator);
    }
    LocatorHelper.addLocator(config.getDistributedSystemId(), locatorId, locatorListener, null);

    RemoteLocatorJoinRequest request = buildRemoteDSJoinRequest(port, config, hostnameForClients);
    StringTokenizer locatorsOnThisVM = new StringTokenizer(config.getLocators(), ",");
    while (locatorsOnThisVM.hasMoreTokens()) {
      DistributionLocatorId localLocatorId =
          new DistributionLocatorId(locatorsOnThisVM.nextToken());
      if (!locatorId.equals(localLocatorId)) {
        LocatorDiscovery localDiscovery =
            new LocatorDiscovery(this, localLocatorId, request, locatorListener);
        LocatorDiscovery.LocalLocatorDiscovery localLocatorDiscovery =
            localDiscovery.new LocalLocatorDiscovery();
        this._executor.execute(localLocatorDiscovery);
      }
    }
  }

  /**
   * For WAN 70 Exchange the locator information across the distributed systems (sites)
   *
   */
  private void exchangeRemoteLocators(int port, DistributionConfigImpl config,
      LocatorMembershipListener locatorListener, final String hostnameForClients) {
    RemoteLocatorJoinRequest request = buildRemoteDSJoinRequest(port, config, hostnameForClients);
    String remoteDistributedSystems = config.getRemoteLocators();
    if (remoteDistributedSystems.length() > 0) {
      StringTokenizer remoteLocators = new StringTokenizer(remoteDistributedSystems, ",");
      while (remoteLocators.hasMoreTokens()) {
        DistributionLocatorId remoteLocatorId =
            new DistributionLocatorId(remoteLocators.nextToken());
        LocatorDiscovery localDiscovery =
            new LocatorDiscovery(this, remoteLocatorId, request, locatorListener);
        LocatorDiscovery.RemoteLocatorDiscovery remoteLocatorDiscovery =
            localDiscovery.new RemoteLocatorDiscovery();
        this._executor.execute(remoteLocatorDiscovery);
      }
    }
  }

  private RemoteLocatorJoinRequest buildRemoteDSJoinRequest(int port, DistributionConfigImpl config,
      final String hostnameForClients) {
    String localLocator = config.getStartLocator();
    DistributionLocatorId locatorId = null;
    if (localLocator.equals(DistributionConfig.DEFAULT_START_LOCATOR)) {
      locatorId = new DistributionLocatorId(port, config.getBindAddress(), hostnameForClients);
    } else {
      locatorId = new DistributionLocatorId(localLocator);
    }
    RemoteLocatorJoinRequest request =
        new RemoteLocatorJoinRequest(config.getDistributedSystemId(), locatorId, "");
    return request;
  }

}
