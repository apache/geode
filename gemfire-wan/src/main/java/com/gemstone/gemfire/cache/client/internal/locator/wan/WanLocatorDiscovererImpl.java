/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.client.internal.locator.wan;

import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.cache.client.internal.locator.wan.LocatorDiscovery;
import com.gemstone.gemfire.cache.client.internal.locator.wan.LocatorMembershipListener;
import com.gemstone.gemfire.cache.client.internal.locator.wan.RemoteLocatorJoinRequest;
import com.gemstone.gemfire.distributed.internal.WanLocatorDiscoverer;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;

public class WanLocatorDiscovererImpl implements WanLocatorDiscoverer{

  private static final Logger logger = LogService.getLogger();
  
  private ExecutorService _executor;
  
  public WanLocatorDiscovererImpl() {
    
  }
  
  @Override
  public void discover(int port, DistributionConfigImpl config, LocatorMembershipListener locatorListener) {
    final LoggingThreadGroup loggingThreadGroup = LoggingThreadGroup
        .createThreadGroup("WAN Locator Discovery Logger Group", logger);

    final ThreadFactory threadFactory = new ThreadFactory() {
      public Thread newThread(final Runnable task) {
        final Thread thread = new Thread(loggingThreadGroup, task,
            "WAN Locator Discovery Thread");
        thread.setDaemon(true);
        return thread;
      }
    };

    this._executor = Executors.newCachedThreadPool(threadFactory);
    exchangeLocalLocators(port, config, locatorListener);
    exchangeRemoteLocators(port, config, locatorListener);
    this._executor.shutdown();
  }
  
  
  /**
   * For WAN 70 Exchange the locator information within the distributed system
   * 
   * @param config
   */
  private void exchangeLocalLocators(int port, DistributionConfigImpl config, LocatorMembershipListener locatorListener) {
    String localLocator = config.getStartLocator();
    DistributionLocatorId locatorId = null;
    if (localLocator.equals(DistributionConfig.DEFAULT_START_LOCATOR)) {
      locatorId = new DistributionLocatorId(port, config.getBindAddress());
    }
    else {
      locatorId = new DistributionLocatorId(localLocator);
    }
    LocatorHelper.addLocator(config.getDistributedSystemId(), locatorId, locatorListener, null);

    RemoteLocatorJoinRequest request = buildRemoteDSJoinRequest(port, config);
    StringTokenizer locatorsOnThisVM = new StringTokenizer(
        config.getLocators(), ",");
    while (locatorsOnThisVM.hasMoreTokens()) {
      DistributionLocatorId localLocatorId = new DistributionLocatorId(
          locatorsOnThisVM.nextToken());
      if (!locatorId.equals(localLocatorId)) {
        LocatorDiscovery localDiscovery = new LocatorDiscovery(localLocatorId, request, locatorListener);
        LocatorDiscovery.LocalLocatorDiscovery localLocatorDiscovery = localDiscovery.new LocalLocatorDiscovery();
        this._executor.execute(localLocatorDiscovery);
      }
    }
  }
  
  /**
   * For WAN 70 Exchange the locator information across the distributed systems
   * (sites)
   * 
   * @param config
   */
  private void exchangeRemoteLocators(int port, DistributionConfigImpl config, LocatorMembershipListener locatorListener) {
    RemoteLocatorJoinRequest request = buildRemoteDSJoinRequest(port, config);
    String remoteDustributedSystems = config.getRemoteLocators();
    if (remoteDustributedSystems.length() > 0) {
      StringTokenizer remoteLocators = new StringTokenizer(
          remoteDustributedSystems, ",");
      while (remoteLocators.hasMoreTokens()) {
        DistributionLocatorId remoteLocatorId = new DistributionLocatorId(
            remoteLocators.nextToken());
        LocatorDiscovery localDiscovery = new LocatorDiscovery(remoteLocatorId,
            request, locatorListener);
        LocatorDiscovery.RemoteLocatorDiscovery remoteLocatorDiscovery = localDiscovery.new RemoteLocatorDiscovery();
        this._executor.execute(remoteLocatorDiscovery);
      }
    }
  }
  
  private RemoteLocatorJoinRequest buildRemoteDSJoinRequest(int port,
      DistributionConfigImpl config) {
    String localLocator = config.getStartLocator();
    DistributionLocatorId locatorId = null;
    if (localLocator.equals(DistributionConfig.DEFAULT_START_LOCATOR)) {
      locatorId = new DistributionLocatorId(port, config.getBindAddress());
    }
    else {
      locatorId = new DistributionLocatorId(localLocator);
    }
    RemoteLocatorJoinRequest request = new RemoteLocatorJoinRequest(
        config.getDistributedSystemId(), locatorId, "");
    return request;
  }
  
}
