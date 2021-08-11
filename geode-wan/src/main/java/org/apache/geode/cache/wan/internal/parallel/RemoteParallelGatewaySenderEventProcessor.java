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
package org.apache.geode.cache.wan.internal.parallel;

import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.internal.GatewaySenderEventRemoteDispatcher;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderEventProcessor;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class RemoteParallelGatewaySenderEventProcessor extends ParallelGatewaySenderEventProcessor {
  private static final Logger logger = LogService.getLogger();

  /**
   * use in concurrent scenario where queue is to be shared among all the processors.
   */
  protected RemoteParallelGatewaySenderEventProcessor(AbstractGatewaySender sender,
      Set<Region<?, ?>> userRegions, int id, int nDispatcher, ThreadsMonitoring tMonitoring,
      boolean cleanQueues) {
    super(sender, id, nDispatcher, tMonitoring, cleanQueues);
  }

  @Override
  protected void rebalance() {
    GatewaySenderStats statistics = sender.getStatistics();
    long startTime = statistics.startLoadBalance();
    try {
      if (dispatcher.isRemoteDispatcher()) {
        GatewaySenderEventRemoteDispatcher remoteDispatcher =
            (GatewaySenderEventRemoteDispatcher) dispatcher;
        if (remoteDispatcher.isConnectedToRemote()) {
          remoteDispatcher.stopAckReaderThread();
          remoteDispatcher.destroyConnection();
        }
      }
    } finally {
      statistics.endLoadBalance(startTime);
    }
  }

  @Override
  public void initializeEventDispatcher() {
    if (logger.isDebugEnabled()) {
      logger.debug(" Creating the GatewayEventRemoteDispatcher");
    }
    if (sender.getRemoteDSId() != GatewaySender.DEFAULT_DISTRIBUTED_SYSTEM_ID) {
      dispatcher = new GatewaySenderEventRemoteDispatcher(this);
    }
  }

}
