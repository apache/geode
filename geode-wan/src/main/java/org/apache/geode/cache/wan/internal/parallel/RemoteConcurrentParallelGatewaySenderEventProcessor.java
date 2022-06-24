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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.internal.GatewaySenderEventRemoteDispatcher;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderEventProcessor;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;

/**
 * Remote version of GatewaySenderEvent Processor
 *
 */
public class RemoteConcurrentParallelGatewaySenderEventProcessor
    extends ConcurrentParallelGatewaySenderEventProcessor {

  public RemoteConcurrentParallelGatewaySenderEventProcessor(AbstractGatewaySender sender,
      ThreadsMonitoring tMonitoring, boolean cleanQueues) {
    super(sender, tMonitoring, cleanQueues);
  }

  @Override
  protected void createProcessors(int dispatcherThreads, Set<Region<?, ?>> targetRs,
      boolean cleanQueues) {
    processors = new RemoteParallelGatewaySenderEventProcessor[sender.getDispatcherThreads()];
    if (logger.isDebugEnabled()) {
      logger.debug("Creating GatewaySenderEventProcessor");
    }
    for (int i = 0; i < sender.getDispatcherThreads(); i++) {
      processors[i] = createRemoteParallelGatewaySenderEventProcessor(sender, i,
          sender.getDispatcherThreads(), getThreadMonitorObj(), cleanQueues);
    }
  }

  protected ParallelGatewaySenderEventProcessor createRemoteParallelGatewaySenderEventProcessor(
      final @NotNull AbstractGatewaySender sender, final int id,
      final int dispatcherThreads, final @Nullable ThreadsMonitoring threadsMonitoring,
      final boolean cleanQueues) {
    return new RemoteParallelGatewaySenderEventProcessor(sender, id, dispatcherThreads,
        threadsMonitoring, cleanQueues);
  }


  @Override
  protected void rebalance() {
    GatewaySenderStats statistics = sender.getStatistics();
    long startTime = statistics.startLoadBalance();
    try {
      for (ParallelGatewaySenderEventProcessor parallelProcessor : processors) {
        GatewaySenderEventRemoteDispatcher remoteDispatcher =
            (GatewaySenderEventRemoteDispatcher) parallelProcessor.getDispatcher();
        if (remoteDispatcher.isConnectedToRemote()) {
          remoteDispatcher.stopAckReaderThread();
          remoteDispatcher.destroyConnection();
        }
      }
    } finally {
      statistics.endLoadBalance(startTime);
    }
  }

  private ThreadsMonitoring getThreadMonitorObj() {
    DistributionManager distributionManager = sender.getDistributionManager();
    if (distributionManager != null) {
      return distributionManager.getThreadMonitoring();
    } else {
      return null;
    }
  }
}
