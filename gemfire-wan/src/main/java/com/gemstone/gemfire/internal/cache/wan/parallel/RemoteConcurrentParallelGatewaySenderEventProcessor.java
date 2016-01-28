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
package com.gemstone.gemfire.internal.cache.wan.parallel;


import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderStats;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventRemoteDispatcher;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderEventProcessor;
/**
 * Remote version of GatewaySenderEvent Processor
 * @author skumar
 *
 */
public class RemoteConcurrentParallelGatewaySenderEventProcessor extends ConcurrentParallelGatewaySenderEventProcessor{
  
  public RemoteConcurrentParallelGatewaySenderEventProcessor(
      AbstractGatewaySender sender) {
    super(sender);
  }

  @Override
  protected void createProcessors(int dispatcherThreads, Set<Region> targetRs) {
    processors = new RemoteParallelGatewaySenderEventProcessor[sender.getDispatcherThreads()];
    if (logger.isDebugEnabled()) {
      logger.debug("Creating GatewaySenderEventProcessor");
    }
    for (int i = 0; i < sender.getDispatcherThreads(); i++) {
      processors[i] = new RemoteParallelGatewaySenderEventProcessor(sender,
          targetRs, i, sender.getDispatcherThreads());
    }
  }
  
  @Override
  protected void rebalance() {
    GatewaySenderStats statistics = this.sender.getStatistics();
    long startTime = statistics.startLoadBalance();
    try {
      for (ParallelGatewaySenderEventProcessor parallelProcessor : this.processors) {
        GatewaySenderEventRemoteDispatcher remoteDispatcher = (GatewaySenderEventRemoteDispatcher)parallelProcessor.getDispatcher();
        if (remoteDispatcher.isConnectedToRemote()) {
          remoteDispatcher.stopAckReaderThread();
          remoteDispatcher.destroyConnection();
        }
      }
    } finally {
      statistics.endLoadBalance(startTime);
    }
  }
}
