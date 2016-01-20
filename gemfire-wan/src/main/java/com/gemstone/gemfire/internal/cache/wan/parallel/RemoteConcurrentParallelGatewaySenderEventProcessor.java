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
