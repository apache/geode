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
package org.apache.geode.internal.cache.wan.parallel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.BucketRegionQueue;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.WaitUntilGatewaySenderFlushedCoordinator;

public class WaitUntilParallelGatewaySenderFlushedCoordinator
    extends WaitUntilGatewaySenderFlushedCoordinator {
  private static final int CALLABLES_CHUNK_SIZE = 10;

  public WaitUntilParallelGatewaySenderFlushedCoordinator(AbstractGatewaySender sender,
      long timeout, TimeUnit unit, boolean initiator) {
    super(sender, timeout, unit, initiator);
  }

  @Override
  public boolean waitUntilFlushed() throws Throwable {
    boolean localResult = true;
    Throwable exceptionToThrow = null;
    ConcurrentParallelGatewaySenderQueue prq =
        (ConcurrentParallelGatewaySenderQueue) this.sender.getQueue();
    PartitionedRegion pr = (PartitionedRegion) prq.getRegion();
    if (pr == null) {
      sender.getCancelCriterion().checkCancelInProgress(null);
    }

    ExecutorService service =
        this.sender.getDistributionManager().getExecutors().getWaitingThreadPool();
    List<Future<Boolean>> callableFutures = new ArrayList<>();
    int callableCount = 0;
    long nanosRemaining = unit.toNanos(timeout);
    long endTime = System.nanoTime() + nanosRemaining;
    Set<BucketRegion> localBucketRegions = getLocalBucketRegions(pr);
    for (BucketRegion br : localBucketRegions) {
      // timeout exceeded, do not submit more callables, return localResult false
      if (System.nanoTime() >= endTime) {
        localResult = false;
        break;
      }
      // create and submit callable with updated timeout
      Callable<Boolean> callable = createWaitUntilBucketRegionQueueFlushedCallable(
          (BucketRegionQueue) br, nanosRemaining, TimeUnit.NANOSECONDS);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "WaitUntilParallelGatewaySenderFlushedCoordinator: Submitting callable for bucket "
                + br.getId() + " callable=" + callable + " nanosRemaining=" + nanosRemaining);
      }
      callableFutures.add(service.submit(callable));
      callableCount++;
      if ((callableCount % CALLABLES_CHUNK_SIZE) == 0
          || callableCount == localBucketRegions.size()) {
        CallablesChunkResults callablesChunkResults =
            new CallablesChunkResults(localResult, exceptionToThrow, callableFutures).invoke();
        localResult = callablesChunkResults.getLocalResult();
        exceptionToThrow = callablesChunkResults.getExceptionToThrow();
        if (logger.isDebugEnabled()) {
          logger.debug("WaitUntilParallelGatewaySenderFlushedCoordinator: Processed local result= "
              + localResult + "; exceptionToThrow= " + exceptionToThrow);
        }
        if (exceptionToThrow != null) {
          throw exceptionToThrow;
        }
      }
      nanosRemaining = endTime - System.nanoTime();
    }

    // Return the full result
    if (logger.isDebugEnabled()) {
      logger.debug("WaitUntilParallelGatewaySenderFlushedCoordinator: Returning full result="
          + (localResult));
    }
    return localResult;
  }

  protected Set<BucketRegion> getLocalBucketRegions(PartitionedRegion pr) {
    Set<BucketRegion> localBucketRegions = new HashSet<BucketRegion>();
    if (pr.isDataStore()) {
      localBucketRegions = pr.getDataStore().getAllLocalBucketRegions();
    }
    return localBucketRegions;
  }

  protected WaitUntilBucketRegionQueueFlushedCallable createWaitUntilBucketRegionQueueFlushedCallable(
      BucketRegionQueue br, long timeout, TimeUnit unit) {
    return new WaitUntilBucketRegionQueueFlushedCallable(br, timeout, unit);
  }

  public static class WaitUntilBucketRegionQueueFlushedCallable implements Callable<Boolean> {

    private BucketRegionQueue brq;

    private long latestQueuedKey;

    private long timeout;

    private TimeUnit unit;

    public WaitUntilBucketRegionQueueFlushedCallable(BucketRegionQueue brq, long timeout,
        TimeUnit unit) {
      this.brq = brq;
      this.latestQueuedKey = brq.getLatestQueuedKey();
      this.timeout = timeout;
      this.unit = unit;
    }

    @Override
    public Boolean call() throws Exception {
      return this.brq.waitUntilFlushed(this.latestQueuedKey, this.timeout, this.unit);
    }

    @Override
    public String toString() {
      return new StringBuilder().append(getClass().getSimpleName()).append("[").append("brq=")
          .append(this.brq.getId()).append("]").toString();
    }
  }

  public static class WaitUntilGatewaySenderFlushedReplyProcessor extends ReplyProcessor21 {

    private Map<DistributedMember, Boolean> responses;

    public WaitUntilGatewaySenderFlushedReplyProcessor(DistributionManager dm,
        Collection initMembers) {
      super(dm, initMembers);
      initializeResponses();
    }

    private void initializeResponses() {
      this.responses = new ConcurrentHashMap<>();
      for (InternalDistributedMember member : getMembers()) {
        this.responses.put(member, false);
      }
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof ReplyMessage) {
          ReplyMessage reply = (ReplyMessage) msg;
          if (logger.isDebugEnabled()) {
            logger
                .debug("WaitUntilGatewaySenderFlushedReplyProcessor: Processing reply from sender="
                    + reply.getSender() + "; returnValue=" + reply.getReturnValue() + "; exception="
                    + reply.getException());
          }
          if (reply.getException() == null) {
            this.responses.put(reply.getSender(), (Boolean) reply.getReturnValue());
          } else {
            reply.getException().printStackTrace();
          }
        }
      } finally {
        super.process(msg);
      }
    }

    public boolean getCombinedResult() {
      boolean combinedResult = true;
      for (boolean singleMemberResult : this.responses.values()) {
        combinedResult = combinedResult && singleMemberResult;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("WaitUntilGatewaySenderFlushedReplyProcessor: Returning combinedResult="
            + combinedResult);
      }
      return combinedResult;
    }
  }

  private class CallablesChunkResults {
    private boolean localResult;
    private Throwable exceptionToThrow;
    private List<Future<Boolean>> callableFutures;

    public CallablesChunkResults(boolean localResult, Throwable exceptionToThrow,
        List<Future<Boolean>> callableFutures) {
      this.localResult = localResult;
      this.exceptionToThrow = exceptionToThrow;
      this.callableFutures = callableFutures;
    }

    public boolean getLocalResult() {
      return localResult;
    }

    public Throwable getExceptionToThrow() {
      return exceptionToThrow;
    }

    public CallablesChunkResults invoke() throws InterruptedException {
      for (Future<Boolean> future : callableFutures) {
        boolean singleBucketResult = false;
        try {
          singleBucketResult = future.get();
        } catch (ExecutionException e) {
          exceptionToThrow = e.getCause();
        }
        localResult = localResult && singleBucketResult;
      }
      callableFutures.clear();
      return this;
    }
  }
}
