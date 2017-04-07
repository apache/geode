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

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.*;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.*;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.WaitUntilGatewaySenderFlushedCoordinator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class WaitUntilParallelGatewaySenderFlushedCoordinator
    extends WaitUntilGatewaySenderFlushedCoordinator {
  final static private int CALLABLES_CHUNK_SIZE = 10;

  public WaitUntilParallelGatewaySenderFlushedCoordinator(AbstractGatewaySender sender,
      long timeout, TimeUnit unit, boolean initiator) {
    super(sender, timeout, unit, initiator);
  }

  public boolean waitUntilFlushed() throws Throwable {
    boolean localResult = true;
    Throwable exceptionToThrow = null;
    ConcurrentParallelGatewaySenderQueue prq =
        (ConcurrentParallelGatewaySenderQueue) this.sender.getQueue();
    PartitionedRegion pr = (PartitionedRegion) prq.getRegion();
    if (pr == null) {
      sender.getCancelCriterion().checkCancelInProgress(null);
    }

    // Create callables for local buckets
    List<WaitUntilBucketRegionQueueFlushedCallable> callables =
        buildWaitUntilBucketRegionQueueFlushedCallables(pr);

    // Submit local callables for execution
    ExecutorService service = this.sender.getDistributionManager().getWaitingThreadPool();
    List<Future<Boolean>> callableFutures = new ArrayList<>();
    int callableCount = 0;
    if (logger.isDebugEnabled()) {
      logger.debug("WaitUntilParallelGatewaySenderFlushedCoordinator: Created and being submitted "
          + callables.size() + " callables=" + callables);
    }
    for (Callable<Boolean> callable : callables) {
      callableFutures.add(service.submit(callable));
      callableCount++;
      if ((callableCount % CALLABLES_CHUNK_SIZE) == 0 || callableCount == callables.size()) {
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
    }

    // Return the full result
    if (logger.isDebugEnabled()) {
      logger.debug("WaitUntilParallelGatewaySenderFlushedCoordinator: Returning full result="
          + (localResult));
    }
    return localResult;
  }

  protected List<WaitUntilBucketRegionQueueFlushedCallable> buildWaitUntilBucketRegionQueueFlushedCallables(
      PartitionedRegion pr) {
    List<WaitUntilBucketRegionQueueFlushedCallable> callables = new ArrayList<>();
    if (pr.isDataStore()) {
      for (BucketRegion br : pr.getDataStore().getAllLocalBucketRegions()) {
        callables.add(new WaitUntilBucketRegionQueueFlushedCallable((BucketRegionQueue) br,
            this.timeout, this.unit));
      }
    }
    return callables;
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

    public WaitUntilGatewaySenderFlushedReplyProcessor(DM dm, Collection initMembers) {
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
