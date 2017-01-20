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

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.*;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.*;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.WaitUntilGatewaySenderFlushedCoordinator;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public class WaitUntilParallelGatewaySenderFlushedCoordinator
    extends WaitUntilGatewaySenderFlushedCoordinator {

  public WaitUntilParallelGatewaySenderFlushedCoordinator(AbstractGatewaySender sender,
      long timeout, TimeUnit unit, boolean initiator) {
    super(sender, timeout, unit, initiator);
  }

  public boolean waitUntilFlushed() throws Throwable {
    boolean remoteResult = true, localResult = true;
    Throwable exceptionToThrow = null;
    ConcurrentParallelGatewaySenderQueue prq =
        (ConcurrentParallelGatewaySenderQueue) this.sender.getQueue();
    PartitionedRegion pr = (PartitionedRegion) prq.getRegion();

    // Create callables for local buckets
    List<WaitUntilBucketRegionQueueFlushedCallable> callables =
        buildWaitUntilBucketRegionQueueFlushedCallables(pr);

    // Submit local callables for execution
    ExecutorService service = this.sender.getDistributionManager().getWaitingThreadPool();
    List<Future<Boolean>> callableFutures = new ArrayList<>();
    for (Callable<Boolean> callable : callables) {
      callableFutures.add(service.submit(callable));
    }
    if (logger.isDebugEnabled()) {
      logger.debug("WaitUntilParallelGatewaySenderFlushedCoordinator: Created and submitted "
          + callables.size() + " callables=" + callables);
    }

    // Send message to remote buckets
    if (this.initiator) {
      remoteResult = false;
      try {
        remoteResult = waitUntilFlushedOnRemoteMembers(pr);
      } catch (Throwable t) {
        exceptionToThrow = t;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("WaitUntilParallelGatewaySenderFlushedCoordinator: Processed remote result="
            + remoteResult + "; exceptionToThrow=" + exceptionToThrow);
      }
    }

    // Process local future results
    for (Future<Boolean> future : callableFutures) {
      boolean singleBucketResult = false;
      try {
        singleBucketResult = future.get();
      } catch (ExecutionException e) {
        exceptionToThrow = e.getCause();
      }
      localResult = localResult && singleBucketResult;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("WaitUntilParallelGatewaySenderFlushedCoordinator: Processed local result="
          + localResult + "; exceptionToThrow=" + exceptionToThrow);
    }

    // Return the full result
    if (exceptionToThrow == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("WaitUntilParallelGatewaySenderFlushedCoordinator: Returning full result="
            + (remoteResult && localResult));
      }
      return remoteResult && localResult;
    } else {
      throw exceptionToThrow;
    }
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

  protected boolean waitUntilFlushedOnRemoteMembers(PartitionedRegion pr) throws Throwable {
    boolean result = true;
    DM dm = this.sender.getDistributionManager();
    Set<InternalDistributedMember> recipients = pr.getRegionAdvisor().adviseDataStore();
    if (!recipients.isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "WaitUntilParallelGatewaySenderFlushedCoordinator: About to send message recipients="
                + recipients);
      }
      WaitUntilGatewaySenderFlushedReplyProcessor processor =
          new WaitUntilGatewaySenderFlushedReplyProcessor(dm, recipients);
      WaitUntilGatewaySenderFlushedMessage message = new WaitUntilGatewaySenderFlushedMessage(
          recipients, processor.getProcessorId(), this.sender.getId(), this.timeout, this.unit);
      dm.putOutgoing(message);
      if (logger.isDebugEnabled()) {
        logger.debug("WaitUntilParallelGatewaySenderFlushedCoordinator: Sent message recipients="
            + recipients);
      }
      try {
        processor.waitForReplies();
        result = processor.getCombinedResult();
      } catch (ReplyException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("WaitUntilParallelGatewaySenderFlushedCoordinator: Caught e=" + e
              + "; cause=" + e.getCause());
        }
        throw e.getCause();
      } catch (InterruptedException e) {
        dm.getCancelCriterion().checkCancelInProgress(e);
        Thread.currentThread().interrupt();
        result = false;
      }
    }
    return result;
  }

  public static class WaitUntilBucketRegionQueueFlushedCallable implements Callable<Boolean> {

    private BucketRegionQueue brq;

    private long timeout;

    private TimeUnit unit;

    public WaitUntilBucketRegionQueueFlushedCallable(BucketRegionQueue brq, long timeout,
        TimeUnit unit) {
      this.brq = brq;
      this.timeout = timeout;
      this.unit = unit;
    }

    @Override
    public Boolean call() throws Exception {
      return this.brq.waitUntilFlushed(this.timeout, this.unit);
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

  public static class WaitUntilGatewaySenderFlushedMessage extends PooledDistributionMessage
      implements MessageWithReply {

    private int processorId;

    private String gatewaySenderId;

    private long timeout;

    private TimeUnit unit;

    /* For serialization */
    public WaitUntilGatewaySenderFlushedMessage() {}

    protected WaitUntilGatewaySenderFlushedMessage(Collection recipients, int processorId,
        String gatewaySenderId, long timeout, TimeUnit unit) {
      super();
      setRecipients(recipients);
      this.processorId = processorId;
      this.gatewaySenderId = gatewaySenderId;
      this.timeout = timeout;
      this.unit = unit;
    }

    @Override
    protected void process(DistributionManager dm) {
      boolean result = false;
      ReplyException replyException = null;
      try {
        if (logger.isDebugEnabled()) {
          logger.debug("WaitUntilGatewaySenderFlushedMessage: Processing gatewaySenderId="
              + this.gatewaySenderId + "; timeout=" + this.timeout + "; unit=" + this.unit);
        }
        Cache cache = GemFireCacheImpl.getInstance();
        if (cache != null) {
          AbstractGatewaySender sender =
              (AbstractGatewaySender) cache.getGatewaySender(this.gatewaySenderId);
          if (sender != null) {
            try {
              WaitUntilParallelGatewaySenderFlushedCoordinator coordinator =
                  new WaitUntilParallelGatewaySenderFlushedCoordinator(sender, this.timeout,
                      this.unit, false);
              result = coordinator.waitUntilFlushed();
            } catch (Throwable e) {
              replyException = new ReplyException(e);
            }
          }
        }
      } finally {
        ReplyMessage replyMsg = new ReplyMessage();
        replyMsg.setRecipient(getSender());
        replyMsg.setProcessorId(this.processorId);
        if (replyException == null) {
          replyMsg.setReturnValue(result);
        } else {
          replyMsg.setException(replyException);
        }
        if (logger.isDebugEnabled()) {
          logger.debug("WaitUntilGatewaySenderFlushedMessage: Sending reply returnValue="
              + replyMsg.getReturnValue() + "; exception=" + replyMsg.getException());
        }
        dm.putOutgoing(replyMsg);
      }
    }

    @Override
    public int getDSFID() {
      return WAIT_UNTIL_GATEWAY_SENDER_FLUSHED_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.processorId);
      DataSerializer.writeString(this.gatewaySenderId, out);
      out.writeLong(this.timeout);
      DataSerializer.writeEnum(this.unit, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
      this.gatewaySenderId = DataSerializer.readString(in);
      this.timeout = in.readLong();
      this.unit = DataSerializer.readEnum(TimeUnit.class, in);
    }
  }
}
