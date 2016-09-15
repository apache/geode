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
package org.apache.geode.internal.cache.wan.parallel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.BucketRegionQueue;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.PartitionMessage;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * Removes a batch of events from the remote secondary queues
 * 
 * 
 * @since GemFire 7.0
 * 
 */
public class ParallelQueueBatchRemovalMessage  extends PartitionMessage {

  private static final Logger logger = LogService.getLogger();
  
  private Map<Integer, List> bucketToTailKey;

  public ParallelQueueBatchRemovalMessage() {

  }

  public ParallelQueueBatchRemovalMessage(
      Set<InternalDistributedMember> recipient, int regionId,
      ReplyProcessor21 processor, Map bucketToTailKey) {
    super(recipient, regionId, processor);
    this.bucketToTailKey = bucketToTailKey;
  }

  @Override
  public int getDSFID() {
    return PARALLEL_QUEUE_BATCH_REMOVAL_MESSAGE;
  }

  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm,
      PartitionedRegion pr, long startTime) throws CacheException {
    for (Integer bucketId : this.bucketToTailKey.keySet()) {
      if (pr.getRegionAdvisor().getBucketAdvisor(bucketId).isHosting()) {
        List dispatchedKeys = this.bucketToTailKey.get(bucketId);
        try {
          BucketRegionQueue bucketRegionQueue = (BucketRegionQueue)pr
              .getDataStore().getInitializedBucketForId(null, bucketId);

          for (Object key : dispatchedKeys) {
            try {
              for (GatewayEventFilter filter : pr.getParallelGatewaySender()
                  .getGatewayEventFilters()) {
                GatewayQueueEvent eventForFilter = (GatewayQueueEvent)bucketRegionQueue
                    .get(key);
                try {
                  if (eventForFilter != null) {
                    filter.afterAcknowledgement(eventForFilter);
                  }
                }
                catch (Exception e) {
                  logger
                      .fatal(
                          LocalizedMessage
                              .create(
                                  LocalizedStrings.GatewayEventFilter_EXCEPTION_OCCURED_WHILE_HANDLING_CALL_TO_0_AFTER_ACKNOWLEDGEMENT_FOR_EVENT_1,
                                  new Object[] { filter.toString(),
                                      eventForFilter }), e);
                }
              }
              bucketRegionQueue.destroyKey(key);
            }
            catch (EntryNotFoundException e) {
              if (logger.isDebugEnabled()) {
                logger.debug("WARNING! Got EntryNotFoundException while destroying the key {} for bucket {}", key, bucketId);
              }
            }
          }
        }
        catch (ForceReattemptException fe) {
          if (logger.isDebugEnabled()) {
            logger.debug("Got ForceReattemptException while getting bucket {} to destroyLocally the keys.", bucketId);
          }
        }
      }
    }
    BatchRemovalReplyMessage.sendWithException(getSender(), getProcessorId(),
        dm, null);
    return false;
  }

  public static ParallelQueueBatchRemovalResponse send(
      Set<InternalDistributedMember> recipients, PartitionedRegion pr,
      Map<Integer, List> bucketToTailKey) {
    Assert
        .assertTrue(recipients != null, "BatchRemovalResponse NULL recipient");

    ParallelQueueBatchRemovalResponse response = new ParallelQueueBatchRemovalResponse(
        pr.getSystem(), recipients, pr);
    ParallelQueueBatchRemovalMessage msg = new ParallelQueueBatchRemovalMessage(
        recipients, pr.getPRId(), response, bucketToTailKey);

    Set<InternalDistributedMember> failures = pr.getDistributionManager()
        .putOutgoing(msg);

    if (failures != null && failures.size() > 0) {
      // throw new ForceReattemptException("Failed sending <" + msg + ">");
      return null;
    }
    pr.getPrStats().incPartitionMessagesSent();
    return response;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(bucketToTailKey, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.bucketToTailKey = (Map)DataSerializer.readObject(in);
  }

  public static final class BatchRemovalReplyMessage extends ReplyMessage {
    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public BatchRemovalReplyMessage() {
    }

    public BatchRemovalReplyMessage(DataInput in) throws IOException,
        ClassNotFoundException {
      fromData(in);
    }

    private BatchRemovalReplyMessage(int processorId, ReplyException re) {
      setProcessorId(processorId);
      setException(re);
    }

    @Override
    public void process(DM dm, ReplyProcessor21 processor) {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.debug("BatchRemovalReplyMessage process invoking reply processor with processorId: {}", this.processorId);
      }

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.debug("BatchRemovalReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      if (logger.isDebugEnabled()) {
        logger.debug("{} processed {}", processor, this);
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    private static void sendWithException(InternalDistributedMember recipient,
        int processorId, DM dm, ReplyException re) {
      Assert.assertTrue(recipient != null,
          "BecomePrimaryBucketReplyMessage NULL recipient");
      BatchRemovalReplyMessage m = new BatchRemovalReplyMessage(processorId, re);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
    }

    @Override
    public int getDSFID() {
      return PARALLEL_QUEUE_BATCH_REMOVAL_REPLY;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("BatchRemovalReplyMessage ").append("processorid=")
          .append(this.processorId).append(" reply to sender ")
          .append(this.getSender());
      return sb.toString();
    }
  }

  public static class ParallelQueueBatchRemovalResponse extends
      PartitionResponse {

    public ParallelQueueBatchRemovalResponse(InternalDistributedSystem dm,
        Set<InternalDistributedMember> recipients) {
      super(dm, recipients);
    }

    public ParallelQueueBatchRemovalResponse(InternalDistributedSystem ds,
        Set<InternalDistributedMember> recipients, PartitionedRegion theRegion) {
      super(ds, recipients);
    }

    @Override
    public void process(DistributionMessage msg) {
      super.process(msg);
    }

    public void waitForResponse() throws ForceReattemptException {
      try {
        waitForCacheException();
      }
      catch (EntryNotFoundException enfe) {
        throw enfe;
      }
      catch (ForceReattemptException e) {
        final String msg = "GetResponse got ForceReattemptException; rethrowing";
        logger.debug(msg, e);
        throw e;
      }
      catch (TransactionDataNotColocatedException e) {
        // Throw this up to user!
        throw e;
      }
    }
  }
}
