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

package org.apache.geode.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ReplySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEventImpl;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class ClearPRMessage extends PartitionMessageWithDirectReply {
  private static final Logger logger = LogService.getLogger();

  private Integer bucketId;

  private EventID eventID;

  public static final String BUCKET_NON_PRIMARY_MESSAGE =
      "The bucket region on target member is no longer primary";
  public static final String EXCEPTION_THROWN_DURING_CLEAR_OPERATION =
      "An exception was thrown during the local clear operation: ";

  /**
   * state from operateOnRegion that must be preserved for transmission from the waiting pool
   */
  transient boolean result = false;

  /**
   * Empty constructor to satisfy {@link DataSerializer}requirements
   */
  public ClearPRMessage() {}

  public ClearPRMessage(int bucketId, EventID eventID) {
    this.bucketId = bucketId;
    this.eventID = eventID;
  }

  public void initMessage(PartitionedRegion region, Set<InternalDistributedMember> recipients,
      DirectReplyProcessor replyProcessor) {
    this.resetRecipients();
    if (recipients != null) {
      setRecipients(recipients);
    }
    this.regionId = region.getPRId();
    this.processor = replyProcessor;
    this.processorId = replyProcessor == null ? 0 : replyProcessor.getProcessorId();
    if (replyProcessor != null) {
      replyProcessor.enableSevereAlertProcessing();
    }
  }

  public ClearResponse send(DistributedMember recipient, PartitionedRegion region)
      throws ForceReattemptException {
    Set<InternalDistributedMember> recipients =
        Collections.singleton((InternalDistributedMember) recipient);
    ClearResponse clearResponse = new ClearResponse(region.getSystem(), recipients);
    initMessage(region, recipients, clearResponse);
    if (logger.isDebugEnabled()) {
      logger.debug("ClearPRMessage.send: recipient is {}, msg is {}", recipient, this);
    }

    Set<InternalDistributedMember> failures = region.getDistributionManager().putOutgoing(this);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException("Failed sending <" + this + "> due to " + failures);
    }
    return clearResponse;
  }

  @Override
  public int getDSFID() {
    return PR_CLEAR_MESSAGE;
  }

  @Override
  public void toData(DataOutput out, SerializationContext context) throws IOException {
    super.toData(out, context);
    if (bucketId == null) {
      InternalDataSerializer.writeSignedVL(-1, out);
    } else {
      InternalDataSerializer.writeSignedVL(bucketId, out);
    }
    DataSerializer.writeObject(this.eventID, out);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.bucketId = (int) InternalDataSerializer.readSignedVL(in);
    this.eventID = (EventID) DataSerializer.readObject(in);
  }

  @Override
  public EventID getEventID() {
    return null;
  }

  /**
   * This method is called upon receipt and make the desired changes to the PartitionedRegion Note:
   * It is very important that this message does NOT cause any deadlocks as the sender will wait
   * indefinitely for the acknowledgement
   */
  @Override
  @VisibleForTesting
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager distributionManager,
      PartitionedRegion region, long startTime) {
    try {
      this.result = doLocalClear(region);
    } catch (ForceReattemptException ex) {
      sendReply(getSender(), getProcessorId(), distributionManager, new ReplyException(ex), region,
          startTime);
      return false;
    }
    return this.result;
  }

  public Integer getBucketId() {
    return this.bucketId;
  }

  public boolean doLocalClear(PartitionedRegion region)
      throws ForceReattemptException {
    // Retrieve local bucket region which matches target bucketId
    BucketRegion bucketRegion =
        region.getDataStore().getInitializedBucketForId(null, this.bucketId);

    boolean lockedForPrimary = bucketRegion.doLockForPrimary(false);
    // Check if we obtained primary lock, throw exception if not
    if (!lockedForPrimary) {
      throw new ForceReattemptException(BUCKET_NON_PRIMARY_MESSAGE);
    }
    try {
      RegionEventImpl regionEvent = new RegionEventImpl(bucketRegion, Operation.REGION_CLEAR, null,
          false, region.getMyId(), eventID);
      bucketRegion.cmnClearRegion(regionEvent, false, true);
    } catch (PartitionOfflineException poe) {
      logger.info(
          "All members holding data for bucket {} are offline, no more retries will be attempted",
          this.bucketId,
          poe);
      throw poe;
    } catch (Exception ex) {
      throw new ForceReattemptException(
          EXCEPTION_THROWN_DURING_CLEAR_OPERATION + ex.getClass().getName(), ex);
    } finally {
      bucketRegion.doUnlockForPrimary();
    }

    return true;
  }

  @Override
  public boolean canStartRemoteTransaction() {
    return false;
  }

  @Override
  protected void sendReply(InternalDistributedMember member, int processorId,
      DistributionManager distributionManager, ReplyException ex,
      PartitionedRegion partitionedRegion, long startTime) {
    if (partitionedRegion != null) {
      if (startTime > 0) {
        partitionedRegion.getPrStats().endPartitionMessagesProcessing(startTime);
      }
    }
    ClearReplyMessage.send(member, processorId, getReplySender(distributionManager), this.result,
        ex);
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; bucketId=").append(this.bucketId);
  }

  public static class ClearReplyMessage extends ReplyMessage {
    @Override
    public boolean getInlineProcess() {
      return true;
    }

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    @SuppressWarnings("unused")
    public ClearReplyMessage() {}

    private ClearReplyMessage(int processorId, boolean result, ReplyException ex) {
      super();
      setProcessorId(processorId);
      if (ex != null) {
        setException(ex);
      } else {
        setReturnValue(result);
      }
    }

    /**
     * Send an ack
     */
    public static void send(InternalDistributedMember recipient, int processorId,
        ReplySender replySender,
        boolean result, ReplyException ex) {
      Assert.assertNotNull(recipient, "ClearReplyMessage recipient was NULL.");
      ClearReplyMessage message = new ClearReplyMessage(processorId, result, ex);
      message.setRecipient(recipient);
      replySender.putOutgoing(message);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the message.
     *
     * @param distributionManager the distribution manager that is processing the message.
     */
    @Override
    public void process(final DistributionManager distributionManager,
        final ReplyProcessor21 replyProcessor) {
      final long startTime = getTimestamp();
      if (replyProcessor == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "{}: processor not found", this);
        }
        return;
      }
      if (replyProcessor instanceof ClearResponse) {
        ((ClearResponse) replyProcessor).setResponse(this);
      }
      replyProcessor.process(this);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} processed {}", replyProcessor, this);
      }
      distributionManager.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return PR_CLEAR_REPLY_MESSAGE;
    }

    @Override
    public String toString() {
      StringBuilder stringBuilder = new StringBuilder(super.toString());
      stringBuilder.append(" returnValue=");
      stringBuilder.append(getReturnValue());
      return stringBuilder.toString();
    }
  }

  /**
   * A processor to capture the value returned by {@link ClearPRMessage}
   */
  public static class ClearResponse extends PartitionResponse {
    private volatile boolean returnValue;

    public ClearResponse(InternalDistributedSystem distributedSystem,
        Set<InternalDistributedMember> recipients) {
      super(distributedSystem, recipients, false);
    }

    public void setResponse(ClearReplyMessage response) {
      if (response.getException() == null) {
        this.returnValue = (boolean) response.getReturnValue();
      }
    }

    /**
     * @return the result of the remote clear operation
     * @throws ForceReattemptException if the peer is no longer available
     * @throws CacheException if the peer generates an error
     */
    public boolean waitForResult() throws CacheException, ForceReattemptException {
      waitForCacheException();
      return this.returnValue;
    }
  }
}
