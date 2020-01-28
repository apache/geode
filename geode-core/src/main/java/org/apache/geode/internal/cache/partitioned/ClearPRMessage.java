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
import org.apache.geode.distributed.DistributedLockService;
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
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.RegionEventImpl;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class ClearPRMessage extends PartitionMessageWithDirectReply {
  private static final Logger logger = LogService.getLogger();

  private RegionEventImpl regionEvent;

  private Integer bucketId;

  /** The time in ms to wait for a lock to be obtained during doLocalClear() */
  public static final int LOCK_WAIT_TIMEOUT_MS = 1000;
  public static final String BUCKET_NON_PRIMARY_MESSAGE =
      "The bucket region on target member is no longer primary";
  public static final String BUCKET_REGION_LOCK_UNAVAILABLE_MESSAGE =
      "A lock for the bucket region could not be obtained.";
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

  public ClearPRMessage(int bucketId) {
    this.bucketId = bucketId;

    // These are both used by the parent class, but don't apply to this message type
    this.notificationOnly = false;
    this.posDup = false;
  }

  public void setRegionEvent(RegionEventImpl event) {
    regionEvent = event;
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

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  public RegionEventImpl getRegionEvent() {
    return regionEvent;
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
      throw new ForceReattemptException("Failed sending <" + this + ">");
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
    DataSerializer.writeObject(regionEvent, out);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.bucketId = (int) InternalDataSerializer.readSignedVL(in);
    this.regionEvent = DataSerializer.readObject(in);
  }

  @Override
  public EventID getEventID() {
    return regionEvent.getEventId();
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
      result = doLocalClear(region);
    } catch (ForceReattemptException ex) {
      sendReply(getSender(), getProcessorId(), distributionManager, new ReplyException(ex), region,
          startTime);
      return false;
    }
    sendReply(getSender(), getProcessorId(), distributionManager, null, region, startTime);
    return false;
  }

  public boolean doLocalClear(PartitionedRegion region) throws ForceReattemptException {
    // Retrieve local bucket region which matches target bucketId
    BucketRegion bucketRegion = region.getDataStore().getInitializedBucketForId(null, bucketId);

    // Check if we are primary, throw exception if not
    if (!bucketRegion.isPrimary()) {
      throw new ForceReattemptException(BUCKET_NON_PRIMARY_MESSAGE);
    }

    DistributedLockService lockService = getPartitionRegionLockService();
    String lockName = bucketRegion.getFullPath();
    try {
      boolean locked = lockService.lock(lockName, LOCK_WAIT_TIMEOUT_MS, -1);

      if (!locked) {
        throw new ForceReattemptException(BUCKET_REGION_LOCK_UNAVAILABLE_MESSAGE);
      }

      // Double check if we are still primary, as this could have changed between our first check
      // and obtaining the lock
      if (!bucketRegion.isPrimary()) {
        throw new ForceReattemptException(BUCKET_NON_PRIMARY_MESSAGE);
      }

      try {
        bucketRegion.cmnClearRegion(regionEvent, true, true);
      } catch (Exception ex) {
        throw new ForceReattemptException(
            EXCEPTION_THROWN_DURING_CLEAR_OPERATION + ex.getClass().getName(), ex);
      }

    } finally {
      lockService.unlock(lockName);
    }

    return true;
  }

  // Extracted for testing
  protected DistributedLockService getPartitionRegionLockService() {
    return DistributedLockService
        .getServiceNamed(PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME);
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

  @Override
  public String toString() {
    StringBuilder buff = new StringBuilder();
    String className = getClass().getName();
    buff.append(className.substring(className.indexOf(PN_TOKEN) + PN_TOKEN.length())); // partition.<foo>
    buff.append("(prid="); // make sure this is the first one
    buff.append(this.regionId);

    // Append name, if we have it
    String name = null;
    try {
      PartitionedRegion region = PartitionedRegion.getPRFromId(this.regionId);
      if (region != null) {
        name = region.getFullPath();
      }
    } catch (Exception ignore) {
      /* ignored */
    }
    if (name != null) {
      buff.append(" (name = \"").append(name).append("\")");
    }

    appendFields(buff);
    buff.append(" ,distTx=");
    buff.append(this.isTransactionDistributed);
    buff.append(")");
    return buff.toString();
  }

  public static class ClearReplyMessage extends ReplyMessage {
    /** Result of the Clear operation */
    boolean result;

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
      this.result = result;
      setProcessorId(processorId);
      setException(ex);
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId,
        ReplySender replySender,
        boolean result, ReplyException ex) {
      Assert.assertTrue(recipient != null, "ClearReplyMessage NULL reply message");
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
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.result = in.readBoolean();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeBoolean(this.result);
    }

    @Override
    public String toString() {
      return "ClearReplyMessage " + "processorid=" + this.processorId + " returning " + this.result
          + " exception=" + getException();
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
      this.returnValue = response.result;
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
