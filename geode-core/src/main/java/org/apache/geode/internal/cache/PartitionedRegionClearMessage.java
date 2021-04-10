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
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ReplySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.CopyOnWriteHashSet;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.partitioned.PartitionMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class PartitionedRegionClearMessage extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();

  public enum OperationType {
    OP_LOCK_FOR_PR_CLEAR, OP_UNLOCK_FOR_PR_CLEAR, OP_PR_CLEAR,
  }

  private Object callbackArgument;
  private OperationType operationType;
  private EventID eventId;
  private PartitionedRegion partitionedRegion;
  private Set<Integer> bucketsCleared;

  public PartitionedRegionClearMessage() {
    // nothing
  }

  PartitionedRegionClearMessage(Set<InternalDistributedMember> recipients,
      PartitionedRegion partitionedRegion, ReplyProcessor21 replyProcessor21,
      PartitionedRegionClearMessage.OperationType operationType, RegionEventImpl regionEvent) {
    super(recipients, partitionedRegion.getPRId(), replyProcessor21);
    this.partitionedRegion = partitionedRegion;
    this.operationType = operationType;
    callbackArgument = regionEvent.getRawCallbackArgument();
    eventId = regionEvent.getEventId();
  }

  @Override
  public EventID getEventID() {
    return eventId;
  }

  public OperationType getOperationType() {
    return operationType;
  }

  public void send() {
    Objects.requireNonNull(getRecipients(), "ClearMessage NULL recipients set");

    setTransactionDistributed(partitionedRegion.getCache().getTxManager().isDistributed());
    partitionedRegion.getDistributionManager().putOutgoing(this);
  }

  @Override
  protected Throwable processCheckForPR(PartitionedRegion partitionedRegion,
      DistributionManager distributionManager) {
    if (partitionedRegion != null && !partitionedRegion.getDistributionAdvisor().isInitialized()) {
      return new ForceReattemptException(
          String.format("%s : could not find partitioned region with Id %s",
              distributionManager.getDistributionManagerId(),
              partitionedRegion.getRegionIdentifier()));
    }
    return null;
  }

  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager distributionManager,
      PartitionedRegion partitionedRegion, long startTime) throws CacheException {
    if (partitionedRegion == null) {
      return true;
    }
    if (partitionedRegion.isDestroyed()) {
      return true;
    }

    if (operationType == OperationType.OP_LOCK_FOR_PR_CLEAR) {
      partitionedRegion.getPartitionedRegionClear().obtainClearLockLocal(getSender());
    } else if (operationType == OperationType.OP_UNLOCK_FOR_PR_CLEAR) {
      partitionedRegion.getPartitionedRegionClear().releaseClearLockLocal();
    } else {
      RegionEventImpl event =
          new RegionEventImpl(partitionedRegion, Operation.REGION_CLEAR, callbackArgument, true,
              partitionedRegion.getMyId(), getEventID());
      bucketsCleared = partitionedRegion.getPartitionedRegionClear().clearRegionLocal(event);
    }
    return true;
  }

  @Override
  protected void appendFields(StringBuilder stringBuilder) {
    super.appendFields(stringBuilder);
    stringBuilder
        .append(" cbArg=")
        .append(callbackArgument)
        .append(" op=")
        .append(operationType);
  }

  @Override
  public int getDSFID() {
    return CLEAR_PARTITIONED_REGION_MESSAGE;
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    callbackArgument = DataSerializer.readObject(in);
    operationType = PartitionedRegionClearMessage.OperationType.values()[in.readByte()];
    eventId = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out, SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(callbackArgument, out);
    out.writeByte(operationType.ordinal());
    DataSerializer.writeObject(eventId, out);
  }

  @Override
  protected void sendReply(InternalDistributedMember recipient, int processorId,
      DistributionManager distributionManager, ReplyException replyException,
      PartitionedRegion partitionedRegion, long startTime) {
    if (partitionedRegion != null && startTime > 0) {
      partitionedRegion.getPrStats().endPartitionMessagesProcessing(startTime);
    }
    PartitionedRegionClearMessage.PartitionedRegionClearReplyMessage
        .send(recipient, processorId, getReplySender(distributionManager), operationType,
            bucketsCleared, replyException);
  }

  /**
   * The response on which to wait for all the replies. This response ignores any exceptions
   * received from the "far side"
   */
  public static class PartitionedRegionClearResponse extends ReplyProcessor21 {

    CopyOnWriteHashSet<Integer> bucketsCleared = new CopyOnWriteHashSet<>();

    public PartitionedRegionClearResponse(InternalDistributedSystem system,
        Set<InternalDistributedMember> recipients) {
      super(system, recipients);
    }

    @Override
    public void process(DistributionMessage message) {
      if (message instanceof PartitionedRegionClearReplyMessage) {
        Set<Integer> buckets = ((PartitionedRegionClearReplyMessage) message).bucketsCleared;
        if (buckets != null) {
          bucketsCleared.addAll(buckets);
        }
      }
      process(message, true);
    }
  }

  public static class PartitionedRegionClearReplyMessage extends ReplyMessage {

    private Set<Integer> bucketsCleared;

    private OperationType operationType;

    @Override
    public boolean getInlineProcess() {
      return true;
    }

    public static void send(InternalDistributedMember recipient, int processorId,
        ReplySender replySender, OperationType operationType, Set<Integer> bucketsCleared,
        ReplyException replyException) {
      Objects.requireNonNull(recipient, "partitionedRegionClearReplyMessage NULL reply message");

      PartitionedRegionClearMessage.PartitionedRegionClearReplyMessage replyMessage =
          new PartitionedRegionClearMessage.PartitionedRegionClearReplyMessage(processorId,
              operationType, bucketsCleared, replyException);

      replyMessage.setRecipient(recipient);
      replySender.putOutgoing(replyMessage);
    }

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public PartitionedRegionClearReplyMessage() {
      // Empty constructor to conform to DataSerializable interface
    }

    private PartitionedRegionClearReplyMessage(int processorId, OperationType operationType,
        Set<Integer> bucketsCleared, ReplyException replyException) {
      this.bucketsCleared = bucketsCleared;
      this.operationType = operationType;
      setProcessorId(processorId);
      setException(replyException);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the message.
     *
     * @param distributionManager the distribution manager that is processing the message.
     */
    @Override
    public void process(DistributionManager distributionManager,
        ReplyProcessor21 replyProcessor21) {
      long startTime = getTimestamp();

      if (replyProcessor21 == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "{}: processor not found", this);
        }
        return;
      }

      replyProcessor21.process(this);

      distributionManager.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return CLEAR_PARTITIONED_REGION_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in, DeserializationContext context)
        throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      operationType = PartitionedRegionClearMessage.OperationType.values()[in.readByte()];
      bucketsCleared = DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out, SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeByte(operationType.ordinal());
      DataSerializer.writeObject(bucketsCleared, out);
    }

    @Override
    public String toString() {
      return new StringBuilder()
          .append("PartitionedRegionClearReplyMessage ")
          .append("processorId=").append(processorId)
          .append(" sender=").append(sender)
          .append(" bucketsCleared ").append(bucketsCleared)
          .append(" exception=").append(getException())
          .toString();
    }
  }
}
