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
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.InterestRegistrationEvent;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * This message is used as the notification that a client interest registration or unregistration
 * event occurred.
 *
 * @since GemFire 5.8BetaSUISSE
 */
public class InterestEventMessage extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();

  /** The <code>InterestRegistrationEvent</code> */
  private InterestRegistrationEvent event;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public InterestEventMessage() {}

  private InterestEventMessage(Set recipients, int regionId, int processorId,
      final InterestRegistrationEvent event, ReplyProcessor21 processor) {
    super(recipients, regionId, processor);
    this.event = event;
  }

  @Override
  public int getProcessorType() {
    return OperationExecutors.STANDARD_EXECUTOR;
  }

  @Override
  protected boolean operateOnPartitionedRegion(final ClusterDistributionManager dm,
      PartitionedRegion r, long startTime) throws ForceReattemptException {
    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace(LogMarker.DM_VERBOSE, "InterestEventMessage operateOnPartitionedRegion: {}",
          r.getFullPath());
    }

    PartitionedRegionDataStore ds = r.getDataStore();

    if (ds != null) {
      try {
        ds.handleInterestEvent(this.event);
        r.getPrStats().endPartitionMessagesProcessing(startTime);
        InterestEventReplyMessage.send(getSender(), getProcessorId(), dm);
      } catch (Exception e) {
        sendReply(getSender(), getProcessorId(), dm, new ReplyException(new ForceReattemptException(
            "Caught exception during interest registration processing:", e)), r, startTime);
        return false;
      }
    } else {
      throw new InternalError("InterestEvent message was sent to a member with no storage.");
    }

    // Unless there was an exception thrown, this message handles sending the
    // response
    return false;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; event=").append(this.event);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.event = (InterestRegistrationEvent) DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(this.event, out);
  }

  /**
   * Sends an InterestEventMessage message
   *
   * @param recipients the Set of members that the get message is being sent to
   * @param region the PartitionedRegion for which interest event was received
   * @param event the InterestRegistrationEvent to send
   * @return the InterestEventResponse
   * @throws ForceReattemptException if the peer is no longer available
   */
  public static InterestEventResponse send(Set recipients, PartitionedRegion region,
      final InterestRegistrationEvent event) throws ForceReattemptException {
    InterestEventResponse response = new InterestEventResponse(region.getSystem(), recipients);
    InterestEventMessage m = new InterestEventMessage(recipients, region.getPRId(),
        response.getProcessorId(), event, response);
    m.setTransactionDistributed(region.getCache().getTxManager().isDistributed());

    Set failures = region.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException("Failed sending <" + m + "> to " + failures);
    }
    return response;
  }

  /**
   * This message is used for the reply to a {@link InterestEventMessage}.
   *
   * @since GemFire 5.8BetaSUISSE
   */
  public static class InterestEventReplyMessage extends HighPriorityDistributionMessage {
    /** The shared obj id of the ReplyProcessor */
    private int processorId;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public InterestEventReplyMessage() {}

    private InterestEventReplyMessage(int processorId) {
      this.processorId = processorId;
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId,
        DistributionManager dm) throws ForceReattemptException {
      InterestEventReplyMessage m = new InterestEventReplyMessage(processorId);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the message.
     *
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    protected void process(final ClusterDistributionManager dm) {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE,
            "InterestEventReplyMessage process invoking reply processor with processorId: {}",
            this.processorId);
      }

      try {
        ReplyProcessor21 processor = ReplyProcessor21.getProcessor(this.processorId);

        if (processor == null) {
          if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
            logger.trace(LogMarker.DM_VERBOSE, "InterestEventReplyMessage processor not found");
          }
          return;
        }
        processor.process(this);

        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace("{} processed {}", processor, this);
        }
      } finally {
        dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
      }
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(processorId);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.processorId = in.readInt();
    }

    @Override
    public String toString() {
      StringBuffer sb =
          new StringBuffer().append("InterestEventReplyMessage ").append("processorid=")
              .append(this.processorId).append(" reply to sender ").append(this.getSender());
      return sb.toString();
    }

    @Override
    public int getDSFID() {
      return INTEREST_EVENT_REPLY_MESSAGE;
    }
  }

  /**
   * A processor to capture the value returned by
   * {@link org.apache.geode.internal.cache.partitioned.InterestEventMessage.InterestEventReplyMessage}
   *
   * @since GemFire 5.1
   */
  public static class InterestEventResponse extends PartitionResponse {

    public InterestEventResponse(InternalDistributedSystem ds, Set recipients) {
      super(ds, recipients);
    }

    /**
     * @throws ForceReattemptException if the peer is no longer available
     */
    public void waitForResponse() throws ForceReattemptException {
      try {
        waitForCacheException();
      } catch (ForceReattemptException e) {
        logger.debug("InterestEventResponse got ForceReattemptException; rethrowing {}",
            e.getMessage(), e);
        throw e;
      } catch (CacheException e) {
        final String msg =
            "InterestEventResponse got remote CacheException, throwing ForceReattemptException";
        logger.debug(msg, e);
        throw new ForceReattemptException(msg, e);
      }
    }
  }

  @Override
  public int getDSFID() {
    return INTEREST_EVENT_MESSAGE;
  }
}
