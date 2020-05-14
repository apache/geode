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

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Moves the bucket to the recipient's PartitionedRegionDataStore. The recipient will create an
 * extra redundant copy of the bucket and then send a {@link RemoveBucketMessage} to the specified
 * source for the bucket.
 *
 * Usage: MoveBucketResponse response = MoveBucketMessage.send( InternalDistributedMember,
 * PartitionedRegion, int bucketId); if (response != null && response.waitForResponse()) { // bucket
 * was moved }
 */
public class MoveBucketMessage extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();

  private volatile int bucketId;
  private volatile InternalDistributedMember source;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public MoveBucketMessage() {}

  private MoveBucketMessage(InternalDistributedMember recipient, int regionId,
      ReplyProcessor21 processor, int bucketId, InternalDistributedMember source) {
    super(recipient, regionId, processor);
    this.bucketId = bucketId;
    this.source = source;
  }

  /**
   * Sends a message to move the bucket from <code>source</code> member to the
   * <code>recipient</code> member.
   *
   * @param recipient the member to move the bucket to
   * @param region the PartitionedRegion of the bucket
   * @param bucketId the bucket to move
   * @param source the member to move the bucket from
   * @return the processor used to wait for the response
   */
  public static MoveBucketResponse send(InternalDistributedMember recipient,
      PartitionedRegion region, int bucketId, InternalDistributedMember source) {

    Assert.assertTrue(recipient != null, "MoveBucketMessage NULL recipient");

    MoveBucketResponse response = new MoveBucketResponse(region.getSystem(), recipient, region);
    MoveBucketMessage msg =
        new MoveBucketMessage(recipient, region.getPRId(), response, bucketId, source);
    msg.setTransactionDistributed(region.getCache().getTxManager().isDistributed());

    Set<InternalDistributedMember> failures = region.getDistributionManager().putOutgoing(msg);
    if (failures != null && failures.size() > 0) {
      // throw new ForceReattemptException("Failed sending <" + msg + ">");
      return null;
    }
    region.getPrStats().incPartitionMessagesSent();
    return response;
  }

  public MoveBucketMessage(DataInput in) throws IOException, ClassNotFoundException {
    fromData(in, InternalDataSerializer.createDeserializationContext(in));
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm,
      PartitionedRegion region, long startTime) throws ForceReattemptException {

    PartitionedRegionDataStore dataStore = region.getDataStore();
    boolean moved = dataStore.moveBucket(this.bucketId, this.source, true);

    region.getPrStats().endPartitionMessagesProcessing(startTime);
    MoveBucketReplyMessage.send(getSender(), getProcessorId(), dm, null, moved);

    return false;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; bucketId=").append(this.bucketId);
    buff.append("; source=").append(this.source);
  }

  @Override
  public int getDSFID() {
    return PR_MOVE_BUCKET_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.bucketId = in.readInt();
    this.source = (InternalDistributedMember) DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(this.bucketId);
    DataSerializer.writeObject(this.source, out);
  }

  public static class MoveBucketReplyMessage extends ReplyMessage {

    private boolean moved;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public MoveBucketReplyMessage() {}

    public MoveBucketReplyMessage(DataInput in) throws IOException, ClassNotFoundException {
      fromData(in, InternalDataSerializer.createDeserializationContext(in));
    }

    private MoveBucketReplyMessage(int processorId, ReplyException re, boolean moved) {
      this.processorId = processorId;
      this.moved = moved;
      setException(re);
    }

    /** Send a reply */
    public static void send(InternalDistributedMember recipient, int processorId,
        DistributionManager dm, ReplyException re, boolean moved) {
      Assert.assertTrue(recipient != null, "MoveBucketReplyMessage NULL recipient");
      MoveBucketReplyMessage m = new MoveBucketReplyMessage(processorId, re, moved);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    boolean moved() {
      return this.moved;
    }

    @Override
    public void process(final DistributionManager dm, final ReplyProcessor21 processor) {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE,
            "MoveBucketReplyMessage process invoking reply processor with processorId: {}",
            this.processorId);
      }

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "MoveBucketReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} processed {}", processor, this);
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeBoolean(this.moved);
    }

    @Override
    public int getDSFID() {
      return PR_MOVE_BUCKET_REPLY;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.moved = in.readBoolean();
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("MoveBucketReplyMessage ").append("processorid=").append(this.processorId)
          .append(" moved=").append(this.moved).append(" reply to sender ")
          .append(this.getSender());
      return sb.toString();
    }
  }

  /**
   * A processor to capture the value returned by the <code>MoveBucketReplyMessage</code>
   */
  public static class MoveBucketResponse extends PartitionResponse {

    private volatile boolean moved = false;

    public MoveBucketResponse(InternalDistributedSystem ds, InternalDistributedMember recipient,
        PartitionedRegion theRegion) {
      super(ds, recipient);
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof MoveBucketReplyMessage) {
          MoveBucketReplyMessage reply = (MoveBucketReplyMessage) msg;
          this.moved = reply.moved();
          if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
            logger.trace(LogMarker.DM_VERBOSE, "MoveBucketResponse is {}", moved);
          }
        }
      } finally {
        super.process(msg);
      }
    }

    /**
     * Ignore any incoming exception from other VMs, we just want an acknowledgement that the
     * message was processed.
     */
    @Override
    protected synchronized void processException(ReplyException ex) {
      logger.debug("MoveBucketMessage ignoring exception: {}", ex.getMessage(), ex);
    }

    public boolean waitForResponse() {
      try {
        waitForRepliesUninterruptibly();
      } catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof CancelException) {
          String msg = "MoveBucketMessage got remote cancellation,";
          logger.debug(msg, t);
          return false;
        }
        if (t instanceof PRLocallyDestroyedException) {
          String msg = "MoveBucketMessage got local destroy on the PartitionRegion ";
          logger.debug(msg, t);
          return false;
        }
        if (t instanceof ForceReattemptException) {
          String msg =
              "MoveBucketMessage got ForceReattemptException due to local destroy on the PartitionRegion";
          logger.debug(msg, t);
          return false;
        }
        e.handleCause();
      }
      return this.moved;
    }
  }

}
