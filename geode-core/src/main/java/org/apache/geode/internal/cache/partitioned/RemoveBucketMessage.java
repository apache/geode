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
 * Removes the hosted bucket from the recipient's PartitionedRegionDataStore.
 *
 * Usage: RemoveBucketResponse response = RemoveBucketMessage.send( InternalDistributedMember,
 * PartitionedRegion, int bucketId); if (response != null && response.waitForResponse()) { // bucket
 * was removed }
 */
public class RemoveBucketMessage extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();

  private int bucketId;
  private boolean forceRemovePrimary;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public RemoveBucketMessage() {}

  private RemoveBucketMessage(InternalDistributedMember recipient, int regionId,
      ReplyProcessor21 processor, int bucketId, boolean forceRemovePrimary) {
    super(recipient, regionId, processor);
    this.bucketId = bucketId;
    this.forceRemovePrimary = forceRemovePrimary;
  }

  /**
   * Sends a message to remove the bucket.
   *
   * @param recipient the member to remove the bucket from
   * @param region the PartitionedRegion of the bucket
   * @param bucketId the bucket to remove
   * @return the processor used to wait for the response
   */
  public static RemoveBucketResponse send(InternalDistributedMember recipient,
      PartitionedRegion region, int bucketId, boolean forceRemovePrimary) {

    Assert.assertTrue(recipient != null, "RemoveBucketMessage NULL recipient");

    RemoveBucketResponse response = new RemoveBucketResponse(region.getSystem(), recipient, region);
    RemoveBucketMessage msg = new RemoveBucketMessage(recipient, region.getPRId(), response,
        bucketId, forceRemovePrimary);
    msg.setTransactionDistributed(region.getCache().getTxManager().isDistributed());

    Set<InternalDistributedMember> failures = region.getDistributionManager().putOutgoing(msg);
    if (failures != null && failures.size() > 0) {
      // throw new ForceReattemptException("Failed sending <" + msg + ">");
      return null;
    }
    region.getPrStats().incPartitionMessagesSent();
    return response;
  }

  public RemoveBucketMessage(DataInput in) throws IOException, ClassNotFoundException {
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
    boolean removed = dataStore.removeBucket(this.bucketId, this.forceRemovePrimary);

    region.getPrStats().endPartitionMessagesProcessing(startTime);
    RemoveBucketReplyMessage.send(getSender(), getProcessorId(), dm, null, removed);

    return false;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; bucketId=").append(this.bucketId);
  }

  @Override
  public int getDSFID() {
    return PR_REMOVE_BUCKET_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.bucketId = in.readInt();
    this.forceRemovePrimary = in.readBoolean();
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(this.bucketId);
    out.writeBoolean(this.forceRemovePrimary);
  }

  public static class RemoveBucketReplyMessage extends ReplyMessage {

    private boolean removed;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public RemoveBucketReplyMessage() {}

    public RemoveBucketReplyMessage(DataInput in) throws IOException, ClassNotFoundException {
      fromData(in, InternalDataSerializer.createDeserializationContext(in));
    }

    private RemoveBucketReplyMessage(int processorId, ReplyException re, boolean removed) {
      this.processorId = processorId;
      this.removed = removed;
      setException(re);
    }

    /** Send a reply */
    public static void send(InternalDistributedMember recipient, int processorId,
        DistributionManager dm, ReplyException re, boolean removed) {
      Assert.assertTrue(recipient != null, "RemoveBucketReplyMessage NULL recipient");
      RemoveBucketReplyMessage m = new RemoveBucketReplyMessage(processorId, re, removed);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    boolean removed() {
      return this.removed;
    }

    @Override
    public void process(final DistributionManager dm, final ReplyProcessor21 processor) {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE,
            "RemoveBucketReplyMessage process invoking reply processor with processorId: {}",
            this.processorId);
      }

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "RemoveBucketReplyMessage processor not found");
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
      out.writeBoolean(this.removed);
    }

    @Override
    public int getDSFID() {
      return PR_REMOVE_BUCKET_REPLY;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.removed = in.readBoolean();
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("RemoveBucketReplyMessage ").append("processorid=").append(this.processorId)
          .append(" removed=").append(this.removed).append(" reply to sender ")
          .append(this.getSender());
      return sb.toString();
    }
  }

  /**
   * A processor to capture the value returned by the <code>RemoveBucketReplyMessage</code>
   */
  public static class RemoveBucketResponse extends PartitionResponse {

    private volatile boolean removed = false;

    public RemoveBucketResponse(InternalDistributedSystem ds, InternalDistributedMember recipient,
        PartitionedRegion theRegion) {
      super(ds, recipient);
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof RemoveBucketReplyMessage) {
          RemoveBucketReplyMessage reply = (RemoveBucketReplyMessage) msg;
          this.removed = reply.removed();
          if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
            logger.trace(LogMarker.DM_VERBOSE, "RemoveBucketResponse is {}", removed);
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
      logger.debug("RemoveBucketMessage ignoring exception: {}", ex.getMessage(), ex);
    }

    public boolean waitForResponse() {
      try {
        waitForRepliesUninterruptibly();
      } catch (ReplyException e) {
        Throwable t = e.getCause();
        // Most of these cases indicate that the PR is destroyed on the
        // remote VM. Which is fine, because that means the bucket was "removed"
        if (t instanceof CancelException) {
          String msg = "RemoveBucketMessage got remote cancellation,";
          logger.debug(msg, t);
          return true;
        }
        if (t instanceof PRLocallyDestroyedException) {
          String msg = "RemoveBucketMessage got local destroy on the PartitionRegion ";
          logger.debug(msg, t);
          return true;
        }
        if (t instanceof ForceReattemptException) {
          String msg =
              "RemoveBucketMessage got ForceReattemptException due to local destroy on the PartitionRegion";
          logger.debug(msg, t);
          return true;
        }
        e.handleCause();
      }
      return this.removed;
    }
  }

}
