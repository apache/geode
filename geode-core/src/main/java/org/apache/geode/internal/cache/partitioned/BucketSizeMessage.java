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

import org.apache.geode.cache.CacheException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message used to determine the number of bytes a Bucket consumes.
 *
 * @since GemFire 5.0
 */

public class BucketSizeMessage extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();

  /** The list of buckets whose size is needed, if null, then all buckets */
  private int bucketId;

  /**
   * Empty contstructor provided for {@link org.apache.geode.DataSerializer}
   */
  public BucketSizeMessage() {
    super();
  }

  private BucketSizeMessage(InternalDistributedMember recipient, int regionId,
      ReplyProcessor21 processor, int bucketId) {
    super(recipient, regionId, processor);
    this.bucketId = bucketId;
  }

  @Override
  public int getProcessorType() {
    return OperationExecutors.STANDARD_EXECUTOR;
  }

  /**
   * Sends a BucketSize message to determine the number of bytes the bucket consumes
   *
   * @param recipient the member that the contains keys/value message is sent to
   * @param r the PartitionedRegion that contains the bucket
   * @param bucketId the identity of the bucket whose size should be returned.
   * @return the processor used to read the returned size
   * @throws ForceReattemptException if the peer is no longer available
   */
  public static BucketSizeResponse send(InternalDistributedMember recipient, PartitionedRegion r,
      int bucketId) throws ForceReattemptException {
    Assert.assertTrue(recipient != null, "BucketSizeMessage NULL reply message");
    BucketSizeResponse p = new BucketSizeResponse(r.getSystem(), Collections.singleton(recipient));
    BucketSizeMessage m = new BucketSizeMessage(recipient, r.getPRId(), p, bucketId);
    m.setTransactionDistributed(r.getCache().getTxManager().isDistributed());
    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException(
          String.format("Failed sending < %s >", m));
    }

    return p;
  }

  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion r,
      long startTime) throws CacheException, ForceReattemptException {

    PartitionedRegionDataStore ds = r.getDataStore();
    final long size;
    if (ds != null) {
      size = ds.getBucketSize(bucketId);
    } else {
      // sender thought this member had a data store, but it doesn't
      throw new ForceReattemptException(String.format("no datastore in %s",
          dm.getDistributionManagerId()));
    }

    r.getPrStats().endPartitionMessagesProcessing(startTime);
    BucketSizeReplyMessage.send(getSender(), getProcessorId(), dm, size);

    return false;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; bucketId=").append(this.bucketId);
  }

  @Override
  public int getDSFID() {
    return PR_BUCKET_SIZE_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.bucketId = in.readInt();
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(this.bucketId); // fix for bug 38228
  }

  public static class BucketSizeReplyMessage extends HighPriorityDistributionMessage {
    /** The shared obj id of the ReplyProcessor */
    private int processorId;

    /** Propagated exception from remote node to operation initiator */
    private long size;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public BucketSizeReplyMessage() {}

    private BucketSizeReplyMessage(int processorId, long size) {
      this.processorId = processorId;
      this.size = size;
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId,
        DistributionManager dm, long size) {
      Assert.assertTrue(recipient != null, "PRDistribuedGetReplyMessage NULL reply message");
      BucketSizeReplyMessage m = new BucketSizeReplyMessage(processorId, size);
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
            "PRDistributedBucketSizeReplyMessage process invoking reply processor with processorId: {}",
            this.processorId);
      }

      ReplyProcessor21 processor = ReplyProcessor21.getProcessor(this.processorId);

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.debug("PRDistributedBucketSizeReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} Processed {}", processor, this);
      }
      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return PR_BUCKET_SIZE_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.processorId = in.readInt();
      this.size = in.readLong();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(processorId);
      out.writeLong(this.size);
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("PRDistributedBucketSizeReplyMessage ").append("processorid=")
          .append(this.processorId).append(" reply to sender ").append(this.getSender())
          .append(" returning numEntries=").append(getSize());
      return sb.toString();
    }

    public long getSize() {
      return this.size;
    }
  }
  /**
   * A processor to capture the value returned by
   * {@link org.apache.geode.internal.cache.partitioned.GetMessage.GetReplyMessage}
   *
   * @since GemFire 5.0
   */
  public static class BucketSizeResponse extends ReplyProcessor21 {
    private volatile long returnValue;

    public BucketSizeResponse(InternalDistributedSystem ds, Set recipients) {
      super(ds, recipients);
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof BucketSizeReplyMessage) {
          BucketSizeReplyMessage reply = (BucketSizeReplyMessage) msg;
          this.returnValue = reply.getSize();
          if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
            logger.trace(LogMarker.DM_VERBOSE, "BucketSizeResponse return value is {}",
                this.returnValue);
          }
        }
      } finally {
        super.process(msg);
      }
    }

    /**
     * @return Set the keys associated with the bucketid of the {@link BucketSizeMessage}
     * @throws ForceReattemptException if the peer is no longer available
     */
    public long waitForSize() throws ForceReattemptException {
      try {
        waitForRepliesUninterruptibly();
      } catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof org.apache.geode.CancelException) {
          logger.debug("BucketSizeResponse got remote cancellation; forcing reattempt. {}",
              t.getMessage(), t);
          throw new ForceReattemptException(
              "BucketSizeResponse got remote CacheClosedException; forcing reattempt.",
              t);
        }
        if (t instanceof ForceReattemptException) {
          logger.debug("BucketSizeResponse got remote Region destroyed; forcing reattempt. {}",
              t.getMessage(), t);
          throw new ForceReattemptException(
              "BucketSizeResponse got remote Region destroyed; forcing reattempt.",
              t);
        }
        e.handleCause();
      }
      return this.returnValue;
    }
  }

}
