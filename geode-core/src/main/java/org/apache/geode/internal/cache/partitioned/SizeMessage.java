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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegion.SizeEntry;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This message is used to determine the number of Entries in a Region, or its size.
 *
 * @since GemFire 5.0
 */
public class SizeMessage extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();

  /** The list of buckets whose size is needed, if null, then all buckets */
  private ArrayList<Integer> bucketIds;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public SizeMessage() {}

  // reuse the NOTIFICATION_ONLY flag since it is not used for SizeMessage
  /** flag to indicate that only an estimated size is required */
  public static final short ESTIMATE = NOTIFICATION_ONLY;

  private transient boolean estimate;

  /**
   * The message sent to a set of {@link InternalDistributedMember}s to caculate the number of
   * Entries in each of their buckets
   *
   * @param recipients members to receive the message
   * @param regionId the <code>PartitionedRegion<code> regionId
   * @param processor the reply processor used to wait on the response
   * @param bucketIds the list of bucketIds to get the size for or null for all buckets
   */
  private SizeMessage(Set recipients, int regionId, ReplyProcessor21 processor,
      ArrayList<Integer> bucketIds, boolean estimate) {
    super(recipients, regionId, processor);
    if (bucketIds != null && bucketIds.isEmpty()) {
      this.bucketIds = null;
    } else {
      this.bucketIds = bucketIds;
    }
    this.estimate = estimate;
  }

  /**
   * sends a message to the given recipients asking for the size of either their primary bucket
   * entries or the values sets of their primary buckets
   *
   * @param recipients recipients of the message
   * @param r the local PartitionedRegion instance
   * @param bucketIds the buckets to look for, or null for all buckets
   */
  public static SizeResponse send(Set recipients, PartitionedRegion r, ArrayList<Integer> bucketIds,
      boolean estimate) {
    Assert.assertTrue(recipients != null, "SizeMessage NULL recipients set");
    SizeResponse p = new SizeResponse(r.getSystem(), recipients);
    SizeMessage m = new SizeMessage(recipients, r.getPRId(), p, bucketIds, estimate);
    m.setTransactionDistributed(r.getCache().getTxManager().isDistributed());
    r.getDistributionManager().putOutgoing(m);
    return p;
  }

  /**
   * This message may be sent to nodes before the PartitionedRegion is completely initialized due to
   * the RegionAdvisor(s) knowing about the existence of a partitioned region at a very early part
   * of the initialization
   */
  @Override
  protected boolean failIfRegionMissing() {
    return false;
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  @Override
  protected void setBooleans(short s, DataInput in,
      DeserializationContext context) throws ClassNotFoundException, IOException {
    super.setBooleans(s, in, context);
    this.estimate = ((s & ESTIMATE) != 0);
  }

  @Override
  protected short computeCompressedShort(short s) {
    s = super.computeCompressedShort(s);
    if (this.estimate)
      s |= ESTIMATE;
    return s;
  }

  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion r,
      long startTime) throws CacheException, ForceReattemptException {
    Map<Integer, SizeEntry> sizes;
    if (r != null) {
      PartitionedRegionDataStore ds = r.getDataStore();
      if (ds != null) { // datastore exists
        if (this.bucketIds != null) {
          if (estimate) {
            sizes = ds.getSizeEstimateLocallyForBuckets(this.bucketIds);
          } else {
            sizes = ds.getSizeLocallyForBuckets(this.bucketIds);
          }
        } else {
          if (estimate) {
            sizes = ds.getSizeEstimateForLocalPrimaryBuckets();
          } else {
            sizes = ds.getSizeForLocalBuckets();
          }
        }
        r.getPrStats().endPartitionMessagesProcessing(startTime);
        SizeReplyMessage.send(getSender(), getProcessorId(), dm, sizes);
      } // datastore exists
      else {
        logger.warn("SizeMessage: data store not configured for this member");
        ReplyMessage.send(getSender(), getProcessorId(),
            new ReplyException(new ForceReattemptException(
                "no datastore here")),
            dm, r.isInternalRegion());
      }
    } else {
      if (logger.isDebugEnabled()) {
        // Note that this is more likely to happen with this message
        // because of it returning false from failIfRegionMissing.
        logger.debug("SizeMessage: Region {} not found for this member", regionId);
      }
      ReplyMessage.send(getSender(), getProcessorId(),
          new ReplyException(new ForceReattemptException(
              String.format("%s : could not find partitioned region with Id %s",
                  dm.getDistributionManagerId(), Integer.valueOf(regionId)))),
          dm, r != null && r.isInternalRegion());
    }
    // Unless there was an exception thrown, this message handles sending the
    // response
    return false;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; bucketIds=").append(this.bucketIds);
  }

  @Override
  public int getDSFID() {
    return PR_SIZE_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.bucketIds = DataSerializer.readArrayList(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeArrayList(this.bucketIds, out);
  }

  public static class SizeReplyMessage extends ReplyMessage {
    /** Propagated exception from remote node to operation initiator */
    private Map<Integer, SizeEntry> bucketSizes;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public SizeReplyMessage() {}

    private SizeReplyMessage(int processorId, Map<Integer, SizeEntry> bucketSizes) {
      this.processorId = processorId;
      this.bucketSizes = bucketSizes;
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId,
        DistributionManager dm, Map<Integer, SizeEntry> sizes) {
      Assert.assertTrue(recipient != null, "SizeReplyMessage NULL reply message");
      SizeReplyMessage m = new SizeReplyMessage(processorId, sizes);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the message.
     *
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DistributionManager dm, final ReplyProcessor21 processor) {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE,
            "{} process invoking reply processor with processorId: {}", getClass().getName(),
            this.processorId);
      }

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "{} processor not found", getClass().getName());
        }
        return;
      }
      processor.process(this);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} processed {}", processor, this);
      }
      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeObject(this.bucketSizes, out);
    }

    @Override
    public int getDSFID() {
      return PR_SIZE_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.bucketSizes = DataSerializer.readObject(in);
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append(this.getClass().getName()).append(" processorid=").append(this.processorId)
          .append(" reply to sender ").append(this.getSender())
          .append(" returning bucketSizes.size=").append(getBucketSizes().size());
      return sb.toString();
    }

    public Map<Integer, SizeEntry> getBucketSizes() {
      return this.bucketSizes;
    }
  }

  /**
   * A processor to capture the value returned by
   * {@link org.apache.geode.internal.cache.partitioned.GetMessage.GetReplyMessage}
   *
   * @since GemFire 5.0
   */
  public static class SizeResponse extends ReplyProcessor21 {
    private final HashMap<Integer, SizeEntry> returnValue = new HashMap<Integer, SizeEntry>();

    public SizeResponse(InternalDistributedSystem ds, Set recipients) {
      super(ds, recipients);
    }

    /**
     * The SizeResponse processor ignores remote exceptions by implmenting this method. Ignoring
     * remote exceptions is acceptable since the SizeMessage is sent to all Nodes and all
     * {@link SizeMessage.SizeReplyMessage}s are processed for each individual bucket size. The hope
     * is that any failure due to an exception will be covered by healthy Nodes.
     */
    @Override
    protected synchronized void processException(ReplyException ex) {
      logger.debug("SizeResponse ignoring exception: {}", ex.getMessage(), ex);
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof SizeReplyMessage) {
          SizeReplyMessage reply = (SizeReplyMessage) msg;
          synchronized (this.returnValue) {
            for (Map.Entry<Integer, SizeEntry> me : reply.getBucketSizes().entrySet()) {
              Integer k = me.getKey();
              if (!this.returnValue.containsKey(k) || !this.returnValue.get(k).isPrimary()) {
                this.returnValue.put(k, me.getValue());
              }
            }
          }
        }
      } finally {
        super.process(msg);
      }
    }

    /**
     * @return Map buckets and their associated sizes, this should never throw due to the
     *         {@link #processException(ReplyException)}method above
     */
    public Map<Integer, SizeEntry> waitBucketSizes() {
      try {
        waitForRepliesUninterruptibly();
      } catch (ReplyException e) {
        logger.debug("{} waitBucketSizes ignoring exception: {}", getClass().getName(),
            e.getMessage(), e);
      }
      synchronized (this.returnValue) {
        return this.returnValue;
      }
    }
  }

}
