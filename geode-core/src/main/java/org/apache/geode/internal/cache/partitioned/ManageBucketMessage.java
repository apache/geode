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
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.Node;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A request to manage a particular bucket
 *
 * @since GemFire 5.0
 *
 */
public class ManageBucketMessage extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();

  /** The key associated with the value that must be sent */
  private int bucketId;

  /** The value associated with the key that must be sent */
  private int bucketSize;

  /**
   * Force the hosting of a bucket
   */
  private boolean forceCreation;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public ManageBucketMessage() {}

  private ManageBucketMessage(InternalDistributedMember recipient, int regionId,
      ReplyProcessor21 processor, int bucketId, int bucketSize, boolean hostItNow) {
    super(recipient, regionId, processor);
    this.bucketId = bucketId;
    this.bucketSize = bucketSize;
    this.forceCreation = hostItNow;
  }

  public ManageBucketMessage(DataInput in) throws IOException, ClassNotFoundException {
    fromData(in, InternalDataSerializer.createDeserializationContext(in));
  }

  @Override
  public int getProcessorType() {
    return OperationExecutors.WAITING_POOL_EXECUTOR;
  }

  /**
   * Sends a PartitionedRegion manage bucket request to the recipient
   *
   * @param recipient the member to which the bucket manage request is sent
   * @param r the PartitionedRegion to which the bucket belongs
   * @param bucketId the unique identifier of the bucket
   * @param bucketSize the size in bytes of the bucket
   * @param forceCreation inform the recipient to accept the bucket appropriately ignoring maximums
   * @return the processor used to fetch the returned Node if any
   * @throws ForceReattemptException if the peer is no longer available
   */
  public static NodeResponse send(InternalDistributedMember recipient, PartitionedRegion r,
      int bucketId, int bucketSize, boolean forceCreation) throws ForceReattemptException {
    Assert.assertTrue(recipient != null, "ManageBucketMessage NULL recipient");
    NodeResponse p = new NodeResponse(r.getSystem(), recipient);
    ManageBucketMessage m =
        new ManageBucketMessage(recipient, r.getPRId(), p, bucketId, bucketSize, forceCreation);
    m.setTransactionDistributed(r.getCache().getTxManager().isDistributed());

    p.enableSevereAlertProcessing();

    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException(
          String.format("Failed sending < %s >", m));
    }

    return p;
  }

  @Override
  public boolean canParticipateInTransaction() {
    return false;
  }

  /**
   * This method is called upon receipt and make the desired changes to the PartitionedRegion Note:
   * It is very important that this message does NOT cause any deadlocks as the sender will wait
   * indefinitely for the acknowledgement
   */
  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion r,
      long startTime) {
    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace(LogMarker.DM_VERBOSE, "ManageBucketMessage operateOnRegion: {}",
          r.getFullPath());
    }

    // This is to ensure that initialization is complete before bucket creation request is
    // serviced. BUGFIX for 35888
    r.waitOnInitialization();

    r.checkReadiness(); // Don't allow closed PartitionedRegions that have datastores to host
                        // buckets
    PartitionedRegionDataStore prDs = r.getDataStore();
    boolean managingBucket = prDs.handleManageBucketRequest(this.bucketId, this.bucketSize,
        this.sender, this.forceCreation);
    r.getPrStats().endPartitionMessagesProcessing(startTime);
    if (managingBucket) {
      // fix for bug 39356 - If the sender died while we were creating the bucket
      // notify other nodes that they should invoke grabBackupBuckets to
      // make copies of this bucket. Normally the sender would be responsible
      // for creating those copies.
      checkSenderStillAlive(r, getSender());

      ManageBucketReplyMessage.sendAcceptance(getSender(), getProcessorId(), dm);
    } else {
      ManageBucketReplyMessage.sendRefusal(getSender(), getProcessorId(), dm);
    }
    return false;
  }

  /**
   * Check that sender is still a participant in the partitioned region. If not, notify other nodes
   * to create backup buckets
   */
  private void checkSenderStillAlive(PartitionedRegion r, InternalDistributedMember sender) {
    if (!r.getDistributionAdvisor().containsId(sender)) {
      r.getRedundancyProvider().finishIncompleteBucketCreation(this.bucketId);
    }
  }

  @Override
  public int getDSFID() {
    return PR_MANAGE_BUCKET_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.bucketId = in.readInt();
    this.bucketSize = in.readInt();
    this.forceCreation = in.readBoolean();
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(this.bucketId);
    out.writeInt(this.bucketSize);
    out.writeBoolean(this.forceCreation);
  }


  /**
   * Assists the toString method in reporting the contents of this message
   *
   * @see PartitionMessage#toString()
   */
  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; bucketId=").append(this.bucketId).append("; bucketSize=")
        .append(this.bucketSize);
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // since bucket management happens during entry operations, it
    // must be severe-alert compatible
    return true;
  }

  /**
   * A class that contains the reply to a {@link ManageBucketMessage} message which contains the
   * {@link Node} that has accepted to manage the bucket.
   *
   * @since GemFire 5.0
   */
  public static class ManageBucketReplyMessage extends ReplyMessage {

    protected boolean acceptedBucket;

    /** true if the vm refused because it was still in initialization */
    protected boolean notYetInitialized;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public ManageBucketReplyMessage() {}

    public ManageBucketReplyMessage(DataInput in) throws IOException, ClassNotFoundException {
      fromData(in, InternalDataSerializer.createDeserializationContext(in));
    }

    private ManageBucketReplyMessage(int processorId, boolean accept, boolean initializing) {
      this.processorId = processorId;
      this.acceptedBucket = accept;
      this.notYetInitialized = initializing;
    }

    /**
     * Refuse the request to manage the bucket
     *
     * @param recipient the requesting node
     * @param processorId the identity of the processor the requesting node is waiting on
     * @param dm the distribution manager used to send the refusal
     */
    public static void sendRefusal(InternalDistributedMember recipient, int processorId,
        DistributionManager dm) {
      Assert.assertTrue(recipient != null, "ManageBucketReplyMessage NULL reply message");
      ManageBucketReplyMessage m = new ManageBucketReplyMessage(processorId, false, false);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    /**
     * Refuse the request to manage the bucket because the region is still being initialized
     *
     * @param recipient the requesting node
     * @param processorId the identity of the processor the requesting node is waiting on
     * @param dm the distribution manager used to send the acceptance message
     */
    public static void sendStillInitializing(InternalDistributedMember recipient, int processorId,
        DistributionManager dm) {
      ManageBucketReplyMessage m = new ManageBucketReplyMessage(processorId, false, true);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    /**
     * Accept the request to manage the bucket
     *
     * @param recipient the requesting node
     * @param processorId the identity of the processor the requesting node is waiting on
     * @param dm the distribution manager used to send the acceptance message
     */
    public static void sendAcceptance(InternalDistributedMember recipient, int processorId,
        DistributionManager dm) {
      Assert.assertTrue(recipient != null, "ManageBucketReplyMessage NULL reply message");
      ManageBucketReplyMessage m = new ManageBucketReplyMessage(processorId, true, false);
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
            "ManageBucketReplyMessage process invoking reply processor with processorId: {}",
            this.processorId);
      }

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "ManageBucketReplyMessage processor not found");
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
      out.writeBoolean(this.acceptedBucket);
      out.writeBoolean(this.notYetInitialized);
    }

    @Override
    public int getDSFID() {
      return PR_MANAGE_BUCKET_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.acceptedBucket = in.readBoolean();
      this.notYetInitialized = in.readBoolean();
    }

    @Override
    public String toString() {
      return new StringBuffer().append("ManageBucketReplyMessage ").append("processorid=")
          .append(this.processorId).append(" accepted bucket=").append(this.acceptedBucket)
          .append(" isInitializing=").append(this.notYetInitialized).toString();
    }
  }

  /**
   * A processor to capture the {@link Node} returned by {@link ManageBucketMessage}
   *
   * @since GemFire 5.0
   */
  public static class NodeResponse extends ReplyProcessor21 {
    /**
     * the message that triggers return from waitForAcceptance. This will be null if the target
     * member exited
     */
    private volatile ManageBucketReplyMessage msg;

    public NodeResponse(InternalDistributedSystem ds, InternalDistributedMember recipient) {
      super(ds, recipient);
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof ManageBucketReplyMessage) {
          ManageBucketReplyMessage reply = (ManageBucketReplyMessage) msg;
          this.msg = reply;
          if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
            logger.trace(LogMarker.DM_VERBOSE, "NodeResponse return value is {} isInitializing {}",
                reply.acceptedBucket, reply.notYetInitialized);
          }
        } else {
          Assert.assertTrue(msg instanceof ReplyMessage);
        }
      } finally {
        super.process(msg);
      }
    }


    /**
     * Wait for the response to a {@link ManageBucketMessage} request.
     *
     * @return true if the node sent the request is managing the bucket
     * @see org.apache.geode.internal.cache.PartitionedRegionDataStore#handleManageBucketRequest(int,
     *      int, InternalDistributedMember, boolean)
     * @throws ForceReattemptException if the peer is no longer available
     */
    public boolean waitForAcceptance() throws ForceReattemptException {
      try {
        waitForRepliesUninterruptibly();
      } catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof CancelException) {
          logger.debug(
              "NodeResponse got remote cancellation, throwing PartitionedRegionCommunication Exception. {}",
              t.getMessage(), t);
          throw new ForceReattemptException(
              "NodeResponse got remote cancellation, throwing PartitionedRegionCommunication Exception.",
              t);
        }
        if (t instanceof PRLocallyDestroyedException) {
          logger.debug(
              "NodeResponse got local destroy on the PartitionRegion , throwing ForceReattemptException. {}",
              t.getMessage(), t);
          throw new ForceReattemptException(
              "NodeResponse got local destroy on the PartitionRegion , throwing ForceReattemptException.",
              t);
        }
        if (t instanceof PartitionOfflineException) {
          throw (PartitionOfflineException) t;
        }
        if (t instanceof ForceReattemptException) {
          String msg =
              "NodeResponse got ForceReattemptException due to local destroy on the PartitionRegion.";
          logger.debug(msg, t);
          throw (ForceReattemptException) t;
        }
        e.handleCause();
      }
      return (this.msg != null) && this.msg.acceptedBucket;
    }

    /**
     * After a response has been returned from waitForAcceptance, this method may be used to see if
     * the other vm rejected the bucket because it was still initializing.
     */
    public boolean rejectedDueToInitialization() {
      return (this.msg != null) && this.msg.notYetInitialized;
    }
  }

}
