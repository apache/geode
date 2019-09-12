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
import org.apache.geode.cache.CacheClosedException;
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
import org.apache.geode.internal.cache.PartitionedRegionDataStore.CreateBucketResult;
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
public class ManageBackupBucketMessage extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();

  /** The key associated with the value that must be sent */
  private int bucketId;

  /** true if the request is directed by a rebalance operation */
  private boolean isRebalance;

  /** true to replace data that is currently offline */
  private boolean replaceOfflineData;

  private InternalDistributedMember moveSource;

  private boolean forceCreation = true;

  private enum ReplyType {
    INITIALIZING, SUCCESS, FAIL;
  }

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public ManageBackupBucketMessage() {}

  ManageBackupBucketMessage(InternalDistributedMember recipient, int regionId,
      ReplyProcessor21 processor, int bucketId, boolean isRebalance, boolean replaceOfflineData,
      InternalDistributedMember moveSource, boolean forceCreation) {
    super(recipient, regionId, processor);
    this.bucketId = bucketId;
    this.isRebalance = isRebalance;
    this.replaceOfflineData = replaceOfflineData;
    this.moveSource = moveSource;
    this.forceCreation = forceCreation;
  }

  public ManageBackupBucketMessage(DataInput in) throws IOException, ClassNotFoundException {
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
   * @param isRebalance true if directed by full rebalance operation
   * @param moveSource If this is a bucket move.
   * @param forceCreation ignore checks which may cause the bucket not to be created
   * @return the processor used to fetch the returned Node if any
   * @throws ForceReattemptException if the peer is no longer available
   */
  public static NodeResponse send(InternalDistributedMember recipient, PartitionedRegion r,
      int bucketId, boolean isRebalance, boolean replaceOfflineData,
      InternalDistributedMember moveSource, boolean forceCreation) throws ForceReattemptException {

    Assert.assertTrue(recipient != null, "ManageBucketMessage NULL recipient");
    NodeResponse p = new NodeResponse(r.getSystem(), recipient);
    ManageBackupBucketMessage m = new ManageBackupBucketMessage(recipient, r.getPRId(), p, bucketId,
        isRebalance, replaceOfflineData, moveSource, forceCreation);
    m.setTransactionDistributed(r.getCache().getTxManager().isDistributed());

    p.enableSevereAlertProcessing();

    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException("Failed sending <" + m + ">");
    }

    return p;
  }


  /**
   * This method is called upon receipt and make the desired changes to the PartitionedRegion Note:
   * It is very important that this message does NOT cause any deadlocks as the sender will wait
   * indefinitely for the acknowledgement
   */
  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm,
      PartitionedRegion partitionedRegion, long startTime) {
    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace(LogMarker.DM_VERBOSE, "ManageBucketMessage operateOnRegion: {}",
          partitionedRegion.getFullPath());
    }

    partitionedRegion.checkReadiness(); // Don't allow closed PartitionedRegions that have
                                        // datastores to host buckets
    PartitionedRegionDataStore prDs = partitionedRegion.getDataStore();

    // This is to ensure that initialization is complete for all colocated regions
    // before bucket creation request is serviced. BUGFIX for 35888
    // GEODE-5255
    boolean isReady = prDs.isPartitionedRegionReady(partitionedRegion, bucketId);
    if (!isReady) {
      // This VM is NOT ready to manage a new bucket, refuse operation
      sendManageBackupBucketReplyMessage(dm, partitionedRegion, startTime, ReplyType.INITIALIZING);
      return false;
    }

    boolean managingBucket = prDs.grabBucket(this.bucketId, this.moveSource, this.forceCreation,
        replaceOfflineData, this.isRebalance, null, false) == CreateBucketResult.CREATED;

    sendManageBackupBucketReplyMessage(dm, partitionedRegion, startTime,
        managingBucket ? ReplyType.SUCCESS : ReplyType.FAIL);
    return false;
  }

  private void sendManageBackupBucketReplyMessage(ClusterDistributionManager dm,
      PartitionedRegion partitionedRegion, long startTime, ReplyType type) {
    partitionedRegion.getPrStats().endPartitionMessagesProcessing(startTime);
    switch (type) {
      case INITIALIZING:
        ManageBackupBucketReplyMessage.sendStillInitializing(getSender(), getProcessorId(), dm);
        break;
      case FAIL:
        ManageBackupBucketReplyMessage.sendRefusal(getSender(), getProcessorId(), dm);
        break;
      case SUCCESS:
        ManageBackupBucketReplyMessage.sendAcceptance(getSender(), getProcessorId(), dm);
        break;
      default:
        throw new RuntimeException("unreachable");
    }
  }

  @Override
  public int getDSFID() {
    return PR_MANAGE_BACKUP_BUCKET_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.bucketId = in.readInt();
    this.isRebalance = in.readBoolean();
    this.replaceOfflineData = in.readBoolean();
    boolean hasMoveSource = in.readBoolean();
    if (hasMoveSource) {
      this.moveSource = new InternalDistributedMember();
      InternalDataSerializer.invokeFromData(this.moveSource, in);
    }
    this.forceCreation = in.readBoolean();
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(this.bucketId);
    out.writeBoolean(this.isRebalance);
    out.writeBoolean(this.replaceOfflineData);
    out.writeBoolean(this.moveSource != null);
    if (this.moveSource != null) {
      InternalDataSerializer.invokeToData(this.moveSource, out);
    }
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
    buff.append("; bucketId=").append(this.bucketId);
    buff.append("; isRebalance=").append(this.isRebalance);
    buff.append("; replaceOfflineData=").append(this.replaceOfflineData);
    buff.append("; moveSource=").append(this.moveSource);
    buff.append("; forceCreation=").append(this.forceCreation);
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // since bucket management happens during entry operations, it
    // must be severe-alert compatible
    return true;
  }

  /**
   * A class that contains the reply to a {@link ManageBackupBucketMessage} message which contains
   * the {@link Node} that has accepted to manage the bucket.
   *
   * @since GemFire 5.0
   */
  public static class ManageBackupBucketReplyMessage extends ReplyMessage {


    protected boolean acceptedBucket;

    /** true if the vm refused because it was still in initialization */
    protected boolean notYetInitialized;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public ManageBackupBucketReplyMessage() {}

    public ManageBackupBucketReplyMessage(DataInput in) throws IOException, ClassNotFoundException {
      fromData(in, InternalDataSerializer.createDeserializationContext(in));
    }

    private ManageBackupBucketReplyMessage(int processorId, boolean accept, boolean initializing) {
      setProcessorId(processorId);
      this.acceptedBucket = accept;
      this.notYetInitialized = initializing;
    }

    boolean isAcceptedBucket() {
      return acceptedBucket;
    }

    boolean isNotYetInitialized() {
      return notYetInitialized;
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
      Assert.assertTrue(recipient != null, "ManageBackupBucketReplyMessage NULL reply message");
      ManageBackupBucketReplyMessage m =
          new ManageBackupBucketReplyMessage(processorId, false, false);
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
      ManageBackupBucketReplyMessage m =
          new ManageBackupBucketReplyMessage(processorId, false, true);
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
      Assert.assertTrue(recipient != null, "ManageBackupBucketReplyMessage NULL reply message");
      ManageBackupBucketReplyMessage m =
          new ManageBackupBucketReplyMessage(processorId, true, false);
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
            "ManageBackupBucketReplyMessage process invoking reply processor with processorId: {}",
            this.processorId);
      }

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "ManageBackupBucketReplyMessage processor not found");
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
      return PR_MANAGE_BACKUP_BUCKET_REPLY_MESSAGE;
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
   * A processor to capture the {@link Node} returned by {@link ManageBackupBucketMessage}
   *
   * @since GemFire 5.0
   */
  public static class NodeResponse extends ReplyProcessor21 {
    /**
     * the message that triggers return from waitForAcceptance. This will be null if the target
     * member exited
     */
    private volatile ManageBackupBucketReplyMessage msg;

    public NodeResponse(InternalDistributedSystem ds, InternalDistributedMember recipient) {
      super(ds, recipient);
    }

    @Override
    public void process(DistributionMessage m) {
      try {
        if (m instanceof ManageBackupBucketReplyMessage) {
          ManageBackupBucketReplyMessage reply = (ManageBackupBucketReplyMessage) m;
          this.msg = reply;
          if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
            logger.trace(LogMarker.DM_VERBOSE, "NodeResponse return value is {} isInitializng={}",
                reply.acceptedBucket, reply.notYetInitialized);
          }
        } else {
          Assert.assertTrue(m instanceof ReplyMessage);
        }
      } finally {
        super.process(m);
      }
    }

    @Override
    protected int getAckWaitThreshold() {
      // increasing ack wait threshold for this message as region/bucket initialization can take
      // time
      return super.getAckWaitThreshold() * 2;
    }

    /**
     * Wait for the response to a {@link ManageBackupBucketMessage} request.
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
        if (t instanceof CacheClosedException) {
          String m =
              "NodeResponse got remote CacheClosedException, throwing PartitionedRegionCommunication Exception";
          logger.debug(m, t);
          throw new ForceReattemptException(m, t);
        }
        if (t instanceof PRLocallyDestroyedException) {
          String m =
              "NodeResponse got local destroy on the PartitionRegion , throwing ForceReattemptException";
          logger.debug(m, t);
          throw new ForceReattemptException(m, t);
        }
        if (t instanceof ForceReattemptException) {
          String m =
              "NodeResponse got ForceReattemptException due to local destroy on the PartitionRegion";
          logger.debug(m, t);
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
