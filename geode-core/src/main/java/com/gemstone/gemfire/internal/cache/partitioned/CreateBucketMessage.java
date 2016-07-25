/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.PartitionedRegionStorageException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.FixedPartitionAttributesImpl;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.Node;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * A request from an accessor to a datastore telling 
 * it to direct the creation of a bucket. This request is somewhat 
 * of a hack. With 6.0, we no longer recover redundancy when a
 * member crashes. However, if the member directing the creation
 * of a bucket crashes, that will leave us with low redundancy. We 
 * decided it was not good behavior to leave the system with impaired
 * redundancy if an accessor crashes. Hence, by forcing a datastore 
 * to direct the creation of the bucket, at least we will expect
 * the redundancy to be impaired if that datastore crashes (even if it
 * never hosted that bucket). 
 *
 * @since GemFire 6.0
 *
 */
public final class CreateBucketMessage extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();
  
  /** The key associated with the value that must be sent */ 
  private int bucketId;

  /** The value associated with the key that must be sent */ 
  private int bucketSize;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public CreateBucketMessage() {}
  
  private CreateBucketMessage(InternalDistributedMember recipient, int regionId,
      ReplyProcessor21 processor, int bucketId, int bucketSize) {
    super(recipient, regionId, processor);
    this.bucketId = bucketId;
    this.bucketSize = bucketSize;
  }
  public CreateBucketMessage(DataInput in) throws IOException, ClassNotFoundException {
    fromData(in);
  }
  
  @Override  
  final public int getProcessorType() {
    return DistributionManager.WAITING_POOL_EXECUTOR;
  }

  /**
   * Sends a PartitionedRegion manage bucket request to the recipient
   * @param recipient the member to which the bucket manage request is sent 
   * @param r  the PartitionedRegion to which the bucket belongs
   * @param bucketId the unique identifier of the bucket
   * @param bucketSize the size in bytes of the bucket
   * @return the processor used to fetch the returned Node if any
   * @throws ForceReattemptException if the peer is no longer available
   */
  public static NodeResponse send(InternalDistributedMember recipient, 
      PartitionedRegion r, int bucketId, int bucketSize) 
      throws ForceReattemptException
  {
    Assert.assertTrue(recipient != null, "CreateBucketMessage NULL recipient");
    NodeResponse p = new NodeResponse(r.getSystem(), recipient);
    CreateBucketMessage m = new CreateBucketMessage(recipient, r.getPRId(), p, bucketId, bucketSize);

    p.enableSevereAlertProcessing();

    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException("Failed sending <" + m + ">");
    }

    return p;
  }

  
  /**
   * This method is called upon receipt and make the desired changes to the
   * PartitionedRegion
   * Note: It is very important that this message does NOT cause any deadlocks as the sender will wait indefinitely for the acknowledgement
   */
  @Override  
  protected boolean operateOnPartitionedRegion(DistributionManager dm, 
      PartitionedRegion r, long startTime) {
    if (logger.isTraceEnabled(LogMarker.DM)) {
      logger.trace(LogMarker.DM, "CreateBucketMessage operateOnRegion: {}", r.getFullPath());
    }
   
    // This is to ensure that initialization is complete before bucket creation request is 
    // serviced. BUGFIX for 35888
    if (! r.isInitialized()) {
      // This VM is NOT ready to manage a new bucket, refuse operation
      CreateBucketReplyMessage.sendResponse(getSender(), getProcessorId(), dm, null);
      return false;
    }
    
    // For FPR, for given bucket id find out the partition to which this bucket
    // belongs
    String partitionName = null;
    if (r.isFixedPartitionedRegion()) {
      FixedPartitionAttributesImpl fpa = PartitionedRegionHelper
          .getFixedPartitionAttributesForBucket(r, bucketId);
      partitionName = fpa.getPartitionName();
    }
    r.checkReadiness(); 
    InternalDistributedMember primary = r.getRedundancyProvider().createBucketAtomically(bucketId, bucketSize,
        startTime, false, partitionName);
    r.getPrStats().endPartitionMessagesProcessing(startTime); 
    CreateBucketReplyMessage.sendResponse(getSender(), getProcessorId(), dm, primary);      
    return false;
  }
  
  public int getDSFID() {
    return PR_CREATE_BUCKET_MESSAGE;
  }

  @Override  
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException
  {
    super.fromData(in);
    this.bucketId = in.readInt();
    this.bucketSize = in.readInt();
  }

  @Override  
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    out.writeInt(this.bucketId);
    out.writeInt(this.bucketSize);
  }

  
  /**
   * Assists the toString method in reporting the contents of this message
   * 
   * @see PartitionMessage#toString()
   */
  @Override
  protected void appendFields(StringBuffer buff)
  {
    super.appendFields(buff);
    buff.append("; bucketId=").append(this.bucketId)
        .append("; bucketSize=").append(this.bucketSize);
  }
  
  @Override  
  public boolean isSevereAlertCompatible() {
    // since bucket management happens during entry operations, it
    // must be severe-alert compatible
    return true;
  }

  /**
   * A class that contains the reply to a {@link CreateBucketMessage} message
   * which contains the {@link Node} that has accepted to manage the bucket. 
   * @since GemFire 5.0
   */
  public static final class CreateBucketReplyMessage extends ReplyMessage {
    private InternalDistributedMember primary;
    
    /**
     * Empty constructor to conform to DataSerializable interface 
     */
    public CreateBucketReplyMessage() {
    }

    public CreateBucketReplyMessage(DataInput in) throws IOException, ClassNotFoundException {
      fromData(in);
    }

    private CreateBucketReplyMessage(int processorId, InternalDistributedMember primary)
    {
      setProcessorId(processorId);
      this.primary  = primary;
    }
    
    /**
     * Accept the request to manage the bucket
     * @param recipient the requesting node
     * @param processorId the identity of the processor the requesting node is waiting on
     * @param dm the distribution manager used to send the acceptance message
     */
    public static void sendResponse(InternalDistributedMember recipient, int processorId, 
        DM dm, InternalDistributedMember primary) 
    {
      Assert.assertTrue(recipient != null, "CreateBucketReplyMessage NULL reply message");
      CreateBucketReplyMessage m = new CreateBucketReplyMessage(processorId, primary);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
}

    /**
     * Processes this message.  This method is invoked by the receiver
     * of the message.
     * @param dm the distribution manager that is processing the message.
     */
    @Override  
    public void process(final DM dm, final ReplyProcessor21 processor) {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "CreateBucketReplyMessage process invoking reply processor with processorId:" + this.processorId);
      }

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "CreateBucketReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "{} processed {}", processor, this);
      }
      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime()-startTime);
    }
    
    @Override  
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeBoolean(primary != null);
      if(primary != null) {
        InternalDataSerializer.invokeToData(primary, out);
      }
    }

    @Override  
  public int getDSFID() {
    return PR_CREATE_BUCKET_REPLY_MESSAGE;
  }

    @Override  
    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
      super.fromData(in);
      boolean hasPrimary = in.readBoolean();
      if(hasPrimary) {
        primary = new InternalDistributedMember();
        InternalDataSerializer.invokeFromData(primary, in);
      }
    }

    @Override  
    public String toString() {
      return new StringBuffer()
      .append("CreateBucketReplyMessage ")
      .append("processorid=").append(this.processorId)
      .toString();
    }
  }
  
  /**
   * A processor to capture the {@link Node} returned by {@link CreateBucketMessage}
   * @since GemFire 5.0
   */
  static public class NodeResponse extends ReplyProcessor21 {
    /**
     * the message that triggers return from waitForAcceptance.  This will
     * be null if the target member exited
     */
    private volatile CreateBucketReplyMessage msg;

    public NodeResponse(InternalDistributedSystem ds, InternalDistributedMember recipient) {
      super(ds, recipient);
    }

    @Override  
    public void process(DistributionMessage msg)
    {
      try {
        if (msg instanceof CreateBucketReplyMessage) {
          CreateBucketReplyMessage reply =(CreateBucketReplyMessage) msg;
          this.msg = reply;
          if (logger.isTraceEnabled(LogMarker.DM)) {
            logger.debug("NodeResponse return value is ");
          }
        } else {
          Assert.assertTrue(msg instanceof ReplyMessage);
        }
      } finally {
        super.process(msg);
      }
    }
    
    
    /**
     * Wait for the response to a {@link CreateBucketMessage} request. 
     * @return true if the node sent the request is managing the bucket
     * @throws ForceReattemptException if the peer is no longer available
     */
    public InternalDistributedMember waitForResponse()  throws ForceReattemptException
    {
      try {
        waitForRepliesUninterruptibly();
      }
      catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof CancelException) {
          logger.debug("NodeResponse got remote cancellation, throwing PartitionedRegionCommunication Exception {}", t.getMessage(), t);
          return null;
        }
        if (t instanceof PRLocallyDestroyedException) {
          logger.debug("NodeResponse got local destroy on the PartitionRegion , throwing ForceReattemptException {}", t.getMessage(), t);
          return null;
        }
        if (t instanceof ForceReattemptException) {
          logger.debug("NodeResponse got ForceReattemptException due to local destroy on the PartitionRegion {}", t.getMessage(), t);
          return null;
        }
        if(t instanceof PartitionedRegionStorageException) {
          throw new PartitionedRegionStorageException(t.getMessage(), t);
        }
        e.handleAsUnexpected();
      }
      CreateBucketReplyMessage message = this.msg;
      if(message == null) {
        return null;
      } else {
        return (message.primary);
      }
    }
  }
}
