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

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.BucketAdvisor;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * Usage:
 * DeposePrimaryBucketResponse response = DeposePrimaryBucketMessage.send(
       InternalDistributedMember, PartitionedRegion, int bucketId);
   if (response != null && response.waitForResponse()) {
     // primary was deposed
   }
 */
public class DeposePrimaryBucketMessage extends PartitionMessage {

  private static final Logger logger = LogService.getLogger();
  
  private volatile int bucketId;
  
  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public DeposePrimaryBucketMessage() {
  }

  private DeposePrimaryBucketMessage(
      InternalDistributedMember recipient, 
      int regionId, 
      ReplyProcessor21 processor,
      int bucketId) {
    super(recipient, regionId, processor);
    this.bucketId = bucketId;
  }

  /**
   * Sends a message to depose the primary bucket.
   * 
   * @param recipient the member to depose as primary
   * @param region the PartitionedRegion of the bucket
   * @param bucketId the bucket to depose primary for
   * @return the processor used to wait for the response
   */
  public static DeposePrimaryBucketResponse send(
      InternalDistributedMember recipient, 
      PartitionedRegion region,
      int bucketId) {
    
    Assert.assertTrue(recipient != null, 
        "DeposePrimaryBucketMessage NULL recipient");
    
    DeposePrimaryBucketResponse response = new DeposePrimaryBucketResponse(
        region.getSystem(), recipient, region);
    DeposePrimaryBucketMessage msg = new DeposePrimaryBucketMessage(
        recipient, region.getPRId(), response, bucketId);

    Set<InternalDistributedMember> failures = 
      region.getDistributionManager().putOutgoing(msg);
    if (failures != null && failures.size() > 0) {
      //throw new ForceReattemptException("Failed sending <" + msg + ">");
      return null;
    }
    region.getPrStats().incPartitionMessagesSent();
    return response;
  }

  public DeposePrimaryBucketMessage(DataInput in) 
  throws IOException, ClassNotFoundException {
    fromData(in);
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  @Override
  protected final boolean operateOnPartitionedRegion(DistributionManager dm,
                                                     PartitionedRegion region, 
                                                     long startTime) 
                                              throws ForceReattemptException {
    
    BucketAdvisor bucketAdvisor = 
        region.getRegionAdvisor().getBucketAdvisor(this.bucketId);
    
    bucketAdvisor.deposePrimary();
    
    region.getPrStats().endPartitionMessagesProcessing(startTime);
    DeposePrimaryBucketReplyMessage.send(
        getSender(), getProcessorId(), dm, (ReplyException)null);
    
    return false;
    
  }

  @Override
  protected void appendFields(StringBuffer buff) {
    super.appendFields(buff);
    buff.append("; bucketId=").append(this.bucketId);
  }

  public int getDSFID() {
    return PR_DEPOSE_PRIMARY_BUCKET_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.bucketId = in.readInt();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.bucketId);
  }

  public static final class DeposePrimaryBucketReplyMessage 
  extends ReplyMessage {
    
    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public DeposePrimaryBucketReplyMessage() {
    }

    public DeposePrimaryBucketReplyMessage(DataInput in)
        throws IOException, ClassNotFoundException {
      fromData(in);
    }

    private DeposePrimaryBucketReplyMessage(
        int processorId, ReplyException re) {
      setProcessorId(processorId);
      setException(re);
    }

    /** Send a reply */
    public static void send(InternalDistributedMember recipient,
                            int processorId, 
                            DM dm, 
                            ReplyException re) {
      Assert.assertTrue(recipient != null,
          "DeposePrimaryBucketReplyMessage NULL recipient");
      DeposePrimaryBucketReplyMessage m = 
          new DeposePrimaryBucketReplyMessage(processorId, re);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }
    
    boolean isSuccess() {
      return true; //this.responseCode == OK;
    }

    @Override
    public void process(final DM dm, final ReplyProcessor21 processor) {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "DeposePrimaryBucketReplyMessage process invoking reply processor with processorId: {}", this.processorId);
      }

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "DeposePrimaryBucketReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "{} processed {}",processor, this);
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
    }

    @Override
    public int getDSFID() {
      return PR_DEPOSE_PRIMARY_BUCKET_REPLY;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("DeposePrimaryBucketReplyMessage ")
        .append("processorid=").append(this.processorId)
        .append(" reply to sender ").append(this.getSender());
      return sb.toString();
    }
  }

  /**
   * A processor to capture the value returned by the
   * <code>DeposePrimaryBucketReplyMessage</code>
   */
  public static class DeposePrimaryBucketResponse extends
      PartitionResponse {
    
    public DeposePrimaryBucketResponse(
        InternalDistributedSystem ds,
        InternalDistributedMember recipient, 
        PartitionedRegion theRegion) {
      super(ds, recipient);
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof DeposePrimaryBucketReplyMessage) {
          DeposePrimaryBucketReplyMessage reply = 
              (DeposePrimaryBucketReplyMessage)msg;
          if (reply.isSuccess()) {
            if (logger.isTraceEnabled(LogMarker.DM)) {
              logger.trace(LogMarker.DM, "DeposePrimaryBucketResponse return OK");
            }
          }
          else if (logger.isTraceEnabled(LogMarker.DM)) {
            logger.trace(LogMarker.DM, "DeposePrimaryBucketResponse return NOT_PRIMARY");
          }
        }
      }
      finally {
        super.process(msg);
      }
    }

    /**
     * Ignore any incoming exception from other VMs, we just want an
     * acknowledgement that the message was processed.
     */
    @Override
    protected void processException(ReplyException ex) {
      logger.debug("DeposePrimaryBucketMessage ignoring exception {}", ex.getMessage(), ex);
    }
  }

}
