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
 * This message is sent to a recipient to make it become the primary for a
 * partitioned region bucket. The recipient will get in line for the bucket's
 * primary lock and then send a {@link DeposePrimaryBucketMessage} to the
 * current primary.
 * 
 * Usage:
 * BecomePrimaryBucketResponse response = BecomePrimaryBucketMessage.send(
 *     InternalDistributedMember, PartitionedRegion, int bucketId);
 * if (response != null && response.waitForResponse()) {
 *   // recipient became primary for the bucket
 * }
 * 
 */
public class BecomePrimaryBucketMessage extends PartitionMessage {

  private static final Logger logger = LogService.getLogger();
  
  private int bucketId;
  private boolean isRebalance;
  
  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public BecomePrimaryBucketMessage() {
  }

  private BecomePrimaryBucketMessage(InternalDistributedMember recipient, 
                                     int regionId, 
                                     ReplyProcessor21 processor,
                                     int bucketId,
                                     boolean isRebalance) {
    super(recipient, regionId, processor);
    this.bucketId = bucketId;
    this.isRebalance = isRebalance;
  }

  /**
   * Sends a message to make the recipient primary for the bucket.
   * 
   * @param recipient the member to to become primary
   * @param pr the PartitionedRegion of the bucket
   * @param bid the bucket to become primary for
   * @param isRebalance true if directed to become primary by rebalancing
   * @return the processor used to wait for the response
   */
  public static BecomePrimaryBucketResponse send(
      InternalDistributedMember recipient, 
      PartitionedRegion pr, 
      int bid,
      boolean isRebalance) {
    
    Assert.assertTrue(recipient != null, 
        "BecomePrimaryBucketMessage NULL recipient");
    
    BecomePrimaryBucketResponse response = new BecomePrimaryBucketResponse(
        pr.getSystem(), recipient, pr);
    BecomePrimaryBucketMessage msg = new BecomePrimaryBucketMessage(
        recipient, pr.getPRId(), response, bid, isRebalance);

    Set<InternalDistributedMember> failures = 
      pr.getDistributionManager().putOutgoing(msg);
    if (failures != null && failures.size() > 0) {
      //throw new ForceReattemptException("Failed sending <" + msg + ">");
      return null;
    }
    pr.getPrStats().incPartitionMessagesSent();
    return response;
  }

  public BecomePrimaryBucketMessage(DataInput in) 
  throws IOException, ClassNotFoundException {
    fromData(in);
  }

  @Override
  public int getProcessorType() {
    // use the waiting pool because operateOnPartitionedRegion will 
    // send out a DeposePrimaryBucketMessage and wait for the reply
    return DistributionManager.WAITING_POOL_EXECUTOR;
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
    
    // this is executing in the WAITING_POOL_EXECUTOR
    byte responseCode = BecomePrimaryBucketReplyMessage.NOT_SECONDARY;
    BucketAdvisor bucketAdvisor = 
        region.getRegionAdvisor().getBucketAdvisor(this.bucketId);
    
    if (bucketAdvisor.isHosting()) {
      if (bucketAdvisor.becomePrimary(this.isRebalance)) { // sends a request/reply message
        responseCode = BecomePrimaryBucketReplyMessage.OK;
      }
    }
    
    region.getPrStats().endPartitionMessagesProcessing(startTime);
    BecomePrimaryBucketReplyMessage.send(
        getSender(), getProcessorId(), dm, null, responseCode);
    
    return false;
    
  }

  @Override
  protected void appendFields(StringBuffer buff) {
    super.appendFields(buff);
    buff.append("; bucketId=").append(this.bucketId);
    buff.append("; isRebalance=").append(this.isRebalance);
  }

  public int getDSFID() {
    return PR_BECOME_PRIMARY_BUCKET_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) 
  throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.bucketId = in.readInt();
    this.isRebalance = in.readBoolean();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.bucketId);
    out.writeBoolean(this.isRebalance);
  }

  public static final class BecomePrimaryBucketReplyMessage 
  extends ReplyMessage {
    
    static final byte NOT_SECONDARY = 0;
    static final byte OK = 1;
    
    private byte responseCode = NOT_SECONDARY;
    
    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public BecomePrimaryBucketReplyMessage() {
    }

    public BecomePrimaryBucketReplyMessage(DataInput in)
        throws IOException, ClassNotFoundException {
      fromData(in);
    }

    private BecomePrimaryBucketReplyMessage(
        int processorId, ReplyException re, byte responseCode) {
      setProcessorId(processorId);
      setException(re);
      this.responseCode = responseCode;
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient,
                            int processorId, 
                            DM dm, 
                            ReplyException re,
                            byte responseCode) {
      Assert.assertTrue(recipient != null,
          "BecomePrimaryBucketReplyMessage NULL recipient");
      BecomePrimaryBucketReplyMessage m = 
          new BecomePrimaryBucketReplyMessage(processorId, re, responseCode);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }
    
    boolean isSuccess() {
      return this.responseCode == OK;
    }

    @Override
    public void process(final DM dm, final ReplyProcessor21 processor) {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "BecomePrimaryBucketReplyMessage process invoking reply processor with processorId:{}",
            this.processorId);
      }

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "BecomePrimaryBucketReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "processed {}", this);
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeByte(responseCode);
    }

    @Override
    public int getDSFID() {
      return PR_BECOME_PRIMARY_BUCKET_REPLY;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
      this.responseCode = in.readByte();
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("BecomePrimaryBucketReplyMessage ")
        .append("processorid=").append(this.processorId)
        .append(" reply to sender ").append(this.getSender())
        .append(" returning responseCode=").append(this.responseCode);
      return sb.toString();
    }
  }

  /**
   * A processor to capture the value returned by 
   * BecomePrimaryBucketReplyMessage.
   */
  public static class BecomePrimaryBucketResponse extends PartitionResponse {
    
    private volatile boolean success; 

    public BecomePrimaryBucketResponse(InternalDistributedSystem ds,
                                       InternalDistributedMember recipient, 
                                       PartitionedRegion theRegion) {
      super(ds, recipient);
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof BecomePrimaryBucketReplyMessage) {
          BecomePrimaryBucketReplyMessage reply = 
              (BecomePrimaryBucketReplyMessage)msg;
          this.success = reply.isSuccess();
          if (reply.isSuccess()) {
            if (logger.isTraceEnabled(LogMarker.DM)) {
              logger.trace(LogMarker.DM, "BecomePrimaryBucketResponse return OK");
            }
          }
          else if (logger.isTraceEnabled(LogMarker.DM)) {
            logger.trace(LogMarker.DM, "BecomePrimaryBucketResponse return NOT_PRIMARY");
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
      logger.debug("BecomePrimaryBucketMessage ignoring exception {}", ex.getMessage(), ex);
    }
    
    /**
     * @return true if recipient successfully became or was already the primary
     */
    public boolean waitForResponse() {
      waitForRepliesUninterruptibly();
      return this.success;
    }
  }

}
