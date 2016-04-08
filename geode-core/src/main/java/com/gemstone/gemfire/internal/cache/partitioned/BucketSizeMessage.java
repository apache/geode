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
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * A message used to determine the number of bytes a Bucket consumes.
 * @since 5.0
 */

public final class BucketSizeMessage extends PartitionMessage
  {
  private static final Logger logger = LogService.getLogger();
  
  /** The list of buckets whose size is needed, if null, then all buckets */
  private int bucketId;
  
  /**
   * Empty contstructor provided for {@link com.gemstone.gemfire.DataSerializer}
   */
  public BucketSizeMessage() {
    super();
  }

  private BucketSizeMessage(InternalDistributedMember recipient,  int regionId, ReplyProcessor21 processor, int bucketId) {
    super(recipient, regionId, processor);
    this.bucketId = bucketId;
  }
  
  @Override
  final public int getProcessorType() {
    return DistributionManager.STANDARD_EXECUTOR;
  }
  
  /**
   * Sends a BucketSize message to determine the number of bytes the bucket consumes 
   * @param recipient the member that the contains keys/value message is sent to 
   * @param r  the PartitionedRegion that contains the bucket
   * @param bucketId the identity of the bucket whose size should be returned.
   * @return the processor used to read the returned size
   * @throws ForceReattemptException if the peer is no longer available
   */
  public static BucketSizeResponse send(InternalDistributedMember recipient, 
      PartitionedRegion r, int bucketId) 
      throws ForceReattemptException {
    Assert.assertTrue(recipient != null, "BucketSizeMessage NULL reply message");
    BucketSizeResponse p = new BucketSizeResponse(r.getSystem(), Collections.singleton(recipient));
    BucketSizeMessage m = new BucketSizeMessage(recipient, r.getPRId(), 
        p, bucketId);
    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException(LocalizedStrings.BucketSizeMessage_FAILED_SENDING_0.toLocalizedString(m));
    }

    return p;
  }

  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm, 
      PartitionedRegion r, long startTime) throws CacheException, ForceReattemptException {

    PartitionedRegionDataStore ds = r.getDataStore();
    final long size;
    if (ds != null) {
      size = ds.getBucketSize(bucketId);
    }
    else {
      // sender thought this member had a data store, but it doesn't
      throw new ForceReattemptException(LocalizedStrings.BucketSizeMessage_NO_DATASTORE_IN_0.toLocalizedString(dm.getDistributionManagerId()));
    }
    
    r.getPrStats().endPartitionMessagesProcessing(startTime); 
    BucketSizeReplyMessage.send(getSender(), getProcessorId(), dm, size);
    
    return false;
  }
  
  @Override
  protected void appendFields(StringBuffer buff)
  {
    super.appendFields(buff);
    buff.append("; bucketId=").append(this.bucketId);
  }

  public int getDSFID() {
    return PR_BUCKET_SIZE_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    this.bucketId = in.readInt();
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    out.writeInt(this.bucketId); // fix for bug 38228
  }
  
  public static final class BucketSizeReplyMessage extends HighPriorityDistributionMessage {
    /** The shared obj id of the ReplyProcessor */
    private int processorId;
    
    /** Propagated exception from remote node to operation initiator */
    private long size;
  
    /**
     * Empty constructor to conform to DataSerializable interface 
     */
    public BucketSizeReplyMessage() {
    }
  
    private BucketSizeReplyMessage(int processorId, long size)
    {
      this.processorId = processorId;
      this.size = size;
    }
    
    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId, DM dm, long size) 
    {
      Assert.assertTrue(recipient != null, "PRDistribuedGetReplyMessage NULL reply message");
      BucketSizeReplyMessage m = new BucketSizeReplyMessage(processorId, size);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }
      
    /**
     * Processes this message.  This method is invoked by the receiver
     * of the message.
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    protected void process(final DistributionManager dm) {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "PRDistributedBucketSizeReplyMessage process invoking reply processor with processorId: {}", this.processorId);
      }
  
      ReplyProcessor21 processor = ReplyProcessor21.getProcessor(this.processorId);
  
      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.debug("PRDistributedBucketSizeReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);
  
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "{} Processed {}", processor, this);
      }
      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime()-startTime);
    }
    
  public int getDSFID() {
    return PR_BUCKET_SIZE_REPLY_MESSAGE;
  }

  @Override
    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
      this.size = in.readLong();
    }
  
  @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(processorId);
      out.writeLong(this.size);
    }
  
  @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("PRDistributedBucketSizeReplyMessage ")
      .append("processorid=").append(this.processorId)
      .append(" reply to sender ").append(this.getSender())
      .append(" returning numEntries=").append(getSize());
      return sb.toString();
    }

    public long getSize()
    {
      return this.size;
    }
  }
  /**
   * A processor to capture the value returned by {@link 
   * com.gemstone.gemfire.internal.cache.partitioned.GetMessage.GetReplyMessage}
   * @since 5.0
   */
  public static class BucketSizeResponse extends ReplyProcessor21  {
    private volatile long returnValue;
    
    public BucketSizeResponse(InternalDistributedSystem ds, Set recipients) {
      super(ds, recipients);
    }

    @Override
    public void process(DistributionMessage msg)
    {
      try {
        if (msg instanceof BucketSizeReplyMessage) {
          BucketSizeReplyMessage reply = (BucketSizeReplyMessage) msg;
          this.returnValue = reply.getSize();
          if (logger.isTraceEnabled(LogMarker.DM)) {
            logger.trace(LogMarker.DM, "BucketSizeResponse return value is {}", this.returnValue);
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
      }
      catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof com.gemstone.gemfire.CancelException) {
          logger.debug("BucketSizeResponse got remote cancellation; forcing reattempt. {}", t.getMessage(), t);
          throw new ForceReattemptException(LocalizedStrings.BucketSizeMessage_BUCKETSIZERESPONSE_GOT_REMOTE_CACHECLOSEDEXCEPTION_FORCING_REATTEMPT.toLocalizedString(), t);
        }
        if (t instanceof ForceReattemptException) {
          logger.debug("BucketSizeResponse got remote Region destroyed; forcing reattempt. {}", t.getMessage(), t);
          throw new ForceReattemptException(LocalizedStrings.BucketSizeMessage_BUCKETSIZERESPONSE_GOT_REMOTE_REGION_DESTROYED_FORCING_REATTEMPT.toLocalizedString(), t);
        }
        e.handleAsUnexpected();
      }
      return this.returnValue;
    }
  }

}
