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
import java.util.ArrayList;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.RegionDestroyedException;
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
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RemoteOperationException;
import com.gemstone.gemfire.internal.cache.RemoteOperationMessage;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * This message is used to determine the number of Entries in a Region, or its
 * size.
 * 
 * @since GemFire 5.0
 */
public final class RemoteSizeMessage extends RemoteOperationMessage
  {
  private static final Logger logger = LogService.getLogger();
  
  /** query type for Entries */
  public static final int TYPE_ENTRIES = 0;
  /** query type for Values */
  public static final int TYPE_VALUES = 1;

  /** The list of buckets whose size is needed, if null, then all buckets */
  private ArrayList bucketIds;
  
  /** the type of query to perform */
  private int queryType;
  
  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public RemoteSizeMessage() {}

  /**
   * The message sent to a set of {@link InternalDistributedMember}s to caculate the
   * number of Entries in each of their buckets
   * 
   * @param recipients
   *          members to receive the message
   * @param regionPath
   *          the path to the region
   * @param processor the reply processor used to wait on the response
   */
  private RemoteSizeMessage(Set recipients, String regionPath, ReplyProcessor21 processor,
      int queryType) {
    super(recipients, regionPath, processor);
    this.queryType = queryType;
  }

  /**
   * @param in
   * @throws ClassNotFoundException 
   * @throws IOException 
   */
  public RemoteSizeMessage(DataInput in) throws IOException, ClassNotFoundException {
    fromData(in);
  }

  /**
   * Sends a PartitionedRegion message for {@link java.util.Map#size()}ignoring
   * any errors on send
   * 
   * @param recipients
   *          the set of members that the size message is sent to
   * @param r
   *          the Region that contains the bucket
   * @return the processor used to read the returned size
   */
  public static SizeResponse send(Set recipients, LocalRegion r)
  {
    return send(recipients, r, TYPE_ENTRIES);
  }
  
  /**
   * sends a message to the given recipients asking for the size of either
   * their primary bucket entries or the values sets of their primary
   * buckets
   * @param recipients recipients of the message
   * @param r the local PartitionedRegion instance
   * @param queryType either TYPE_ENTRIES or TYPE_VALUES
   */
  public static SizeResponse send(Set recipients, LocalRegion r,
      int queryType) {
    Assert.assertTrue(recipients != null, "RemoteSizeMessage NULL recipients set");
    SizeResponse p = new SizeResponse(r.getSystem(), recipients);
    RemoteSizeMessage m = new RemoteSizeMessage(recipients, r.getFullPath(),
        p, queryType);
    r.getDistributionManager().putOutgoing(m);
    return p;
  }

  /**
   * This message may be sent to nodes before the PartitionedRegion is
   * completely initialized due to the RegionAdvisor(s) knowing about the
   * existance of a partitioned region at a very early part of the
   * initialization
   */
  @Override
  protected final boolean failIfRegionMissing() {
    return false;
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  @Override
  public boolean canStartRemoteTransaction() {
    return false;
  }

  @Override
  protected boolean operateOnRegion(DistributionManager dm,
      LocalRegion r,long startTime) throws RemoteOperationException {
    
    int size = 0;
    if (r != null) { // bug #43372 - NPE returned when bucket not found during tx replay
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.debug("{} operateOnRegion: {}", getClass().getName(), r.getFullPath());
      }
      size = r.size();
    }
    SizeReplyMessage.send(getSender(), getProcessorId(), dm, size);
    return false;
  }

  @Override
  protected void appendFields(StringBuffer buff)
  {
    super.appendFields(buff);
    buff.append("; bucketIds=").append(this.bucketIds);
    if (queryType == TYPE_ENTRIES) {
      buff.append("; queryType=TYPE_ENTRIES");
    }
    else {
      buff.append("; queryType=TYPE_VALUES");
    }
  }

  public int getDSFID() {
    return R_SIZE_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    this.bucketIds = DataSerializer.readArrayList(in);
    this.queryType = in.readByte();
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    DataSerializer.writeArrayList(this.bucketIds, out);
    out.writeByte((byte)queryType);
  }

  public static final class SizeReplyMessage extends ReplyMessage
   {
    /** Propagated exception from remote node to operation initiator */
    private int size;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public SizeReplyMessage() {
    }

    private SizeReplyMessage(int processorId, int size) {
      this.processorId = processorId;
      this.size = size;
    }

    /**
     * @param in
     * @throws ClassNotFoundException
     * @throws IOException 
     */
    public SizeReplyMessage(DataInput in) throws IOException, ClassNotFoundException {
      fromData(in);
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId,
        DM dm, int size)
    {
      Assert.assertTrue(recipient != null,
          "SizeReplyMessage NULL reply message");
      SizeReplyMessage m = new SizeReplyMessage(processorId, size);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the
     * message.
     * 
     * @param dm
     *          the distribution manager that is processing the message.
     */
    @Override
    public void process(final DM dm, final ReplyProcessor21 processor)
    {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "{}: process invoking reply processor with processorId: {}", this.processorId);
      }

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "{} processor not found", getClass().getName());
        }
        return;
      }
      processor.process(this);

      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "{} processed {}", getClass().getName(), this);
      }
      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      out.writeInt(size);
    }

    @Override
  public int getDSFID() {
    return R_SIZE_REPLY_MESSAGE;
  }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      this.size = in.readInt();
    }

    @Override
    public String toString()
    {
      StringBuffer sb = new StringBuffer();
      sb.append(this.getClass().getName()).append(" processorid=").append(
          this.processorId).append(" reply to sender ")
          .append(this.getSender()).append(" returning size=")
          .append(getSize());
      return sb.toString();
    }

    public int getSize() {
      return size;
    }
  }

  /**
   * A processor to capture the value returned by {@link 
   * com.gemstone.gemfire.internal.cache.partitioned.GetMessage.GetReplyMessage}
   * 
   * @since GemFire 5.0
   */
  public static class SizeResponse extends ReplyProcessor21
   {
    private int returnValue;

    public SizeResponse(InternalDistributedSystem ds, Set recipients) {
      super(ds, recipients);
    }

    /**
     * The SizeResponse processor ignores remote exceptions by implmenting this
     * method. Ignoring remote exceptions is acceptable since the RemoteSizeMessage is
     * sent to all Nodes and all {@link RemoteSizeMessage.SizeReplyMessage}s are processed for
     * each individual bucket size. The hope is that any failure due to an
     * exception will be covered by healthy Nodes.
     */
    @Override
    protected void processException(ReplyException ex)
    {
      logger.debug("SizeResponse ignoring exception: {}", ex.getMessage(), ex);
    }

    @Override
    public void process(DistributionMessage msg)
    {
      try {
        if (msg instanceof SizeReplyMessage) {
          SizeReplyMessage reply = (SizeReplyMessage)msg;
          this.returnValue = reply.getSize();
        }
      }
      finally {
        super.process(msg);
      }
    }

    /**
     * @return wait for and return the size
     */
    public int waitForSize()
    {
      try {
        waitForRepliesUninterruptibly();
      }
      catch (ReplyException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RegionDestroyedException) {
          RegionDestroyedException rde = (RegionDestroyedException) cause;
          throw rde;
        }
        throw e;
      }
      return this.returnValue;
    }
  }

}
