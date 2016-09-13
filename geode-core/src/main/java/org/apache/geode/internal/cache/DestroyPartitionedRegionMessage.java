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

package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.partitioned.PartitionMessage;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor.PartitionProfile;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * This message is sent for two purposes <br>
 * 1) To destroy the
 * {@link org.apache.geode.internal.cache.PartitionedRegion} for all members
 * specified (typically sent to all members that have the
 * <code>PartitionedRegion</code> defined.) <br>
 * 2) To inform the other nodes that {@link org.apache.geode.internal.cache.PartitionedRegion} is closed/locally destroyed or cache is closed on a node<br>
 * This results in updating of the RegionAdvisor of the remote nodes.
 * 
 * Sending this message should flush all previous
 * {@link org.apache.geode.cache.Region} operations, which means this
 * operation should not over-ride
 * {@link org.apache.geode.internal.cache.partitioned.PartitionMessage#getProcessorId()}.  It is critical 
 * guarantee delivery of events sent prior to this message.

 * A standard {@link ReplyMessage} is used to send the reply, however any exception that it carries
 * is ignored, preventing interuption after sending this message.
 * 
 * @since GemFire 5.0
 */
public final class DestroyPartitionedRegionMessage extends PartitionMessage
{
  private static final Logger logger = LogService.getLogger();
  
  private Object cbArg;

  /** The specific destroy operation performed on the sender */
  private Operation op;
  
  /** Serial number of the region being removed */
  private int prSerial;
  
  /** Serial numbers of the buckets for this region */
  private int bucketSerials[];
  
  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public DestroyPartitionedRegionMessage() {
  }

  /**
   * 
   * @param recipients the set of members on which the partitioned region should be destoryed
   * @param region the partitioned region
   * @param processor the processor that the reply will use to notify of the reply.
   * @see #send(Set, PartitionedRegion, RegionEventImpl, int[])
   */
  private DestroyPartitionedRegionMessage(Set recipients, PartitionedRegion region,
      ReplyProcessor21 processor,
      final RegionEventImpl event, int serials[]) {
    super(recipients, region.getPRId(), processor);
    this.cbArg = event.getRawCallbackArgument();
    this.op = event.getOperation();
    this.prSerial = region.getSerialNumber();
    Assert.assertTrue(this.prSerial != DistributionAdvisor.ILLEGAL_SERIAL);
    this.bucketSerials = serials;
  }

  /**
   * 
   * @param recipients
   *          set of members who have the PartitionedRegion defined.
   * @param r
   *          the PartitionedRegion to destroy on each member
   * @return the response on which to wait for the confirmation
   */
  public static DestroyPartitionedRegionResponse send(Set recipients, PartitionedRegion r,
      final RegionEventImpl event, int serials[])
  {
    Assert.assertTrue(recipients != null, "DestroyMessage NULL recipients set");
    DestroyPartitionedRegionResponse resp = new DestroyPartitionedRegionResponse(r.getSystem(),
        recipients);
    DestroyPartitionedRegionMessage m = new DestroyPartitionedRegionMessage(
        recipients, r,
        resp, event, serials);
    r.getDistributionManager().putOutgoing(m);
    return resp;
  }

  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm, 
      PartitionedRegion r, long startTime) throws CacheException {
    if (this.op.isLocal()) {
      // notify the advisor that the sending member has locally destroyed (or closed) the region
            
      PartitionProfile pp = r.getRegionAdvisor().getPartitionProfile(getSender());
      if (pp==null) { //Fix for bug#36863
        return true;
      }
     // final Lock isClosingWriteLock  = r.getRegionAdvisor().getPartitionProfile(getSender()).getIsClosingWriteLock();
      
      Assert.assertTrue(this.prSerial != DistributionAdvisor.ILLEGAL_SERIAL);
      
      boolean ok = true;
      // Examine this peer's profile and look at the serial number in that
      // profile.  If we have a newer profile, ignore the request.

      int oldSerial = pp.getSerialNumber();
      if (DistributionAdvisor.isNewerSerialNumber(oldSerial, this.prSerial)) {
        ok = false;
        if (logger.isDebugEnabled()) {
          logger.debug("Not removing region {}l serial requested = {}; actual is {}", r.getName(), this.prSerial, r.getSerialNumber());
        }
      }
      if (ok) {
        RegionAdvisor ra = r.getRegionAdvisor();
        ra.removeIdAndBuckets(this.sender, this.prSerial, this.bucketSerials, !this.op.isClose());
//      r.getRegionAdvisor().removeId(this.sender);
      }
      
      sendReply(getSender(), getProcessorId(), dm, null, r, startTime);
     /* } finally {
        isClosingWriteLock.unlock();
      }*/
      return false;
    }
    
    // If region's isDestroyed flag is true, we can check if local destroy is done or not and if NOT, 
    // we can invoke destroyPartitionedRegionLocally method.
    if (r.isDestroyed()) {
      boolean isClose = this.op.isClose();
      r.destroyPartitionedRegionLocally(!isClose);
      return true;
    }
    
    if (logger.isTraceEnabled(LogMarker.DM)) {  
      logger.trace(LogMarker.DM, "{} operateOnRegion: {}", getClass().getName(), r.getFullPath());
    }
    RegionEventImpl event = new RegionEventImpl(r, this.op,
        this.cbArg, true, r.getMyId());
    r.basicDestroyRegion(event, false, false, true);

    return true;
  }

  @Override
  protected void appendFields(StringBuffer buff)
  {
    super.appendFields(buff);
    buff.append("; cbArg=").append(this.cbArg).append("; op=").append(this.op);
    buff.append("; prSerial=" + prSerial);
    buff.append("; bucketSerials (" + bucketSerials.length + ")=(");
    for (int i = 0; i < bucketSerials.length; i ++) {
      buff.append(Integer.toString(bucketSerials[i]));
      if (i < bucketSerials.length - 1) {
        buff.append(", ");
      }
    }
  }

  public int getDSFID() {
    return DESTROY_PARTITIONED_REGION_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    this.cbArg = DataSerializer.readObject(in);
    this.op = Operation.fromOrdinal(in.readByte());
    this.prSerial = in.readInt();
    int len = in.readInt();
    this.bucketSerials = new int[len];
    for (int i = 0; i < len ; i ++) {
      this.bucketSerials[i] = in.readInt();
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    DataSerializer.writeObject(this.cbArg, out);
    out.writeByte(this.op.ordinal);
    out.writeInt(this.prSerial);
    out.writeInt(this.bucketSerials.length);
    for (int i = 0; i < this.bucketSerials.length; i ++) {
      out.writeInt(this.bucketSerials[i]);
    }
  }

  /**
   * The response on which to wait for all the replies.  This response ignores any exceptions received from the "far side"
   * 
   * @since GemFire 5.0
   */
  static public class DestroyPartitionedRegionResponse extends ReplyProcessor21
  {
    public DestroyPartitionedRegionResponse(InternalDistributedSystem system,
        Set initMembers) {
      super(system, initMembers);
    }

    /**
     * Ignore any incoming exception from other VMs, we just want an
     * acknowledgement that the message was processed.
     */
    @Override
    protected void processException(ReplyException ex)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("DestroyRegionResponse ignoring exception", ex);
      }
    }
  }

}
