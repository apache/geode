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
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.internal.cache.BucketAdvisor;
import com.gemstone.gemfire.internal.cache.BucketAdvisor.BucketProfile;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * A Partitioned Region meta-data update message.  This is used to send 
 * a bucket's meta-data to other members with the same Partitioned Region.  
 * 
 * @since 5.1
 */
public final class BucketProfileUpdateMessage extends DistributionMessage
    implements MessageWithReply
{
  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = 1L;
  private int prId;
  private int bucketId;
  private int processorId = 0;
  private BucketAdvisor.BucketProfile profile;

  public BucketProfileUpdateMessage() {}
  
  @Override
  final public int getProcessorType() {
    return DistributionManager.WAITING_POOL_EXECUTOR;
  }

  private BucketProfileUpdateMessage(Set recipients, int partitionedRegionId, int processorId, int bucketId, BucketProfile profile) {
    setRecipients(recipients);
    this.processorId = processorId;
    this.prId = partitionedRegionId;
    this.bucketId = bucketId;
    this.profile = profile;
  }

  @Override
  public int getProcessorId() {
    return this.processorId;
  }

  @Override
  public boolean sendViaUDP() {
    return true;
  }

  @Override
  protected void process(DistributionManager dm)
  {
    try {
      PartitionedRegion pr = PartitionedRegion.getPRFromId(this.prId);
//      pr.waitOnBucketInitialization();  // While PR doesn't directly do GII, wait on this for bucket initialization -- mthomas 5/17/2007
      pr.getRegionAdvisor().putBucketProfile(this.bucketId, this.profile);
    }
    catch (PRLocallyDestroyedException fre) {
      if (logger.isDebugEnabled())
        logger.debug("<region locally destroyed> ///{}", this);
    }
    catch (RegionDestroyedException e) {
      if (logger.isDebugEnabled())
        logger.debug("<region destroyed> ///{}", this);
    }
    catch (CancelException e) {
      if (logger.isDebugEnabled())
        logger.debug("<cache closed> ///{}", this);
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable ignore) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
    }
    finally {
      if (this.processorId != 0) {
        ReplyMessage.send(getSender(), this.processorId, null, dm);
      }
    }
  }
  
  /**
   * Send a profile update to a set of members.
   * @param recipients the set of members to be notified
   * @param dm the distribution manager used to send the message
   * @param prId the unique partitioned region identifier 
   * @param bucketId the unique bucket identifier
   * @param bp the updated bucket profile to send 
   * @param requireAck whether or not to expect a reply
   * @return an instance of reply processor if requireAck is true on which the caller
   * can wait until the event has finished. 
   */
  public static ReplyProcessor21 send(Set recipients, DM dm, int prId, int bucketId, BucketProfile bp, boolean requireAck)
  {
    if (recipients.isEmpty()) {
      return null;
    }
    ReplyProcessor21 rp = null;
    int procId = 0; 
    if (requireAck) {
      rp = new ReplyProcessor21(dm, recipients);
      procId = rp.getProcessorId();
    }
    BucketProfileUpdateMessage m = new BucketProfileUpdateMessage(recipients, prId, procId, bucketId, bp);
    dm.putOutgoing(m);
    return rp;
  }

  public int getDSFID() {
    return PR_BUCKET_PROFILE_UPDATE_MESSAGE;
  }
  
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    this.prId = in.readInt();
    this.bucketId = in.readInt();
    this.processorId = in.readInt();
    this.profile = (BucketAdvisor.BucketProfile)DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    out.writeInt(this.prId);
    out.writeInt(this.bucketId);
    out.writeInt(this.processorId);
    DataSerializer.writeObject(this.profile, out);
  }

  @Override
  public String toString()
  {
    StringBuffer buff = new StringBuffer();
    String className = getClass().getName();
    String shortName = 
      className.substring(
          className.lastIndexOf('.', className.lastIndexOf('.') - 1) + 1);  // partition.<foo>
    return buff.append(shortName)
    .append("(prid=").append(this.prId)
    .append("; bucketid=").append(this.bucketId)
    .append("; sender=").append(getSender())
    .append("]; processorId=").append(this.processorId)
    .append("; profile=").append(this.profile)
    .append(")").toString();
  }
}
