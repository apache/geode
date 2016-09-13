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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.TimeoutException;
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
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * A message used for debugging purposes. For example if a test fails it can
 * call 
 * {@link com.gemstone.gemfire.internal.cache.PartitionedRegion#sendDumpB2NRegionForBucket(int)}
 * which sends this message to all VMs that have that PartitionedRegion defined.
 * 
 * @see com.gemstone.gemfire.internal.cache.PartitionedRegion#sendDumpB2NRegionForBucket(int)
 */
public final class DumpB2NRegion extends PartitionMessage
  {
  private static final Logger logger = LogService.getLogger();
  
  private int bucketId;
  private boolean onlyReturnPrimaryInfo;
 

  public DumpB2NRegion() {
  }

  private DumpB2NRegion(Set recipients, int regionId, ReplyProcessor21 processor, int bId, boolean justPrimaryInfo) {
    super(recipients, regionId, processor);
    this.bucketId = bId;
    this.onlyReturnPrimaryInfo = justPrimaryInfo;
  }

  public static DumpB2NResponse send(Set recipients, PartitionedRegion r,
      int bId, boolean justPrimaryInfo)
  {
    DumpB2NResponse p = new DumpB2NResponse(r.getSystem(), recipients);
    DumpB2NRegion m = new DumpB2NRegion(recipients, r.getPRId(),
	p, bId, justPrimaryInfo);
    r.getDistributionManager().putOutgoing(m);
    return p;
  }

  @Override
  public void process(final DistributionManager dm)
  {
    PartitionedRegion pr = null;
    
    // Get the region, or die trying...
    final long finish = System.currentTimeMillis() + 10 * 1000;
    try {
      for (;;) {
        dm.getCancelCriterion().checkCancelInProgress(null);

//      pr = null; (redundant assignment)
        pr = PartitionedRegion.getPRFromId(this.regionId);

        if (pr != null) {
          break;
        }

        if (System.currentTimeMillis() > finish) {
          ReplyException rex = new ReplyException(
              new TimeoutException("Waited too long for region to initialize"));
          sendReply(getSender(), this.processorId, dm, rex, null, 0);
          return;
        }

        // wait a little
        boolean interrupted = Thread.interrupted();
        try {
          Thread.sleep(2000);
        }
        catch (InterruptedException e) {
          interrupted = true;
          pr.checkReadiness();
        }
        finally {
          if (interrupted) Thread.currentThread().interrupt();
        }
      }

      // Now, wait for the PR to finish initializing
      pr.waitForData();

      // OK, now it's safe to process this.
      super.process(dm);
    }
    catch (CancelException e) {
      sendReply(this.sender,this.processorId , dm, new ReplyException(e), pr, 0);
    }
    catch (PRLocallyDestroyedException e) {
      sendReply(this.sender,this.processorId , dm, new ReplyException(e), pr, 0);
      return;
    }
    catch (RegionDestroyedException rde) {
      sendReply(this.sender,this.processorId , dm, new ReplyException(rde), pr, 0);
      return;
    }
  }
  

  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm, 
      PartitionedRegion pr, long startTime) throws CacheException {
    PrimaryInfo pinfo = null; 
    if (this.onlyReturnPrimaryInfo) {
      pinfo = new PrimaryInfo(pr.getRegionAdvisor().getBucket(this.bucketId).isHosting(), 
          pr.getRegionAdvisor().isPrimaryForBucket(this.bucketId), "");
    } else {
      pr.dumpB2NForBucket(this.bucketId);
    }
    DumpB2NReplyMessage.send(getSender(), getProcessorId(), dm, pinfo);

    return false;
  }
  
  public int getDSFID() {
    return PR_DUMP_B2N_REGION_MSG;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    this.bucketId = in.readInt();
    this.onlyReturnPrimaryInfo = in.readBoolean();
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    out.writeInt(this.bucketId);
    out.writeBoolean(this.onlyReturnPrimaryInfo);
  }
  

  
  public static final class DumpB2NReplyMessage extends ReplyMessage {
    private PrimaryInfo primaryInfo;
    
    public DumpB2NReplyMessage() {}
    
    private DumpB2NReplyMessage(int procid, PrimaryInfo pinfo) 
    {
      super();
      setProcessorId(procid);
      this.primaryInfo = pinfo;
    }
    
    public static void send(InternalDistributedMember recipient, int processorId, DM dm, PrimaryInfo pinfo) 
    {
      DumpB2NReplyMessage m = new DumpB2NReplyMessage(processorId, pinfo);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }
    
    
    @Override
    public void process(final DM dm, final ReplyProcessor21 processor)
    {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "DumpB2NReplyMessage process invoking reply processor with processorId: {}", this.processorId);
      }
      
      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "DumpB2NReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);
      
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "{} processed {}", processor, this);
      }
      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
    }
    
    
    public PrimaryInfo getPrimaryInfo()
    {
      return this.primaryInfo;
    }
    
    @Override
    public int getDSFID() {
      return PR_DUMP_B2N_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException
    {
      super.fromData(in);
      this.primaryInfo = (PrimaryInfo) DataSerializer.readObject(in);
    }
    
    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      DataSerializer.writeObject(this.primaryInfo, out);
    }
    
    @Override
    public String toString()
    {
      StringBuffer sb = new StringBuffer();
      sb.append("DumpB2NReplyMessage ");
      sb.append(this.processorId);
      sb.append(" from ");
      sb.append(this.getSender());
      ReplyException ex = this.getException();
      if (ex != null) {
        sb.append(" with exception ");
        sb.append(ex);
      }
      return sb.toString();
    }
  }    
  
  public static class DumpB2NResponse extends PartitionResponse  {
    public final ArrayList primaryInfos = new ArrayList();
    
    public DumpB2NResponse(InternalDistributedSystem dm, Set initMembers) {
      super(dm, initMembers);
    }

    @Override
    public void process(DistributionMessage msg)
    {
      if (msg instanceof DumpB2NReplyMessage) {
        DumpB2NReplyMessage reply = (DumpB2NReplyMessage)msg;
        if (reply.getPrimaryInfo() != null && reply.getPrimaryInfo().isHosting) {
          Object[] newBucketHost = new Object[] {reply.getSender(), Boolean.valueOf(reply.getPrimaryInfo().isPrimary), reply.getPrimaryInfo().hostToken};
          synchronized(this.primaryInfos) {
            this.primaryInfos.add(newBucketHost);
          }
        }
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "DumpB2NResponse got a primaryInfo {} from {}", reply.getPrimaryInfo(), reply.getSender());
        }
      }
      super.process(msg);
    }

    public List waitForPrimaryInfos() 
        throws ForceReattemptException {
      try {
          waitForCacheException();
      }
      catch (ForceReattemptException e) {
        logger.debug("B2NResponse got ForceReattemptException; rethrowing {}", e.getMessage(), e);
        throw e;
      }
      catch (CacheException e) {
        logger.debug("B2NResponse got remote CacheException, throwing ForceReattemptException. {}", e.getMessage(), e);
        throw new ForceReattemptException(LocalizedStrings.DumpB2NRegion_B2NRESPONSE_GOT_REMOTE_CACHEEXCEPTION_THROWING_FORCEREATTEMPTEXCEPTION.toLocalizedString(), e);
      }
      synchronized (this.primaryInfos) {
        return this.primaryInfos;
      }
    }
  }
  
  private static class PrimaryInfo implements Serializable {
    private static final long serialVersionUID = 6334695270795306178L;
    public final boolean isHosting, isPrimary;
    public final String hostToken;
    PrimaryInfo(boolean isHosting, boolean isPrimary, String hToken) {
      this.isHosting = isHosting; this.isPrimary = isPrimary;
      this.hostToken = hToken;
      if (this.isPrimary) {
        Assert.assertTrue(this.isHosting);
      }
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.partitioned.PartitionMessage#appendFields(java.lang.StringBuffer)
   */
  @Override
  protected void appendFields(StringBuffer buff) {
    super.appendFields(buff);
    buff.append(" bucketId=").append(this.bucketId)
      .append(" primaryInfoOnly=").append(this.onlyReturnPrimaryInfo);
  }

  /* (non-Javadoc)
   * @see java.lang.Object#clone()
   */
  @Override
  protected Object clone() throws CloneNotSupportedException {
    // TODO Auto-generated method stub
    return super.clone();
  }
}
