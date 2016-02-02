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
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheEvent;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

/**
 * 
 * @author Asif
 * 
 */
public class DistributedClearOperation extends DistributedCacheOperation
{
  public static enum OperationType {
    OP_LOCK_FOR_CLEAR,
    OP_CLEAR,
  }
  private RegionVersionVector rvv;
  private OperationType operation;
  private Set<InternalDistributedMember> recipients;
  private VersionTag<?> operationTag;
  
  /**
   * A map of all the regions that are currently locked for a clear in this VM
   */
  private static final ConcurrentHashMap<LockKey, DistributedRegion> lockedRegions = new ConcurrentHashMap<LockKey, DistributedRegion>();
  
  public static void regionLocked(InternalDistributedMember locker, String regionPath, DistributedRegion region) {
    lockedRegions.put(new LockKey(locker, regionPath), region);
  }
  
  public static DistributedRegion regionUnlocked(InternalDistributedMember locker, String regionPath) {
    return lockedRegions.remove(new LockKey(locker, regionPath));
  }
  

  /** distribute a clear() to other members 
   **/
  public static void clear(RegionEventImpl regionEvent, RegionVersionVector rvv, Set<InternalDistributedMember> recipients) {
    new DistributedClearOperation(
        DistributedClearOperation.OperationType.OP_CLEAR,
        regionEvent, rvv, recipients).distribute();
  }
  
  /** obtain locks on version generation in other members have them do a state-flush back to this member 
   * @param recipients */
  public static void lockAndFlushToOthers(RegionEventImpl regionEvent, Set<InternalDistributedMember> recipients) {
    DistributedClearOperation dco = new DistributedClearOperation(
        DistributedClearOperation.OperationType.OP_LOCK_FOR_CLEAR,
        regionEvent, null, recipients);
    dco.distribute();
  }

  @Override
  public String toString() {
    String cname = getClass().getName().substring(
        getClass().getPackage().getName().length() + 1);
    return cname + "(op=" + this.operation + ", tag=" + this.operationTag + "event=" + this.event + ")";
  }

  /** release locks in other members obtained for an RVV clear */
  public static void releaseLocks(RegionEventImpl regionEvent, Set<InternalDistributedMember> recipients) {
    DistributedRegion region =  (DistributedRegion) regionEvent.getRegion();
    ReleaseClearLockMessage.send(recipients, region.getDistributionManager(), region.getFullPath());
  }

  
  // clear must receive responses when versioning is enabled
  @Override
  protected boolean shouldAck() {
    return true;
  }

  @Override
  public boolean containsRegionContentChange() {
    return this.operation == OperationType.OP_CLEAR;
  }
  
  /** Creates new instance of DistributedClearOperation 
   * @param recipients */
  private DistributedClearOperation(OperationType op, RegionEventImpl event, RegionVersionVector rvv, Set<InternalDistributedMember> recipients) {
    super(event);
    this.rvv = rvv;
    this.operation = op;
    this.recipients = recipients;
    if (event != null) {
      this.operationTag = event.getVersionTag();
    }
  }

  @Override
  protected void initProcessor(CacheOperationReplyProcessor p,
      CacheOperationMessage msg) {
    super.initProcessor(p, msg);
  }

  @Override
  protected boolean supportsAdjunctMessaging() {
    return false;
  }
 
  @Override
  public boolean supportsMulticast() {
    return false;
  }
 
  @Override
  protected CacheOperationMessage createMessage()
  {
    ClearRegionMessage mssg;
    if (this.event instanceof ClientRegionEventImpl) {
      mssg = new ClearRegionWithContextMessage();
      ((ClearRegionWithContextMessage)mssg).context = ((ClientRegionEventImpl)this.event)
          .getContext();

    }
    else {
      mssg = new ClearRegionMessage();
    }
    mssg.clearOp = this.operation;
    mssg.eventID = this.event.getEventId();
    mssg.rvv = this.rvv;
    mssg.owner = this;
    mssg.operationTag = this.operationTag;
    return mssg;
  }
  
  @Override
  protected Set getRecipients()
  {
    return recipients;
  }
  
  
  public static class ClearRegionMessage extends CacheOperationMessage
  {

    protected EventID eventID;
    protected RegionVersionVector rvv;
    protected OperationType clearOp;
    protected VersionTag operationTag;
    protected transient DistributedClearOperation owner;
    
    @Override
    public boolean containsRegionContentChange() {
      return this.clearOp == OperationType.OP_CLEAR;
    }

    @Override
    public int getProcessorType() {
      /* since clear() can block for a while we don't want to run it in a reader
       * thread.  It is also okay to run this in a non-ordered executor since
       * the operation contains its own ordering information.
       */
      return DistributionManager.HIGH_PRIORITY_EXECUTOR;
    }

    @Override
    protected InternalCacheEvent createEvent(DistributedRegion rgn)
        throws EntryNotFoundException
    {
      RegionEventImpl event = createRegionEvent(rgn);
      event.setEventID(this.eventID);
      if (this.filterRouting != null) {
        event.setLocalFilterInfo(this.filterRouting
            .getFilterInfo(rgn.getMyId()));
      }
      return event;
    }
    
    protected RegionEventImpl createRegionEvent(DistributedRegion rgn)
    {     
      RegionEventImpl event = new RegionEventImpl(rgn, getOperation(),
          this.callbackArg, true /* originRemote */, getSender());
      event.setVersionTag(this.operationTag);
      return event;
    }
    
    @Override
    protected boolean operateOnRegion(CacheEvent event, DistributionManager dm)
        throws EntryNotFoundException
    {
      
      DistributedRegion region = (DistributedRegion)event.getRegion();
      switch (this.clearOp) {
      case OP_CLEAR:
        region.clearRegionLocally((RegionEventImpl)event, false, this.rvv);
        region.notifyBridgeClients((RegionEventImpl)event);
        this.appliedOperation = true;
        break;
      case OP_LOCK_FOR_CLEAR:
        if (region.getDataPolicy().withStorage()) {
          DistributedClearOperation.regionLocked(this.getSender(), region.getFullPath(), region);
          region.lockLocallyForClear(dm, this.getSender());
        }
        this.appliedOperation = true;
        break;
      }
      return true;
    }

    public int getDSFID() {
      return CLEAR_REGION_MESSAGE;
    }
    

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      this.clearOp = OperationType.values()[in.readByte()];
      this.eventID = (EventID)DataSerializer.readObject(in);
      this.rvv = (RegionVersionVector)DataSerializer.readObject(in);
      this.operationTag = (VersionTag<?>)DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      out.writeByte(this.clearOp.ordinal());
      DataSerializer.writeObject(this.eventID, out);
      DataSerializer.writeObject(this.rvv, out);
      DataSerializer.writeObject(this.operationTag, out);
    }

    @Override
    protected void appendFields(StringBuilder buff)
    {
      super.appendFields(buff);
      buff.append("; op=").append(this.clearOp);
      buff.append("; eventID=").append(this.eventID);
      buff.append("; tag=").append(this.operationTag);
      buff.append("; rvv=").append(this.rvv);
    }

  }

  public static final class ClearRegionWithContextMessage extends ClearRegionMessage
  {
    protected transient Object context;
    protected RegionVersionVector rvv;

    
    @Override
    final public RegionEventImpl createRegionEvent(DistributedRegion rgn)
    {
      
      ClientRegionEventImpl event = new ClientRegionEventImpl(rgn,
          getOperation(), this.callbackArg, true /* originRemote */,
          getSender(), (ClientProxyMembershipID)this.context);
      return event;
    }

    @Override
    protected void appendFields(StringBuilder buff)
    {
      super.appendFields(buff);
      buff.append("; context=").append(this.context);
    }

    @Override
    public int getDSFID() {
      return CLEAR_REGION_MESSAGE_WITH_CONTEXT;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      this.context = DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      DataSerializer.writeObject(this.context, out);
    }
  }
  
  public static class LockKey {

    private final InternalDistributedMember locker;
    private final String regionPath;

    public LockKey(InternalDistributedMember locker, String regionPath) {
      this.locker = locker;
      this.regionPath = regionPath;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((locker == null) ? 0 : locker.hashCode());
      result = prime * result
          + ((regionPath == null) ? 0 : regionPath.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      LockKey other = (LockKey) obj;
      if (locker == null) {
        if (other.locker != null)
          return false;
      } else if (!locker.equals(other.locker))
        return false;
      if (regionPath == null) {
        if (other.regionPath != null)
          return false;
      } else if (!regionPath.equals(other.regionPath))
        return false;
      return true;
    }
    
  }
}
