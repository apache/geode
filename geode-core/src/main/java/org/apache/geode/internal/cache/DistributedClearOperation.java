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
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

public class DistributedClearOperation extends DistributedCacheOperation {
  public enum OperationType {
    OP_LOCK_FOR_CLEAR, OP_CLEAR,
  }

  private final RegionVersionVector rvv;
  private final OperationType operation;
  private final Set<InternalDistributedMember> recipients;
  private VersionTag<?> operationTag;

  /**
   * A map of all the regions that are currently locked for a clear in this VM
   */
  @MakeNotStatic
  private static final ConcurrentHashMap<LockKey, DistributedRegion> lockedRegions =
      new ConcurrentHashMap<>();

  public static void regionLocked(InternalDistributedMember locker, String regionPath,
      DistributedRegion region) {
    lockedRegions.put(new LockKey(locker, regionPath), region);
  }

  public static DistributedRegion regionUnlocked(InternalDistributedMember locker,
      String regionPath) {
    return lockedRegions.remove(new LockKey(locker, regionPath));
  }


  /**
   * distribute a clear() to other members
   **/
  public static void clear(RegionEventImpl regionEvent, RegionVersionVector rvv,
      Set<InternalDistributedMember> recipients) {
    new DistributedClearOperation(DistributedClearOperation.OperationType.OP_CLEAR, regionEvent,
        rvv, recipients).distribute();
  }

  /**
   * obtain locks on version generation in other members have them do a state-flush back to this
   * member
   *
   */
  public static void lockAndFlushToOthers(RegionEventImpl regionEvent,
      Set<InternalDistributedMember> recipients) {
    DistributedClearOperation dco = new DistributedClearOperation(
        DistributedClearOperation.OperationType.OP_LOCK_FOR_CLEAR, regionEvent, null, recipients);
    dco.distribute();
  }

  @Override
  public String toString() {
    String cname = getClass().getName().substring(getClass().getPackage().getName().length() + 1);
    return cname + "(op=" + operation + ", tag=" + operationTag + "event=" + event
        + ")";
  }

  /** release locks in other members obtained for an RVV clear */
  public static void releaseLocks(RegionEventImpl regionEvent,
      Set<InternalDistributedMember> recipients) {
    DistributedRegion region = (DistributedRegion) regionEvent.getRegion();
    ReleaseClearLockMessage.send(recipients, region.getDistributionManager(), region.getFullPath());
  }


  // clear must receive responses when versioning is enabled
  @Override
  protected boolean shouldAck() {
    return true;
  }

  @Override
  public boolean containsRegionContentChange() {
    return operation == OperationType.OP_CLEAR;
  }

  /**
   * Creates new instance of DistributedClearOperation
   *
   */
  private DistributedClearOperation(OperationType op, RegionEventImpl event,
      RegionVersionVector rvv, Set<InternalDistributedMember> recipients) {
    super(event);
    this.rvv = rvv;
    operation = op;
    this.recipients = recipients;
    if (event != null) {
      operationTag = event.getVersionTag();
    }
  }

  @Override
  protected void initProcessor(CacheOperationReplyProcessor p, CacheOperationMessage msg) {
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
  protected CacheOperationMessage createMessage() {
    ClearRegionMessage mssg;
    if (event instanceof ClientRegionEventImpl) {
      mssg = new ClearRegionWithContextMessage();
      ((ClearRegionWithContextMessage) mssg).context =
          event.getContext();

    } else {
      mssg = new ClearRegionMessage();
    }
    mssg.clearOp = operation;
    mssg.eventID = event.getEventId();
    mssg.rvv = rvv;
    mssg.owner = this;
    mssg.operationTag = operationTag;
    return mssg;
  }

  @Override
  protected Set getRecipients() {
    return recipients;
  }


  public static class ClearRegionMessage extends CacheOperationMessage {

    protected EventID eventID;
    protected RegionVersionVector rvv;
    protected OperationType clearOp;
    protected VersionTag operationTag;
    protected transient DistributedClearOperation owner;

    @Override
    public boolean containsRegionContentChange() {
      return clearOp == OperationType.OP_CLEAR;
    }

    @Override
    public int getProcessorType() {
      /*
       * since clear() can block for a while we don't want to run it in a reader thread. It is also
       * okay to run this in a non-ordered executor since the operation contains its own ordering
       * information.
       */
      return OperationExecutors.HIGH_PRIORITY_EXECUTOR;
    }

    @Override
    protected InternalCacheEvent createEvent(DistributedRegion rgn) throws EntryNotFoundException {
      RegionEventImpl event = createRegionEvent(rgn);
      event.setEventID(eventID);
      if (filterRouting != null) {
        event.setLocalFilterInfo(filterRouting.getFilterInfo(rgn.getMyId()));
      }
      return event;
    }

    protected RegionEventImpl createRegionEvent(DistributedRegion rgn) {
      RegionEventImpl event = new RegionEventImpl(rgn, getOperation(), callbackArg,
          true /* originRemote */, getSender());
      event.setVersionTag(operationTag);
      return event;
    }

    @Override
    protected boolean operateOnRegion(CacheEvent event, ClusterDistributionManager dm)
        throws EntryNotFoundException {

      DistributedRegion region = (DistributedRegion) event.getRegion();
      switch (clearOp) {
        case OP_CLEAR:
          region.clearRegionLocally((RegionEventImpl) event, false, rvv);
          region.notifyBridgeClients(event);
          appliedOperation = true;
          break;
        case OP_LOCK_FOR_CLEAR:
          if (region.getDataPolicyEnum().withStorage()) {
            DistributedClearOperation.regionLocked(getSender(), region.getFullPath(), region);
            region.lockLocallyForClear(dm, getSender(), event);
          }
          appliedOperation = true;
          break;
      }
      return true;
    }

    @Override
    public int getDSFID() {
      return CLEAR_REGION_MESSAGE;
    }


    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      clearOp = OperationType.values()[in.readByte()];
      eventID = DataSerializer.readObject(in);
      rvv = DataSerializer.readObject(in);
      operationTag = DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeByte(clearOp.ordinal());
      DataSerializer.writeObject(eventID, out);
      DataSerializer.writeObject(rvv, out);
      DataSerializer.writeObject(operationTag, out);
    }

    @Override
    protected void appendFields(StringBuilder buff) {
      super.appendFields(buff);
      buff.append("; op=").append(clearOp);
      buff.append("; eventID=").append(eventID);
      buff.append("; tag=").append(operationTag);
      buff.append("; rvv=").append(rvv);
    }

  }

  public static class ClearRegionWithContextMessage extends ClearRegionMessage {
    protected transient Object context;
    protected RegionVersionVector rvv;


    @Override
    public RegionEventImpl createRegionEvent(DistributedRegion rgn) {

      ClientRegionEventImpl event = new ClientRegionEventImpl(rgn, getOperation(), callbackArg,
          true /* originRemote */, getSender(), (ClientProxyMembershipID) context);
      return event;
    }

    @Override
    protected void appendFields(StringBuilder buff) {
      super.appendFields(buff);
      buff.append("; context=").append(context);
    }

    @Override
    public int getDSFID() {
      return CLEAR_REGION_MESSAGE_WITH_CONTEXT;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.context = DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
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
      result = prime * result + ((regionPath == null) ? 0 : regionPath.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      LockKey other = (LockKey) obj;
      if (locker == null) {
        if (other.locker != null) {
          return false;
        }
      } else if (!locker.equals(other.locker)) {
        return false;
      }
      if (regionPath == null) {
        return other.regionPath == null;
      } else
        return regionPath.equals(other.regionPath);
    }

  }
}
