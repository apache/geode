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

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheEvent;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.SerializationVersions;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataSerializable;
import com.gemstone.gemfire.internal.cache.persistence.DiskStoreID;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.cache.FilterRoutingInfo.FilterInfo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Bruce Schuchardt
 *
 */
public class DistributedTombstoneOperation extends DistributedCacheOperation {
  private enum TOperation {
    GC,
  }

  //  private long regionVersion;
  private final Map<VersionSource, Long> regionGCVersions;

  private TOperation op;

  public static DistributedTombstoneOperation gc(DistributedRegion region, EventID eventId) {
    RegionEventImpl rev = new RegionEventImpl(region, Operation.REGION_EXPIRE_DESTROY, null, false, region.getMyId());
    rev.setEventID(eventId);
    DistributedTombstoneOperation top = new DistributedTombstoneOperation(rev);
    top.op = TOperation.GC;
    return top;
  }

  private DistributedTombstoneOperation(RegionEventImpl rev) {
    super(rev);
    //    this.regionVersion = ((DistributedRegion)rev.getRegion()).getVersionVector().getMaxTombstoneGCVersion();
    this.regionGCVersions = ((DistributedRegion) rev.getRegion()).getVersionVector().getTombstoneGCVector();
  }

  @Override
  protected boolean supportsAdjunctMessaging() {
    return false;
  }

  @Override
  boolean isOperationReliable() {
    return false; // no need to wait for required roles
  }

  @Override
  protected CacheOperationMessage createMessage() {
    TombstoneMessage mssg = new TombstoneMessage();
    //    mssg.regionVersion = this.regionVersion;
    mssg.regionGCVersions = this.regionGCVersions;
    mssg.eventID = this.event.getEventId();
    mssg.op = this.op;
    return mssg;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.DistributedCacheOperation#getLocalFilterRouting(com.gemstone.gemfire.internal.cache.FilterRoutingInfo)
   */
  @Override
  protected FilterInfo getLocalFilterRouting(FilterRoutingInfo frInfo) {
    // fix for bug #47494, CQs destroyed by Distributed GC.  CQs remove
    // the queries for a region if they see a destroy-region operation, which
    // this message uses in order not to create a customer-visible
    // GC operation
    return null;
  }

  @Override
  protected Set getRecipients() {
    CacheDistributionAdvisor advisor = getRegion().getCacheDistributionAdvisor();
    return advisor.adviseInvalidateRegion();
  }

  /**
   * returns the region versions sent to other members for tombstone collection
   */
  public Map<VersionSource, Long> getRegionGCVersions() {
    return this.regionGCVersions;
  }

  public static class TombstoneMessage extends CacheOperationMessage implements SerializationVersions {
    //    protected long regionVersion;
    protected Map<VersionSource, Long> regionGCVersions;
    protected TOperation op;
    protected EventID eventID;
    
    private static Version[] serializationVersions = null; // new Version[]{ };

    /**
     * for deserialization
     */
    public TombstoneMessage() {
    }

    @Override
    protected InternalCacheEvent createEvent(DistributedRegion rgn)
            throws EntryNotFoundException {
      RegionEventImpl event = createRegionEvent(rgn);
      event.setEventID(this.eventID);
      return event;
    }

    protected RegionEventImpl createRegionEvent(DistributedRegion rgn) {
      RegionEventImpl event = new RegionEventImpl(rgn, getOperation(),
              this.callbackArg, true /* originRemote */, getSender());
      event.setEventID(this.eventID);
      return event;
    }

    @Override
    protected boolean operateOnRegion(CacheEvent event, DistributionManager dm)
            throws EntryNotFoundException {
      boolean sendReply = true;

      DistributedRegion region = (DistributedRegion) event.getRegion();
      region.getCachePerfStats().incTombstoneGCCount();
      FilterInfo routing = null;
      if (this.filterRouting != null) {
        routing = this.filterRouting.getFilterInfo(region.getMyId());
      }

      region.expireTombstones(this.regionGCVersions, this.eventID, routing);
      this.appliedOperation = true;
      return sendReply;
    }

    public int getDSFID() {
      return TOMBSTONE_MESSAGE;
    }

    public Version[] getSerializationVersions() {
      return serializationVersions;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
            ClassNotFoundException {
      super.fromData(in);
      this.op = TOperation.values()[in.readByte()];
      //      this.regionVersion = in.readLong();
      int count = in.readInt();
      this.regionGCVersions = new HashMap<VersionSource, Long>(count);
      boolean persistent = in.readBoolean();
      for (int i = 0; i < count; i++) {
        VersionSource mbr;
        if (persistent) {
          DiskStoreID id = new DiskStoreID();
          InternalDataSerializer.invokeFromData(id, in);
          mbr = id;
        } else {
          mbr = InternalDistributedMember.readEssentialData(in);
        }
        this.regionGCVersions.put(mbr, Long.valueOf(in.readLong()));
      }
      this.eventID = (EventID)DataSerializer.readObject(in);
    }
    
    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeByte(this.op.ordinal());
      //      out.writeLong(this.regionVersion);
      out.writeInt(this.regionGCVersions.size());
      boolean persistent = false;
      if (!regionGCVersions.isEmpty()) {
        VersionSource firstEntry = regionGCVersions.keySet().iterator().next();
        if (firstEntry instanceof DiskStoreID) {
          persistent = true;
        }
      }
      out.writeBoolean(persistent);
      for (Map.Entry<VersionSource, Long> entry : this.regionGCVersions.entrySet()) {
        VersionSource member = entry.getKey();
        if (member instanceof DiskStoreID) {
          InternalDataSerializer.invokeToData((DiskStoreID) member, out);
        } else {
          ((InternalDistributedMember) member).writeEssentialData(out);
        }
        out.writeLong(entry.getValue());
      }
      DataSerializer.writeObject(this.eventID, out);
    }

    @Override
    protected void appendFields(StringBuilder buff) {
      super.appendFields(buff);
      buff.append("; op=").append(this.op);
      buff.append("; eventID=").append(this.eventID);
      buff.append("; regionGCVersions=").append(this.regionGCVersions);
    }

  }

}
