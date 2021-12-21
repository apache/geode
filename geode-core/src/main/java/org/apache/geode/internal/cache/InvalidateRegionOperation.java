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

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;


public class InvalidateRegionOperation extends DistributedCacheOperation {

  /** Creates new instance of InvalidateRegionOperation */
  public InvalidateRegionOperation(RegionEventImpl event) {
    super(event);
  }

  @Override
  protected CacheOperationMessage createMessage() {
    InvalidateRegionMessage msg = new InvalidateRegionMessage();
    RegionEventImpl regionEvent = (RegionEventImpl) event;
    msg.eventID = regionEvent.getEventId();
    return msg;
  }

  @Override
  protected Set getRecipients() {
    CacheDistributionAdvisor advisor = getRegion().getCacheDistributionAdvisor();
    return advisor.adviseInvalidateRegion();
  }

  @Override
  protected boolean supportsAdjunctMessaging() {
    return false;
  }

  public static class InvalidateRegionMessage extends CacheOperationMessage {

    protected EventID eventID;

    @Override
    protected InternalCacheEvent createEvent(DistributedRegion rgn) throws EntryNotFoundException {
      RegionEventImpl event =
          new RegionEventImpl(rgn, getOperation(), callbackArg, true, getSender());
      event.setEventID(eventID);
      if (filterRouting != null) {
        event.setLocalFilterInfo(
            filterRouting.getFilterInfo(rgn.getMyId()));
      }
      return event;
    }

    @Override
    protected boolean operateOnRegion(CacheEvent event, ClusterDistributionManager dm)
        throws EntryNotFoundException {
      RegionEventImpl ev = (RegionEventImpl) event;
      DistributedRegion rgn = (DistributedRegion) ev.region;

      rgn.basicInvalidateRegion(ev);
      appliedOperation = true;
      return true;
    }

    @Override
    public int getDSFID() {
      return INVALIDATE_REGION_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      eventID = DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeObject(eventID, out);
    }
  }
}
