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
import java.util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.DataSerializer;


/**
 *
 *
 */
public class InvalidateRegionOperation extends DistributedCacheOperation {

  /** Creates new instance of InvalidateRegionOperation */
  public InvalidateRegionOperation(RegionEventImpl event) {
    super(event);
  }
  
  @Override
  protected CacheOperationMessage createMessage() {
    InvalidateRegionMessage msg = new InvalidateRegionMessage();
    RegionEventImpl regionEvent = (RegionEventImpl)this.event;
    msg.eventID = regionEvent.getEventId();
    return msg ;
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
  
  public static final class InvalidateRegionMessage extends CacheOperationMessage {
  
    protected EventID eventID;
    
    @Override
    protected InternalCacheEvent createEvent(DistributedRegion rgn)
    throws EntryNotFoundException {
      RegionEventImpl event = new RegionEventImpl(rgn,
          getOperation(),
          this.callbackArg,
          true, getSender());
      event.setEventID(this.eventID);
      if (this.filterRouting != null) {
        event.setLocalFilterInfo(this.filterRouting
            .getFilterInfo((InternalDistributedMember)rgn.getMyId()));
      }
      return event;
    }    
  
    @Override
    protected boolean operateOnRegion(CacheEvent event, DistributionManager dm) throws EntryNotFoundException {
      RegionEventImpl ev = (RegionEventImpl)event;
      DistributedRegion rgn = (DistributedRegion)ev.region;
      
      rgn.basicInvalidateRegion(ev);
      this.appliedOperation = true; 
      return true;
    }
    public int getDSFID() {
      return INVALIDATE_REGION_MESSAGE;
    }
    
    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      this.eventID = (EventID)DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      DataSerializer.writeObject(this.eventID, out);
    }
  }
}
