/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author Eric Zoerner
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
