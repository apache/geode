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
import java.util.Collections;
import java.util.List;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheEvent;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.distributed.internal.ConflationKey;
import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.versions.ConcurrentCacheModificationException;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;

/**
 * Handles distribution messaging for destroying an entry in a region.
 * 
 *  
 */
public class DestroyOperation extends DistributedCacheOperation
{
  /** Creates a new instance of DestroyOperation */
  public DestroyOperation(EntryEventImpl event) {
    super(event);
  }

  @Override
  protected CacheOperationMessage createMessage()
  {
    if (this.event.hasClientOrigin()) {
      DestroyWithContextMessage msgwithContxt = new DestroyWithContextMessage(event);
      msgwithContxt.context = ((EntryEventImpl)this.event).getContext();
      return msgwithContxt;
    }
    else {
      return new DestroyMessage(event);
    }
  }

  @Override
  protected void initMessage(CacheOperationMessage msg,
      DirectReplyProcessor processor)
  {
    super.initMessage(msg, processor);
    DestroyMessage m = (DestroyMessage)msg;
    EntryEventImpl event = getEvent();
    m.key = event.getKey();
    m.eventId = event.getEventId();
  }

  public static class DestroyMessage extends CacheOperationMessage
  {
    protected EventID eventId = null;

    protected Object key;
    
    protected EntryEventImpl event = null;
    
    private Long tailKey = 0L;
    
    public DestroyMessage() {
    }
    
    public DestroyMessage(InternalCacheEvent event) {
      this.event = (EntryEventImpl) event; 
    }
    
    @Override
    protected boolean operateOnRegion(CacheEvent event, DistributionManager dm)
        throws EntryNotFoundException
    {
      EntryEventImpl ev = (EntryEventImpl)event;
      DistributedRegion rgn = (DistributedRegion)ev.region;

      try {
        if(!rgn.isCacheContentProxy()) {
          rgn.basicDestroy(ev,
                           false,
                           null); // expectedOldValue not supported on
                                  // non- partitioned regions
        }
        this.appliedOperation = true;
        
      } catch (ConcurrentCacheModificationException e) {
        dispatchElidedEvent(rgn, ev);
        return true;  // concurrent modifications are not reported to the sender
        
      } catch (EntryNotFoundException e) {
        dispatchElidedEvent(rgn, ev);
        if (!ev.isConcurrencyConflict()) {
          rgn.notifyGatewaySender(EnumListenerEvent.AFTER_DESTROY, ev);
        }
        throw e;
      }
      catch (CacheWriterException e) {
        throw new Error(LocalizedStrings.DestroyOperation_CACHEWRITER_SHOULD_NOT_BE_CALLED.toLocalizedString(), e);
      }
      catch (TimeoutException e) {
        throw new Error(LocalizedStrings.DestroyOperation_DISTRIBUTEDLOCK_SHOULD_NOT_BE_ACQUIRED.toLocalizedString(), e);
      }
      return true;
    }

    @Override
    @Retained
    protected final InternalCacheEvent createEvent(DistributedRegion rgn)
        throws EntryNotFoundException {
      EntryEventImpl ev = createEntryEvent(rgn);
      boolean evReturned = false;
      try {
      ev.setEventId(this.eventId);
      ev.setOldValueFromRegion();
      ev.setVersionTag(this.versionTag);
      if (this.filterRouting != null) {
        ev.setLocalFilterInfo(this.filterRouting
            .getFilterInfo(rgn.getMyId()));
      }
      ev.setTailKey(tailKey);
      ev.setInhibitAllNotifications(this.inhibitAllNotifications);
      evReturned = true;
      return ev;
      } finally {
        if (!evReturned) {
          ev.release();
        }
      }
    }

    @Retained
    EntryEventImpl createEntryEvent(DistributedRegion rgn)
    {
      @Retained EntryEventImpl event = EntryEventImpl.create(rgn,
          getOperation(), this.key, null, this.callbackArg, true, getSender());
//      event.setNewEventId(); Don't set the event here...
      setOldValueInEvent(event);
      event.setTailKey(this.tailKey);
      return event;
    }

    @Override
    protected void appendFields(StringBuilder buff)
    {
      super.appendFields(buff);
      buff.append(" key=")
          .append(this.key)
          .append(" id=")
          .append(this.eventId);
    }

    public int getDSFID() {
      return DESTROY_MESSAGE;
    }
    
    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      this.eventId = (EventID)DataSerializer.readObject(in);
      this.key = DataSerializer.readObject(in);
      Boolean hasTailKey = DataSerializer.readBoolean(in);
      if(hasTailKey.booleanValue()){
        this.tailKey = DataSerializer.readLong(in);
      }
    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      DataSerializer.writeObject(this.eventId, out);
      DataSerializer.writeObject(this.key, out);
      
      DistributedRegion region = (DistributedRegion)this.event.getRegion();
      if (region instanceof BucketRegion) {
        PartitionedRegion pr = region.getPartitionedRegion();
        if (pr.isParallelWanEnabled()) {
          DataSerializer.writeBoolean(Boolean.TRUE, out);
          DataSerializer.writeLong(this.event.getTailKey(), out);
        }else {
          DataSerializer.writeBoolean(Boolean.FALSE, out);
        }
      }
      else if(((LocalRegion)region).isUsedForSerialGatewaySenderQueue()){
        DataSerializer.writeBoolean(Boolean.TRUE, out);
        DataSerializer.writeLong(this.event.getTailKey(), out);
      }
      else{
        DataSerializer.writeBoolean(Boolean.FALSE, out);
      }
    }

    @Override
    public EventID getEventID() {
      return this.eventId;
    }

    @Override
    public List getOperations() {
      return Collections.singletonList(new QueuedOperation(getOperation(),
          this.key, null, null, DistributedCacheOperation
              .DESERIALIZATION_POLICY_NONE, this.callbackArg));
    }

    @Override
    public ConflationKey getConflationKey()
    {
      if (!super.regionAllowsConflation || getProcessorId() != 0) {
        // if the publisher's region attributes do not support conflation
        // or if it is an ack region
        // then don't even bother with a conflation key
        return null;
      }
      else {
        // don't conflate destroys
        return new ConflationKey(this.key, super.regionPath, false);
      }
    }
    @Override
    protected boolean mayAddToMultipleSerialGateways(DistributionManager dm) {
      return _mayAddToMultipleSerialGateways(dm);
    }
  }

  public static final class DestroyWithContextMessage extends DestroyMessage
  {
    transient ClientProxyMembershipID context;

    public DestroyWithContextMessage() {
    }
    
    public DestroyWithContextMessage(InternalCacheEvent event) {
      super(event);
    }
    
    @Override
    @Retained
    EntryEventImpl createEntryEvent(DistributedRegion rgn)
    {
      EntryEventImpl event = EntryEventImpl.create(rgn, getOperation(), 
          this.key, null, /* newvalue */
          this.callbackArg, true /* originRemote */, getSender(),
          true/* generateCallbacks */
      );
      event.setContext(this.context);
      return event;
    }

    @Override
    protected void appendFields(StringBuilder buff)
    {
      super.appendFields(buff);
      buff.append("; membershipID=");
      buff.append(this.context == null ? "" : this.context.toString());
    }

    @Override
    public int getDSFID() {
      return DESTROY_WITH_CONTEXT_MESSAGE;
    }
    
    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      this.context = ClientProxyMembershipID.readCanonicalized(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      DataSerializer.writeObject(this.context, out);
    }

  }

}
