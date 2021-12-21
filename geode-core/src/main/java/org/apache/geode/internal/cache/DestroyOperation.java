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

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.ConflationKey;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Handles distribution messaging for destroying an entry in a region.
 */
public class DestroyOperation extends DistributedCacheOperation {

  /** Creates a new instance of DestroyOperation */
  public DestroyOperation(EntryEventImpl event) {
    super(event);
  }

  @Override
  protected CacheOperationMessage createMessage() {
    if (event.hasClientOrigin()) {
      DestroyWithContextMessage msgwithContxt = new DestroyWithContextMessage(event);
      msgwithContxt.context = event.getContext();
      return msgwithContxt;
    } else {
      return new DestroyMessage(event);
    }
  }

  @Override
  protected void initMessage(CacheOperationMessage msg, DirectReplyProcessor processor) {
    super.initMessage(msg, processor);
    DestroyMessage m = (DestroyMessage) msg;
    EntryEventImpl event = getEvent();
    m.key = event.getKey();
    m.eventId = event.getEventId();
  }

  public static class DestroyMessage extends CacheOperationMessage {
    protected EventID eventId = null;

    protected Object key;

    protected EntryEventImpl event = null;

    private Long tailKey = 0L;

    public DestroyMessage() {}

    public DestroyMessage(InternalCacheEvent event) {
      this.event = (EntryEventImpl) event;
    }

    @Override
    protected boolean operateOnRegion(CacheEvent event, ClusterDistributionManager dm)
        throws EntryNotFoundException {
      EntryEventImpl ev = (EntryEventImpl) event;
      DistributedRegion rgn = (DistributedRegion) ev.getRegion();

      try {
        if (!rgn.isCacheContentProxy()) {
          rgn.basicDestroy(ev, false, null); // expectedOldValue not supported on
                                             // non- partitioned regions
        }
        appliedOperation = true;

      } catch (ConcurrentCacheModificationException e) {
        dispatchElidedEvent(rgn, ev);
        return true; // concurrent modifications are not reported to the sender

      } catch (EntryNotFoundException e) {
        dispatchElidedEvent(rgn, ev);
        if (!ev.isConcurrencyConflict()) {
          rgn.notifyGatewaySender(EnumListenerEvent.AFTER_DESTROY, ev);
        }
        throw e;
      } catch (CacheWriterException e) {
        throw new Error(
            "CacheWriter should not be called",
            e);
      } catch (TimeoutException e) {
        throw new Error("DistributedLock should not be acquired", e);
      }
      return true;
    }

    @Override
    @Retained
    protected InternalCacheEvent createEvent(DistributedRegion rgn) throws EntryNotFoundException {
      EntryEventImpl ev = createEntryEvent(rgn);
      boolean evReturned = false;
      try {
        ev.setEventId(eventId);
        ev.setOldValueFromRegion();
        ev.setVersionTag(versionTag);
        if (filterRouting != null) {
          ev.setLocalFilterInfo(filterRouting.getFilterInfo(rgn.getMyId()));
        }
        ev.setTailKey(tailKey);
        ev.setInhibitAllNotifications(inhibitAllNotifications);
        evReturned = true;
        return ev;
      } finally {
        if (!evReturned) {
          ev.release();
        }
      }
    }

    @Retained
    EntryEventImpl createEntryEvent(DistributedRegion rgn) {
      @Retained
      EntryEventImpl event = EntryEventImpl.create(rgn, getOperation(), key, null,
          callbackArg, true, getSender());
      // event.setNewEventId(); Don't set the event here...
      setOldValueInEvent(event);
      event.setTailKey(tailKey);
      return event;
    }

    @Override
    protected void appendFields(StringBuilder buff) {
      super.appendFields(buff);
      buff.append(" key=").append(key).append(" id=").append(eventId);
    }

    @Override
    public int getDSFID() {
      return DESTROY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      eventId = DataSerializer.readObject(in);
      key = DataSerializer.readObject(in);
      Boolean hasTailKey = DataSerializer.readBoolean(in);
      if (hasTailKey.booleanValue()) {
        tailKey = DataSerializer.readLong(in);
      }
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeObject(eventId, out);
      DataSerializer.writeObject(key, out);

      DistributedRegion region = (DistributedRegion) event.getRegion();
      if (region instanceof BucketRegion) {
        PartitionedRegion pr = region.getPartitionedRegion();
        if (pr.isParallelWanEnabled()) {
          DataSerializer.writeBoolean(Boolean.TRUE, out);
          DataSerializer.writeLong(event.getTailKey(), out);
        } else {
          DataSerializer.writeBoolean(Boolean.FALSE, out);
        }
      } else if (region.isUsedForSerialGatewaySenderQueue()) {
        DataSerializer.writeBoolean(Boolean.TRUE, out);
        DataSerializer.writeLong(event.getTailKey(), out);
      } else {
        DataSerializer.writeBoolean(Boolean.FALSE, out);
      }
    }

    @Override
    public EventID getEventID() {
      return eventId;
    }

    @Override
    public ConflationKey getConflationKey() {
      if (!super.regionAllowsConflation || getProcessorId() != 0) {
        // if the publisher's region attributes do not support conflation
        // or if it is an ack region
        // then don't even bother with a conflation key
        return null;
      } else {
        // don't conflate destroys
        return new ConflationKey(key, super.regionPath, false);
      }
    }

    @Override
    protected boolean mayNotifySerialGatewaySender(ClusterDistributionManager dm) {
      return notifiesSerialGatewaySender(dm);
    }
  }

  public static class DestroyWithContextMessage extends DestroyMessage {
    transient ClientProxyMembershipID context;

    public DestroyWithContextMessage() {}

    public DestroyWithContextMessage(InternalCacheEvent event) {
      super(event);
    }

    @Override
    @Retained
    EntryEventImpl createEntryEvent(DistributedRegion rgn) {
      EntryEventImpl event =
          EntryEventImpl.create(rgn, getOperation(), key, null, /* newvalue */
              callbackArg, true /* originRemote */, getSender(), true/* generateCallbacks */
          );
      event.setContext(context);
      return event;
    }

    @Override
    protected void appendFields(StringBuilder buff) {
      super.appendFields(buff);
      buff.append("; membershipID=");
      buff.append(context == null ? "" : context.toString());
    }

    @Override
    public int getDSFID() {
      return DESTROY_WITH_CONTEXT_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.context = ClientProxyMembershipID.readCanonicalized(in);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeObject(this.context, out);
    }

  }

}
