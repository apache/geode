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

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.ConflationKey;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Handles distribution messaging for invalidating an entry in a region.
 *
 *
 */
public class InvalidateOperation extends DistributedCacheOperation {
  private static final Logger logger = LogService.getLogger();

  /** Creates a new instance of InvalidateOperation */
  public InvalidateOperation(EntryEventImpl event) {
    super(event);
  }

  @Override
  protected CacheOperationMessage createMessage() {
    if (event.hasClientOrigin()) {
      InvalidateWithContextMessage msgwithContxt = new InvalidateWithContextMessage();
      msgwithContxt.context = event.getContext();
      return msgwithContxt;
    } else {
      return new InvalidateMessage();
    }
  }

  @Override
  protected void initMessage(CacheOperationMessage msg, DirectReplyProcessor processor) {
    super.initMessage(msg, processor);
    InvalidateMessage imsg = (InvalidateMessage) msg;
    EntryEventImpl eei = (EntryEventImpl) event;
    imsg.key = eei.getKey();
    imsg.eventId = eei.getEventId();
  }

  public static class InvalidateMessage extends CacheOperationMessage {
    protected Object key;
    protected EventID eventId = null;

    @Override
    protected boolean operateOnRegion(CacheEvent event, ClusterDistributionManager dm)
        throws EntryNotFoundException {
      EntryEventImpl ev = (EntryEventImpl) event;
      DistributedRegion rgn = (DistributedRegion) ev.getRegion();

      try {
        if (!rgn.isCacheContentProxy()) {
          if (logger.isTraceEnabled()) {
            logger.trace("InvalidateMessage.operationOnRegion; key={}", ev.getKey());
          }

          // if this is a mirrored region and we're still initializing, or
          // concurrency conflict detection is enabled (requiring version #
          // retention) then force new entry creation
          boolean forceNewEntry = rgn.getDataPolicy().withReplication()
              && (!rgn.isInitialized() || rgn.getConcurrencyChecksEnabled());
          boolean invokeCallbacks = rgn.isInitialized();
          rgn.basicInvalidate(ev, invokeCallbacks, forceNewEntry);
        }
        appliedOperation = true;
        return true;
      } catch (ConcurrentCacheModificationException e) {
        dispatchElidedEvent(rgn, ev);
        return true; // concurrent modification problems are not reported to senders
      }
    }

    @Override
    @Retained
    protected InternalCacheEvent createEvent(DistributedRegion rgn) throws EntryNotFoundException {
      @Retained
      EntryEventImpl ev = EntryEventImpl.create(rgn, getOperation(), key, null,
          callbackArg, true, getSender());
      ev.setEventId(eventId);
      setOldValueInEvent(ev);
      ev.setVersionTag(versionTag);
      if (filterRouting != null) {
        ev.setLocalFilterInfo(filterRouting.getFilterInfo(rgn.getMyId()));
      }
      ev.setInhibitAllNotifications(inhibitAllNotifications);
      return ev;
    }

    @Override
    protected void appendFields(StringBuilder buff) {
      super.appendFields(buff);
      buff.append("; key=");
      buff.append(key);
    }

    @Override
    public int getDSFID() {
      return INVALIDATE_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      eventId = DataSerializer.readObject(in);
      key = DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeObject(eventId, out);
      DataSerializer.writeObject(key, out);
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
        // don't conflate invalidates
        return new ConflationKey(key, super.regionPath, false);
      }
    }
  }

  public static class InvalidateWithContextMessage extends InvalidateMessage {
    transient ClientProxyMembershipID context;

    @Override
    @Retained
    protected InternalCacheEvent createEvent(DistributedRegion rgn) throws EntryNotFoundException {
      EntryEventImpl event = (EntryEventImpl) super.createEvent(rgn);
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
      return INVALIDATE_WITH_CONTEXT_MESSAGE;
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
