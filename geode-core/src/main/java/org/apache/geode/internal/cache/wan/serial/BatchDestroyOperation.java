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
package org.apache.geode.internal.cache.wan.serial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.ConflationKey;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.internal.cache.DistributedCacheOperation;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCacheEvent;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Handles distribution messaging for destroying a batch of entry in a queue region. In this message
 * key represents the lastDestroyedKey and tailKey represent the last dispatched key.
 *
 * We iterate from key to tailKey and destroy all the keys.
 *
 *
 */
public class BatchDestroyOperation extends DistributedCacheOperation {

  private static final Logger logger = LogService.getLogger();

  /** Creates a new instance of DestroyOperation */
  public BatchDestroyOperation(EntryEventImpl event) {
    super(event);
  }

  @Override
  protected CacheOperationMessage createMessage() {
    return new DestroyMessage(event);
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

      final boolean isDebugEnabled = logger.isDebugEnabled();
      try {
        if (isDebugEnabled) {
          logger.debug(
              "Received batch destroyed message with key {} tail key {} this size of the region is {} they keys are {}",
              key, tailKey, rgn.size(), rgn.keys());
        }

        // Optimized way
        for (long k = (Long) this.key; k <= this.tailKey && this.tailKey != -1; k++) {
          try {
            for (GatewayEventFilter filter : rgn.getSerialGatewaySender()
                .getGatewayEventFilters()) {
              GatewayQueueEvent eventForFilter = (GatewayQueueEvent) rgn.get(k);
              try {
                if (eventForFilter != null) {
                  filter.afterAcknowledgement(eventForFilter);
                }
              } catch (Exception e) {
                logger.fatal(String.format(
                    "Exception occurred while handling call to %s.afterAcknowledgement for event %s:",
                    new Object[] {filter.toString(), eventForFilter}),
                    e);
              }
            }
            rgn.localDestroy(k, RegionQueue.WAN_QUEUE_TOKEN);
          } catch (EntryNotFoundException e) {
            if (isDebugEnabled) {
              logger.debug("For key {} there is no entry in the region.", k);
            }
          }
        }

        // destroy dropped event from unprocessedKeys
        if (this.tailKey == -1) {
          SerialGatewaySenderEventProcessor ep = null;
          int index = ((Long) this.key).intValue();
          if (index == -1) {
            // this is SerialGatewaySenderEventProcessor
            ep = (SerialGatewaySenderEventProcessor) rgn.getSerialGatewaySender()
                .getEventProcessor();
          } else {
            ConcurrentSerialGatewaySenderEventProcessor csgep =
                (ConcurrentSerialGatewaySenderEventProcessor) rgn.getSerialGatewaySender()
                    .getEventProcessor();
            if (csgep != null) {
              ep = csgep.processors.get(index);
            }
          }
          if (ep != null) {
            // if sender is being shutdown, the ep could be null
            boolean removed = ep.basicHandlePrimaryDestroy(ev.getEventId());
            if (removed) {
              if (isDebugEnabled) {
                logger.debug("Removed a dropped event {} from unprocessedEvents.",
                    (EntryEventImpl) event);
              }
            }
          }
        }
        this.appliedOperation = true;
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
        ev.setEventId(this.eventId);
        ev.setOldValueFromRegion();
        if (this.filterRouting != null) {
          ev.setLocalFilterInfo(this.filterRouting.getFilterInfo(rgn.getCache().getMyId()));
        }
        ev.setTailKey(tailKey);
        evReturned = true;
        return ev;
      } finally {
        if (!evReturned)
          ev.release();
      }
    }

    @Retained
    EntryEventImpl createEntryEvent(DistributedRegion rgn) {
      @Retained
      EntryEventImpl event = EntryEventImpl.create(rgn, getOperation(), this.key, null,
          this.callbackArg, true, getSender());
      // event.setNewEventId(); Don't set the event here...
      setOldValueInEvent(event);
      event.setTailKey(this.tailKey);
      return event;
    }

    @Override
    protected void appendFields(StringBuilder buff) {
      super.appendFields(buff);
      buff.append(" lastDestroydKey=").append(this.key).append(" lastDispatchedKey=")
          .append(this.tailKey).append(" id=").append(this.eventId);
    }

    @Override
    public int getDSFID() {
      return BATCH_DESTROY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.eventId = (EventID) DataSerializer.readObject(in);
      this.key = DataSerializer.readObject(in);
      Boolean hasTailKey = DataSerializer.readBoolean(in);
      if (hasTailKey.booleanValue()) {
        this.tailKey = DataSerializer.readLong(in);
      }
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeObject(this.eventId, out);
      DataSerializer.writeObject(this.key, out);
      DataSerializer.writeBoolean(Boolean.TRUE, out);
      DataSerializer.writeLong(this.event.getTailKey(), out);
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
        return new ConflationKey(this.key, super.regionPath, false);
      }
    }
  }
}
