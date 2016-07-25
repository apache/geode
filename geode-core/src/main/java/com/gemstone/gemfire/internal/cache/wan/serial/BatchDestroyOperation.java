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
package com.gemstone.gemfire.internal.cache.wan.serial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheEvent;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayQueueEvent;
import com.gemstone.gemfire.distributed.internal.ConflationKey;
import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.cache.DistributedCacheOperation;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.InternalCacheEvent;
import com.gemstone.gemfire.internal.cache.QueuedOperation;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;

/**
 * Handles distribution messaging for destroying a batch of entry in a queue region.
 * In this message key represents the lastDestroyedKey
 * and tailKey represent the last dispatched key.
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
  protected void initMessage(CacheOperationMessage msg,
      DirectReplyProcessor processor) {
    super.initMessage(msg, processor);
    DestroyMessage m = (DestroyMessage)msg;
    EntryEventImpl event = getEvent();
    m.key = event.getKey();
    m.eventId = event.getEventId();
  }

  public static class DestroyMessage extends CacheOperationMessage {
    protected EventID eventId = null;

    protected Object key;

    protected EntryEventImpl event = null;

    private Long tailKey = 0L;

    public DestroyMessage() {
    }

    public DestroyMessage(InternalCacheEvent event) {
      this.event = (EntryEventImpl)event;
    }

    @Override
    protected boolean operateOnRegion(CacheEvent event, DistributionManager dm)
        throws EntryNotFoundException {
      EntryEventImpl ev = (EntryEventImpl)event;
      DistributedRegion rgn = (DistributedRegion)ev.getRegion();

      final boolean isDebugEnabled = logger.isDebugEnabled();
      try {
        if (isDebugEnabled) {
          logger.debug("Received batch destroyed message with key {} tail key {} this size of the region is {} they keys are {}",
              key, tailKey, rgn.size(), rgn.keys());
        }
        
        // Optimized way
        for (long k = (Long)this.key; k <= this.tailKey; k++) {
          try {
            for (GatewayEventFilter filter : rgn.getSerialGatewaySender()
                .getGatewayEventFilters()) {
              GatewayQueueEvent eventForFilter = (GatewayQueueEvent)rgn.get(k);
              try {
                if (eventForFilter != null) {
                  filter.afterAcknowledgement(eventForFilter);
                }
              }
              catch (Exception e) {
                logger
                    .fatal(
                        LocalizedMessage
                            .create(
                                LocalizedStrings.GatewayEventFilter_EXCEPTION_OCCURED_WHILE_HANDLING_CALL_TO_0_AFTER_ACKNOWLEDGEMENT_FOR_EVENT_1,
                                new Object[] { filter.toString(),
                                    eventForFilter }), e);
              }
            }
            rgn.localDestroy(k, RegionQueue.WAN_QUEUE_TOKEN);
          } catch (EntryNotFoundException e) {
            if (isDebugEnabled) {
              logger.debug("For key {} there is no entry in the region.", k);
            }
          }
        }
        // Non-optimized way
//        for (Long k : (Set<Long>)rgn.keys()) {
//          if (k > this.tailKey) {
//            continue;
//          }
//          rgn.localDestroy(k, RegionQueue.WAN_QUEUE_TOKEN);
//        }
        this.appliedOperation = true;
      } catch (CacheWriterException e) {
        throw new Error(
            LocalizedStrings.DestroyOperation_CACHEWRITER_SHOULD_NOT_BE_CALLED
                .toLocalizedString(),
            e);
      } catch (TimeoutException e) {
        throw new Error(
            LocalizedStrings.DestroyOperation_DISTRIBUTEDLOCK_SHOULD_NOT_BE_ACQUIRED
                .toLocalizedString(), e);
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
      if (this.filterRouting != null) {
        ev.setLocalFilterInfo(this.filterRouting.getFilterInfo(rgn.getCache()
            .getMyId()));
      }
      ev.setTailKey(tailKey);
      evReturned = true;
      return ev;
      } finally {
        if (!evReturned) ev.release();
      }
    }

    @Retained
    EntryEventImpl createEntryEvent(DistributedRegion rgn) {
      @Retained EntryEventImpl event = EntryEventImpl.create(rgn, getOperation(), this.key,
          null, this.callbackArg, true, getSender());
      // event.setNewEventId(); Don't set the event here...
      setOldValueInEvent(event);
      event.setTailKey(this.tailKey);
      return event;
    }

    @Override
    protected void appendFields(StringBuilder buff) {
      super.appendFields(buff);
      buff.append(" lastDestroydKey=").append(this.key)
          .append(" lastDispatchedKey=").append(this.tailKey).append(" id=")
          .append(this.eventId);
    }

    public int getDSFID() {
      return BATCH_DESTROY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
      this.eventId = (EventID)DataSerializer.readObject(in);
      this.key = DataSerializer.readObject(in);
      Boolean hasTailKey = DataSerializer.readBoolean(in);
      if (hasTailKey.booleanValue()) {
        this.tailKey = DataSerializer.readLong(in);
      }
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeObject(this.eventId, out);
      DataSerializer.writeObject(this.key, out);
      DataSerializer.writeBoolean(Boolean.TRUE, out);
      DataSerializer.writeLong(this.event.getTailKey(), out);
    }

    @Override
    public List getOperations() {
      return Collections.singletonList(new QueuedOperation(getOperation(),
          this.key, null, null,
          DistributedCacheOperation.DESERIALIZATION_POLICY_NONE,
          this.callbackArg));
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
