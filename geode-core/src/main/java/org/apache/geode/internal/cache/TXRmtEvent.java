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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.TransactionEvent;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;

/**
 * <p>
 * The internal implementation of the {@link TransactionEvent}interface used by the remote commit
 * code.
 *
 *
 * @since GemFire 4.0
 *
 */
public class TXRmtEvent implements TransactionEvent {
  private final TransactionId txId;

  private final Cache cache;

  // This list of EntryEventImpls are released by calling freeOffHeapResources
  @Released
  private List events;

  TXRmtEvent(TransactionId txId, Cache cache) {
    this.txId = txId;
    this.cache = cache;
    events = null;
  }

  @Override
  public TransactionId getTransactionId() {
    return txId;
  }

  private boolean isEventUserVisible(CacheEvent ce) {
    return BucketRegion.FORCE_LOCAL_LISTENERS_INVOCATION
        || !(ce.getRegion() instanceof PartitionedRegion);
  }

  @Override
  public List getEvents() {
    if (events == null) {
      return Collections.EMPTY_LIST;
    } else {
      ArrayList result = new ArrayList(events.size());
      Iterator it = events.iterator();
      while (it.hasNext()) {
        CacheEvent ce = (CacheEvent) it.next();
        if (isEventUserVisible(ce)) {
          result.add(ce);
        }
      }
      if (result.isEmpty()) {
        return Collections.EMPTY_LIST;
      } else {
        return Collections.unmodifiableList(result);
      }
    }
  }

  /**
   * Do all operations touch internal regions? Returns false if the transaction is empty or if any
   * events touch non-internal regions.
   */
  public boolean hasOnlyInternalEvents() {
    if (events == null || events.isEmpty()) {
      return false;
    }
    Iterator<CacheEvent<?, ?>> it = events.iterator();
    while (it.hasNext()) {
      CacheEvent<?, ?> event = it.next();
      if (isEventUserVisible(event)) {
        LocalRegion region = (LocalRegion) event.getRegion();
        if (region != null && !region.isPdxTypesRegion() && !region.isInternalRegion()) {
          return false;
        }
      }
    }
    return true;
  }



  public boolean isEmpty() {
    return (events == null) || events.isEmpty();
  }

  @Retained
  private EntryEventImpl createEvent(InternalRegion r, Operation op, RegionEntry re, Object key,
      Object newValue, Object aCallbackArgument) {
    DistributedMember originator = ((TXId) txId).getMemberId();
    // TODO:ASIF :EventID will not be generated with this constructor . Check if
    // this is correct
    InternalRegion eventRegion = r;
    if (r.isUsedForPartitionedRegionBucket()) {
      eventRegion = r.getPartitionedRegion();
    }
    @Retained
    EntryEventImpl event = EntryEventImpl.create(eventRegion, op, key, newValue, aCallbackArgument, // callbackArg
        true, // originRemote
        originator);
    event.setOldValue(re.getValueInVM(r)); // OFFHEAP: copy into heap cd
    event.setTransactionId(getTransactionId());
    return event;
  }

  /**
   * Add an event to our internal list
   */
  private void addEvent(EntryEventImpl e) {
    synchronized (this) {
      if (events == null) {
        events = new ArrayList();
      }
      events.add(e);
    }
  }

  public void addDestroy(InternalRegion r, RegionEntry re, Object key, Object aCallbackArgument) {
    addEvent(createEvent(r, Operation.DESTROY, re, key, null, aCallbackArgument));
  }

  public void addInvalidate(InternalRegion r, RegionEntry re, Object key, Object newValue,
      Object aCallbackArgument) {
    addEvent(createEvent(r, Operation.INVALIDATE, re, key, newValue, aCallbackArgument));
  }

  public void addPut(Operation putOp, InternalRegion r, RegionEntry re, Object key, Object newValue,
      Object aCallbackArgument) {
    addEvent(createEvent(r, putOp, re, key, newValue, aCallbackArgument));
  }

  @Override
  public Cache getCache() {
    return cache;
  }

  public void freeOffHeapResources() {
    if (events != null) {
      for (EntryEventImpl e : (List<EntryEventImpl>) events) {
        e.release();
      }
    }
  }
}
