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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheEvent;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.TransactionEvent;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;

/**
 * <p>
 * The internal implementation of the {@link TransactionEvent}interface used by
 * the remote commit code.
 * 
 * 
 * @since GemFire 4.0
 *  
 */
public class TXRmtEvent implements TransactionEvent
{
  private final TransactionId txId;

  private Cache cache;

  // This list of EntryEventImpls are released by calling freeOffHeapResources
  @Released
  private List events;
  
  TXRmtEvent(TransactionId txId, Cache cache) {
    this.txId = txId;
    this.cache = cache;
    this.events = null;
  }
  
  public TransactionId getTransactionId()
  {
    return this.txId;
  }

  private boolean isEventUserVisible(CacheEvent ce) {
    return BucketRegion.FORCE_LOCAL_LISTENERS_INVOCATION || !(ce.getRegion() instanceof PartitionedRegion);  
  }
  
  public List getEvents()
  {
    if (this.events == null) {
      return Collections.EMPTY_LIST;
    }
    else {
      ArrayList result = new ArrayList(this.events.size());
      Iterator it = this.events.iterator();
      while (it.hasNext()) {
        CacheEvent ce = (CacheEvent)it.next();
        if (isEventUserVisible(ce)) {
          result.add(ce);
        }
      }
      if (result.isEmpty()) {
        return Collections.EMPTY_LIST;
      }
      else {
        return Collections.unmodifiableList(result);
      }
    }
  }
  
  /**
   * Do all operations touch internal regions?
   * Returns false if the transaction is empty
   * or if any events touch non-internal regions.
   */
  public boolean hasOnlyInternalEvents() {
    if (events == null || events.isEmpty()) {
      return false;
    }
    Iterator<CacheEvent<?,?>> it = this.events.iterator();
    while (it.hasNext()) {
      CacheEvent<?,?> event = it.next();
      if (isEventUserVisible(event)) {
        LocalRegion region = (LocalRegion)event.getRegion();
        if (region != null
            && !region.isPdxTypesRegion()
            && !region.isInternalRegion()) {
          return false;
        }
      }
    }
    return true;
  }

  public List getCreateEvents()
  {
    if (this.events == null) {
      return Collections.EMPTY_LIST;
    }
    else {
      ArrayList result = new ArrayList(this.events.size());
      Iterator it = this.events.iterator();
      while (it.hasNext()) {
        CacheEvent ce = (CacheEvent)it.next();
        if (ce.getOperation().isCreate() && isEventUserVisible(ce)) {
          result.add(ce);
        }
      }
      if (result.isEmpty()) {
        return Collections.EMPTY_LIST;
      }
      else {
        return Collections.unmodifiableList(result);
      }
    }
  }

  public List getPutEvents()
  {
    if (this.events == null) {
      return Collections.EMPTY_LIST;
    }
    else {
      ArrayList result = new ArrayList(this.events.size());
      Iterator it = this.events.iterator();
      while (it.hasNext()) {
        CacheEvent ce = (CacheEvent)it.next();
        if (ce.getOperation().isUpdate() && isEventUserVisible(ce)) {
          result.add(ce);
        }
      }
      if (result.isEmpty()) {
        return Collections.EMPTY_LIST;
      }
      else {
        return Collections.unmodifiableList(result);
      }
    }
  }

  public List getInvalidateEvents()
  {
    if (this.events == null) {
      return Collections.EMPTY_LIST;
    }
    else {
      ArrayList result = new ArrayList(this.events.size());
      Iterator it = this.events.iterator();
      while (it.hasNext()) {
        CacheEvent ce = (CacheEvent)it.next();
        if (ce.getOperation().isInvalidate() && isEventUserVisible(ce)) {
          result.add(ce);
        }
      }
      if (result.isEmpty()) {
        return Collections.EMPTY_LIST;
      }
      else {
        return Collections.unmodifiableList(result);
      }
    }
  }

  public List getDestroyEvents()
  {
    if (this.events == null) {
      return Collections.EMPTY_LIST;
    }
    else {
      ArrayList result = new ArrayList(this.events.size());
      Iterator it = this.events.iterator();
      while (it.hasNext()) {
        CacheEvent ce = (CacheEvent)it.next();
        if (ce.getOperation().isDestroy() && isEventUserVisible(ce)) {
          result.add(ce);
        }
      }
      if (result.isEmpty()) {
        return Collections.EMPTY_LIST;
      }
      else {
        return Collections.unmodifiableList(result);
      }
    }
  }
  
  public boolean isEmpty() {
    return (events == null) || events.isEmpty();
  }

  @Retained
  private EntryEventImpl createEvent(LocalRegion r, Operation op,
      RegionEntry re, Object key, Object newValue,Object aCallbackArgument)
  {
    DistributedMember originator = ((TXId)this.txId).getMemberId();
    //TODO:ASIF :EventID will not be generated with this constructor . Check if
    // this is correct
    LocalRegion eventRegion = r;
    if (r.isUsedForPartitionedRegionBucket()) {
      eventRegion = r.getPartitionedRegion();
    }
    @Retained EntryEventImpl event = EntryEventImpl.create(
        eventRegion, op, key, newValue,
        aCallbackArgument, // callbackArg
        true, // originRemote
        originator);
    event.setOldValue(re.getValueInVM(r)); // OFFHEAP: copy into heap cd
    event.setTransactionId(getTransactionId());
    return event;
  }

  /**
   * Add an event to our internal list
   */
  private void addEvent(EntryEventImpl e)
  {
    synchronized (this) {
      if (this.events == null) {
        this.events = new ArrayList();
      }
      this.events.add(e);
    }
  }

  void addDestroy(LocalRegion r, RegionEntry re, Object key,Object aCallbackArgument)
  {
    addEvent(createEvent(r, Operation.DESTROY, re, key, null,aCallbackArgument));
  }

  void addInvalidate(LocalRegion r, RegionEntry re, Object key, Object newValue,Object aCallbackArgument)
  {
    addEvent(createEvent(r, Operation.INVALIDATE, re, key, newValue,aCallbackArgument));
  }

  void addPut(Operation putOp, LocalRegion r, RegionEntry re, Object key,
      Object newValue,Object aCallbackArgument)
  {
    addEvent(createEvent(r, putOp, re, key, newValue,aCallbackArgument));
  }

  public Cache getCache()
  {
    return this.cache;
  }

  public void freeOffHeapResources() {
    if (this.events != null) {
      for (EntryEventImpl e: (List<EntryEventImpl>)this.events) {
        e.release();
      }
    }
  }
}
