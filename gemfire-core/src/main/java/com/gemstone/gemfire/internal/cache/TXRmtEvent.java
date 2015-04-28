/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
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

/**
 * <p>
 * The internal implementation of the {@link TransactionEvent}interface used by
 * the remote commit code.
 * 
 * @author Darrel Schneider
 * 
 * @since 4.0
 *  
 */
public class TXRmtEvent implements TransactionEvent
{
  private final TransactionId txId;

  private Cache cache;

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
    EntryEventImpl event = new EntryEventImpl(
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
}
