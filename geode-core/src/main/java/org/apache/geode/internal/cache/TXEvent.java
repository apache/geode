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

import java.util.Iterator;
import java.util.List;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.TransactionEvent;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.internal.offheap.Releasable;

/**
 * The internal implementation of the {@link TransactionEvent} interface
 *
 * @since GemFire 4.0
 */
public class TXEvent implements TransactionEvent, Releasable {

  private final TXStateInterface localTxState;
  private List events;
  private final Cache cache;

  TXEvent(TXStateInterface localTxState, Cache cache) {
    this.localTxState = localTxState;
    this.cache = cache;
    events = null;
  }

  @Override
  public TransactionId getTransactionId() {
    return localTxState.getTransactionId();
  }


  @Override
  public synchronized List getEvents() {
    if (events == null) {
      events = localTxState.getEvents();
    }
    return events;
  }

  /**
   * Do all operations touch internal regions? Returns false if the transaction is empty or if any
   * events touch non-internal regions.
   */
  public boolean hasOnlyInternalEvents() {
    List<CacheEvent<?, ?>> txevents = getEvents();
    if (txevents == null || txevents.isEmpty()) {
      return false;
    }
    for (CacheEvent<?, ?> txevent : txevents) {
      LocalRegion region = (LocalRegion) txevent.getRegion();
      if (region != null && !region.isPdxTypesRegion() && !region.isInternalRegion()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Cache getCache() {
    return cache;
  }

  @Override
  public synchronized void release() {
    if (events != null) {
      Iterator it = getEvents().iterator();
      while (it.hasNext()) {
        Object o = it.next();
        if (o instanceof EntryEventImpl) {
          ((EntryEventImpl) o).release();
        }
      }
    }
  }
}
