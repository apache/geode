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

package org.apache.geode.internal.cache;
import org.apache.geode.cache.*;
import java.util.*;
import org.apache.geode.internal.offheap.Releasable;

/** <p>The internal implementation of the {@link TransactionEvent} interface
 * 
 *
 * @since GemFire 4.0
 * 
 */
public class TXEvent implements TransactionEvent, Releasable {
  private final TXStateInterface localTxState;
  private List events;
  private List createEvents = null;
  private List putEvents = null;
  private List invalidateEvents = null;
  private List destroyEvents = null;
  final private Cache cache;

  TXEvent(TXStateInterface localTxState, Cache cache) {
    this.localTxState = localTxState;
    this.cache = cache;
    this.events = null;
  }

  public TransactionId getTransactionId() {
    return this.localTxState.getTransactionId();
  }

  public synchronized List getCreateEvents() {
    if (this.createEvents == null) {
      ArrayList result = new ArrayList();
      Iterator it = getEvents().iterator();
      while (it.hasNext()) {
        CacheEvent ce = (CacheEvent)it.next();
        if (ce.getOperation().isCreate()) {
          result.add(ce);
        }
      }
      if (result.isEmpty()) {
        this.createEvents = Collections.EMPTY_LIST;
      } else {
        this.createEvents = Collections.unmodifiableList(result);
      }
    }
    return this.createEvents;
  }

  public synchronized List getPutEvents() {
    if (this.putEvents == null) {
      ArrayList result = new ArrayList();
      Iterator it = getEvents().iterator();
      while (it.hasNext()) {
        CacheEvent ce = (CacheEvent)it.next();
        if (ce.getOperation().isUpdate()) {
          result.add(ce);
        }
      }
      if (result.isEmpty()) {
        this.putEvents = Collections.EMPTY_LIST;
      } else {
        this.putEvents = Collections.unmodifiableList(result);
      }
    }
    return this.putEvents;
  }

  public synchronized List getInvalidateEvents() {
    if (this.invalidateEvents == null) {
      ArrayList result = new ArrayList();
      Iterator it = getEvents().iterator();
      while (it.hasNext()) {
        CacheEvent ce = (CacheEvent)it.next();
        if (ce.getOperation().isInvalidate()) {
          result.add(ce);
        }
      }
      if (result.isEmpty()) {
        this.invalidateEvents = Collections.EMPTY_LIST;
      } else {
        this.invalidateEvents = Collections.unmodifiableList(result);
      }
    }
    return this.invalidateEvents;
  }

  public synchronized List getDestroyEvents() {
    if (this.destroyEvents == null) {
      ArrayList result = new ArrayList();
      Iterator it = getEvents().iterator();
      while (it.hasNext()) {
        CacheEvent ce = (CacheEvent)it.next();
        if (ce.getOperation().isDestroy()) {
          result.add(ce);
        }
      }
      if (result.isEmpty()) {
        this.destroyEvents = Collections.EMPTY_LIST;
      } else {
        this.destroyEvents = Collections.unmodifiableList(result);
      }
    }
    return this.destroyEvents;
  }
  
  public synchronized List getEvents() {
    if (this.events == null) {
      this.events = this.localTxState.getEvents();
    }
    return this.events;
  }

  /**
   * Do all operations touch internal regions?
   * Returns false if the transaction is empty
   * or if any events touch non-internal regions.
   */
  public boolean hasOnlyInternalEvents() {
    List<CacheEvent<?,?>> txevents = getEvents();
    if (txevents == null || txevents.isEmpty()) {
      return false;
    }
    for (CacheEvent<?,?> txevent: txevents) {
      LocalRegion region = (LocalRegion)txevent.getRegion();
      if (region != null
          && !region.isPdxTypesRegion()
          && !region.isInternalRegion()) {
        return false;
      }
    }
    return true;
  }

  public final Cache getCache() {
    return this.cache;
  }

  @Override
  public synchronized void release() {
    if (this.events != null) {
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
