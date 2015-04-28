/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache;
import com.gemstone.gemfire.cache.*;
import java.util.*;

/** <p>The internal implementation of the {@link TransactionEvent} interface
 * 
 * @author Darrel Schneider
 *
 * @since 4.0
 * 
 */
public class TXEvent implements TransactionEvent {
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

  public final Cache getCache() {
    return this.cache;
  }
}
