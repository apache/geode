/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

public class MyAsyncEventListener implements AsyncEventListener {

  private final Map eventsMap;

  public MyAsyncEventListener() {
    this.eventsMap = new ConcurrentHashMap();
  }

  public boolean processEvents(List<AsyncEvent> events) {
    for (AsyncEvent event : events) {
      if(this.eventsMap.containsKey(event.getKey())) {
        InternalDistributedSystem.getConnectedInstance().getLogWriter().fine("This is a duplicate event --> " + event.getKey());
      }
      this.eventsMap.put(event.getKey(), event.getDeserializedValue());
      
      InternalDistributedSystem.getConnectedInstance().getLogWriter().fine("Received an event --> " + event.getKey());
    }
    return true;
  }

  public Map getEventsMap() {
    return eventsMap;
  }

  public void close() {
  }
}