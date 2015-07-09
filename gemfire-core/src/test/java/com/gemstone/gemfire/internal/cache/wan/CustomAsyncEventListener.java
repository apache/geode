/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;

public class CustomAsyncEventListener implements AsyncEventListener {

  private Map<Long, AsyncEvent> eventsMap;
  private boolean exceptionThrown = false;

  public CustomAsyncEventListener() {
    this.eventsMap = new HashMap<Long, AsyncEvent>();
  }

  public boolean processEvents(List<AsyncEvent> events) {
    int i = 0;
    for (AsyncEvent event : events) {
      i++;
      if (!exceptionThrown && i == 40) {
        i = 0;
        exceptionThrown = true;
        throw new Error("TestError");
      }
      if (exceptionThrown) {
        eventsMap.put((Long) event.getKey(), event);
      }
    }
    return true;
  }

  public Map<Long, AsyncEvent> getEventsMap() {
    return eventsMap;
  }

  public void close() {
  }
}