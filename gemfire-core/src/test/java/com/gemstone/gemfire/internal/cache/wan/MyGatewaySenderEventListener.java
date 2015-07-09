/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;

public class MyGatewaySenderEventListener implements
    AsyncEventListener, Serializable {
  String id = "MyGatewaySenderEventListener";
  /**
   * Creates a latency listener.
   */
  private final Map eventsMap;

  public MyGatewaySenderEventListener() {
    this.eventsMap = new HashMap();
  }

  /**
   * Processes events by recording their latencies.
   */
  public boolean processEvents(List<AsyncEvent> events) {
	  System.out.println("hitesh got event");
    synchronized (eventsMap) {		  
   	 for (AsyncEvent event : events) {
      	    this.eventsMap.put(event.getKey(), event.getDeserializedValue());
        }
    }
    return true;
  }

  public void close() {
  }

  public Map getEventsMap() {
    return this.eventsMap;
  }

  public void printMap() {
    System.out.println("Printing Map " + this.eventsMap);
  }
  
  @Override
  public boolean equals(Object obj){
    if(this == obj){
      return true;
    }
    if ( !(obj instanceof MyGatewaySenderEventListener) ) return false;
    MyGatewaySenderEventListener listener = (MyGatewaySenderEventListener)obj;
    return this.id.equals(listener.id);
  }
  
  @Override
  public String toString(){
    return id;
  }
}
