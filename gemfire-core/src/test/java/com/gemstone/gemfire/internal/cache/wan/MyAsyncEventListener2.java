/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;

public class MyAsyncEventListener2 implements AsyncEventListener {

  private Map<Integer, List<GatewaySenderEventImpl>> bucketToEventsMap;

  public MyAsyncEventListener2() {
    this.bucketToEventsMap = new HashMap<Integer, List<GatewaySenderEventImpl>>();
  }

  public boolean processEvents(List<AsyncEvent> events) {
    for (AsyncEvent event : events) {
      GatewaySenderEventImpl gatewayEvent = (GatewaySenderEventImpl)event;
      int bucketId = gatewayEvent.getBucketId();
      List<GatewaySenderEventImpl> bucketEvents = this.bucketToEventsMap
          .get(bucketId);
      if (bucketEvents == null) {
        bucketEvents = new ArrayList<GatewaySenderEventImpl>();
        bucketEvents.add(gatewayEvent);
        this.bucketToEventsMap.put(bucketId, bucketEvents);
      }
      else {
        bucketEvents.add(gatewayEvent);
      }
    }
    return true;
  }

  public Map<Integer, List<GatewaySenderEventImpl>> getBucketToEventsMap() {
    return bucketToEventsMap;
  }

  public void close() {
  }
  
//  protected void addExceptionTag(final String expectedException)
//  {
//        
//          SerializableRunnable addExceptionTag = new CacheSerializableRunnable("addExceptionTag")
//          {
//                 public void run2()
//                 {
//                         getCache().getLogger().info("<ExpectedException action=add>" + 
//                                                expectedException + "</ExpectedException>"); 
//                 }
//          };
//          
//         vm2.invoke(addExceptionTag);
//         vm3.invoke(addExceptionTag);
//         vm4.invoke(addExceptionTag);
//         vm5.invoke(addExceptionTag);
//         vm6.invoke(addExceptionTag);
//         vm7.invoke(addExceptionTag);
//  }
// 
//  protected void removeExceptionTag(final String expectedException)
//  {     
//        
//          SerializableRunnable removeExceptionTag = new CacheSerializableRunnable("removeExceptionTag")
//          {
//                public void run2() throws CacheException
//                {
//                          getCache().getLogger().info("<ExpectedException action=remove>" + 
//                                                 expectedException + "</ExpectedException>");   
//                }
//          };
//          vm2.invoke(removeExceptionTag);
//          vm3.invoke(removeExceptionTag);
//          vm4.invoke(removeExceptionTag);
//          vm5.invoke(removeExceptionTag);
//          vm6.invoke(removeExceptionTag);
//          vm7.invoke(removeExceptionTag);
//  }
}