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
package org.apache.geode.internal.cache.wan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;

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