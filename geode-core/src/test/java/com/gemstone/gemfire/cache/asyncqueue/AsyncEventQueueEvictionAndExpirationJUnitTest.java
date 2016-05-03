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
package com.gemstone.gemfire.cache.asyncqueue;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.jayway.awaitility.Awaitility;

import static org.mockito.Mockito.*;

@Category(IntegrationTest.class)
public class AsyncEventQueueEvictionAndExpirationJUnitTest {
  
  private AsyncEventQueue aeq;
  private Cache cache;
  
  @Rule 
  public TestName name = new TestName();
  
  @Before
  public void getCache() {
    try {
       cache = CacheFactory.getAnyInstance();
    } catch (Exception e) {
      //ignore
    }
    if (null == cache) {
      cache = (GemFireCacheImpl) new CacheFactory().set("mcast-port", "0").create();
    }
  }

  @After
  public void destroyCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache = null;
    }
  }

  
  @Test
  public void isIgnoreEvictionAndExpirationAttributeTrueByDefault() {
    AsyncEventListener al = mock(AsyncEventListener.class);
    aeq = cache.createAsyncEventQueueFactory().create("aeq", al);
    // Test for default value of isIgnoreEvictionAndExpiration setting.
    assertTrue(aeq.isIgnoreEvictionAndExpiration());
  }
  
  @Test
  public void canSetFalseForIgnoreEvictionAndExpiration() {
    AsyncEventListener al = mock(AsyncEventListener.class);
    aeq = cache.createAsyncEventQueueFactory().setIgnoreEvictionAndExpiration(false).create("aeq", al);
    // Test for default value of isIgnoreEvictionAndExpiration setting.
    assertFalse(aeq.isIgnoreEvictionAndExpiration());
  }
  
  
  @Test
  public void evictionDestroyOpEventsNotPropogatedByDefault() {
    // For Replicated Region with eviction-destroy op.
    // Number of expected events 2. Two for create and none for eviction destroy.
    createPopulateAndVerifyEvents(false /*isPR */, true /* ignoreEvictionExpiration */, 
        2 /* expectedEvents */ , true /* eviction */, false /* evictionOverflow */, 
        false /* expirationDestroy */, false /* expirationInvalidate */, 
        false /* checkForDestroyOp */, false /* checkForInvalidateOp */);
  }

  @Test
  public void evictionDestroyOpEventsNotPropogatedByDefaultForPR() {
    // For PR with eviction-destroy op.
    // Number of expected events 2. Two for create and none for eviction destroy.
    createPopulateAndVerifyEvents(true /*isPR */, true /* ignoreEvictionExpiration */, 
        2 /* expectedEvents */ , true /* eviction */, false /* evictionOverflow */, 
        false /* expirationDestroy */, false /* expirationInvalidate */, 
        false /* checkForDestroyOp */, false /* checkForInvalidateOp */);
  }

  @Test
  public void expirationDestroyOpEventsNotPropogatedByDefault() {
    // For Replicated Region with expiration-destroy op.
    // Number of expected events 2. Two for create and none for eviction destroy.
    createPopulateAndVerifyEvents(false /*isPR */, true /* ignoreEvictionExpiration */, 
        2 /* expectedEvents */ , false /* eviction */, false /* evictionOverflow */, 
        true /* expirationDestroy */, false /* expirationInvalidate */, 
        false /* checkForDestroyOp */, false /* checkForInvalidateOp */);
  }

  @Test
  public void expirationDestroyOpEventsNotPropogatedByDefaultForPR() {
    // For PR with expiration-destroy op.
    // Number of expected events 2. Two for create and none for eviction destroy.
    createPopulateAndVerifyEvents(true /*isPR */, true /* ignoreEvictionExpiration */, 
        2 /* expectedEvents */ , false /* eviction */, false /* evictionOverflow */, 
        true /* expirationDestroy */, false /* expirationInvalidate */, 
        false /* checkForDestroyOp */, false /* checkForInvalidateOp */);
  }

  @Test
  public void expirationInvalidOpEventsNotPropogatedByDefault() {
    // For Replicated Region with expiration-invalid op.
    // Number of expected events 2. Two for create and none for eviction destroy.
    createPopulateAndVerifyEvents(false /*isPR */, true /* ignoreEvictionExpiration */, 
        2 /* expectedEvents */ , false /* eviction */, false /* evictionOverflow */, 
        false /* expirationDestroy */, true /* expirationInvalidate */, 
        false /* checkForDestroyOp */, false /* checkForInvalidateOp */);
  }

  @Test
  public void expirationInvalidOpEventsNotPropogatedByDefaultForPR() {
    // For Replicated Region with expiration-invalid op.
    // Number of expected events 2. Two for create and none for eviction destroy.
    createPopulateAndVerifyEvents(true /*isPR */, true /* ignoreEvictionExpiration */, 
        2 /* expectedEvents */ , false /* eviction */, false /* evictionOverflow */, 
        false /* expirationDestroy */, true /* expirationInvalidate */, 
        false /* checkForDestroyOp */, false /* checkForInvalidateOp */);
  }
  
  @Test
  public void evictionPropogatedUsingIgnoreEvictionAndExpirationAttribute() {
    // For Replicated Region with eviction-destroy op.
    // Number of expected events 3. Two for create and One for eviction destroy.
    createPopulateAndVerifyEvents(false /*isPR */, false /* ignoreEvictionExpiration */, 
        3 /* expectedEvents */ , true /* eviction */, false /* evictionOverflow */, 
        false /* expirationDestroy */, false /* expirationInvalidate */, 
        true /* checkForDestroyOp */, false /* checkForInvalidateOp */);
  }

  @Test
  public void evictionPropogatedUsingIgnoreEvictionAndExpirationAttributeForPR() {
    // For PR with eviction-destroy op.
    // Number of expected events 3. Two for create and One for eviction destroy.
    createPopulateAndVerifyEvents(true /*isPR */, false /* ignoreEvictionExpiration */, 
        3 /* expectedEvents */ , true /* eviction */, false /* evictionOverflow */, 
        false /* expirationDestroy */, false /* expirationInvalidate */, 
        true /* checkForDestroyOp */, false /* checkForInvalidateOp */);
  }

  @Test
  public void overflowNotPropogatedUsingIgnoreEvictionAndExpirationAttribute() {
    // For Replicated Region with eviction-overflow op.
    // Number of expected events 2. Two for create and non for eviction overflow.
    createPopulateAndVerifyEvents(false /*isPR */, false /* ignoreEvictionExpiration */,  
        2 /* expectedEvents */ , false /* eviction */, true /* evictionOverflow */, 
        false /* expirationDestroy */, false /* expirationInvalidate */, 
        false /* checkForDestroyOp */, false /* checkForInvalidateOp */);
  }

  @Test
  public void overflowNotPropogatedUsingIgnoreEvictionAndExpirationAttributeForPR() {
    // For PR with eviction-overflow op.
    // Number of expected events 2. Two for create and non for eviction overflow.
    createPopulateAndVerifyEvents(true /*isPR */, false /* ignoreEvictionExpiration */,  
        2 /* expectedEvents */ , false /* eviction */, true /* evictionOverflow */, 
        false /* expirationDestroy */, false /* expirationInvalidate */, 
        false /* checkForDestroyOp */, false /* checkForInvalidateOp */);
  }

  @Test
  public void expirationDestroyPropogatedUsingIgnoreEvictionAndExpirationAttribute() {
    // For Replicated Region with expiration-destroy op.
    // Number of expected events 4. Two for create and Two for expiration destroy.
    createPopulateAndVerifyEvents(false /*isPR */, false /* ignoreEvictionExpiration */,  
        4 /* expectedEvents */ , false /* eviction */, false /* evictionOverflow */, 
        true /* expirationDestroy */, false /* expirationInvalidate */, 
        true /* checkForDestroyOp */, false /* checkForInvalidateOp */);    
  }

  @Test
  public void expirationDestroyPropogatedUsingIgnoreEvictionAndExpirationAttributeForPR() {
    // For PR with expiration-destroy op.
    // Number of expected events 4. Two for create and Two for expiration destroy.
    createPopulateAndVerifyEvents(true /*isPR */, false /* ignoreEvictionExpiration */,  
        4 /* expectedEvents */ , false /* eviction */, false /* evictionOverflow */, 
        true /* expirationDestroy */, false /* expirationInvalidate */, 
        true /* checkForDestroyOp */, false /* checkForInvalidateOp */);    
  }

  @Test
  public void expirationInvalidateNotPropogatedUsingIgnoreEvictionAndExpirationAttribute() {
    // For Replicated Region with expiration-invalidate op.
    // Currently invalidate event callbacks are not made to GateWay sender.
    // Invalidates are not sent to AEQ.
    // Number of expected events 2. None for expiration invalidate.
    createPopulateAndVerifyEvents(false /*isPR */, false /* ignoreEvictionExpiration */,  
        2 /* expectedEvents */ , false /* eviction */, false /* evictionOverflow */, 
        false /* expirationDestroy */, true /* expirationInvalidate */, 
        false /* checkForDestroyOp */, false /* checkForInvalidateOp */);
  }

  @Test
  public void expirationInvalidateNotPropogatedUsingIgnoreEvictionAndExpirationAttributeForPR() {
    // For PR with expiration-invalidate op.
    // Currently invalidate event callbacks are not made to GateWay sender.
    // Invalidates are not sent to AEQ.
    // Number of expected events 2. None for expiration invalidate.
    createPopulateAndVerifyEvents(true /*isPR */, false /* ignoreEvictionExpiration */,  
        2 /* expectedEvents */ , false /* eviction */, false /* evictionOverflow */, 
        false /* expirationDestroy */, true /* expirationInvalidate */, 
        false /* checkForDestroyOp */, false /* checkForInvalidateOp */);
  }

  
  
  private void createPopulateAndVerifyEvents(boolean isPR, boolean ignoreEvictionExpiration, int expectedEvents, boolean eviction, boolean evictionOverflow, 
      boolean expirationDestroy, boolean expirationInvalidate, boolean checkForDestroyOp, boolean checkForInvalidateOp) {
    
    // String aeqId = "AEQTest";
    String aeqId = name.getMethodName();
    
    // To store AEQ events for validation.
    List<AsyncEvent> events = new ArrayList<AsyncEvent>();
    
    // Create AEQ
    createAsyncEventQueue(aeqId, ignoreEvictionExpiration, events);    
    
    // Create region with eviction/expiration
    Region r = createRegion("ReplicatedRegionForAEQ", isPR, aeqId, eviction, evictionOverflow, expirationDestroy, expirationInvalidate);
    
    // Populate region with two entires.
    r.put("Key-1", "Value-1");
    r.put("Key-2", "Value-2");
    
    // The AQListner should get two events. One for create, one for destroy.
    Awaitility.await().atMost(100, TimeUnit.SECONDS).until(() -> {return (events.size() == expectedEvents);});
    
    // Check for the expected operation.
    if (checkForDestroyOp) {
      assertTrue("Expiration event not arrived", checkForOperation(events, false, true));
    }

    if (checkForInvalidateOp) {
      assertTrue("Invalidate event not arrived", checkForOperation(events, true, false));
    }
    
    // Test complete. Destroy region.
    r.destroyRegion();
  }

  private boolean checkForOperation(List<AsyncEvent> events, boolean invalidate, boolean destroy) {
    boolean found = false;
    for (AsyncEvent e : events) {
      if (invalidate && e.getOperation().isInvalidate()) {
        found = true;
        break;
      }
      if (destroy && e.getOperation().isDestroy()) {
        found = true;
        break;
      }
    }
    return found;
  }

  private void createAsyncEventQueue(String id, boolean ignoreEvictionExpiration, List<AsyncEvent> storeEvents) {
    AsyncEventListener al = this.createAsyncListener(storeEvents);
    aeq = cache.createAsyncEventQueueFactory().setParallel(false)
        .setIgnoreEvictionAndExpiration(ignoreEvictionExpiration)
        .setBatchSize(1).setBatchTimeInterval(1).create(id, al);
  }
  
  private Region createRegion(String name, boolean isPR, String aeqId, boolean evictionDestroy, 
      boolean evictionOverflow, boolean expirationDestroy, boolean expirationInvalidate) {
    RegionFactory rf;
    if (isPR) {
      rf = cache.createRegionFactory(RegionShortcut.PARTITION);
    } else {
      rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    }
    // Set AsyncQueue.
    rf.addAsyncEventQueueId(aeqId);
    if (evictionDestroy) {
      rf.setEvictionAttributes(EvictionAttributes.createLIFOEntryAttributes(1, EvictionAction.LOCAL_DESTROY));
    }
    if (evictionOverflow) {
      rf.setEvictionAttributes(EvictionAttributes.createLIFOEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK)); 
    }
    if (expirationDestroy) {
      rf.setEntryTimeToLive(new ExpirationAttributes(1, ExpirationAction.DESTROY));
    }
    if (expirationInvalidate) {
      rf.setEntryTimeToLive(new ExpirationAttributes(1, ExpirationAction.INVALIDATE));
    }
    
    return rf.create(name);
  }
  
  private AsyncEventListener createAsyncListener(List<AsyncEvent> list) {
    AsyncEventListener listener = new AsyncEventListener() {
      private List<AsyncEvent> aeList = list;
      
      @Override
      public void close() {
        // TODO Auto-generated method stub
      }

      @Override
      public boolean processEvents(List<AsyncEvent> arg0) {
        System.out.println("AEQ Listener.process()");
        new Exception("Stack trace for AsyncEventQueue").printStackTrace();
        // TODO Auto-generated method stub
        aeList.addAll(arg0);
        System.out.println("AEQ Event :" + arg0);
        return true;
      }
    };
    return listener;
  }


}
