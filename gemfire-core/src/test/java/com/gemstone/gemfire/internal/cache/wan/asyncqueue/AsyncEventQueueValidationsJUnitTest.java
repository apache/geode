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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.wan.asyncqueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.internal.cache.wan.AsyncEventQueueConfigurationException;
import com.gemstone.gemfire.internal.cache.wan.MyAsyncEventListener;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import junit.framework.TestCase;

/**
 * @author skumar
 *
 */
@Category(IntegrationTest.class)
public class AsyncEventQueueValidationsJUnitTest {

  private Cache cache;
  
  @Test
  public void testConcurrentParallelAsyncEventQueueAttributesWrongDispatcherThreads() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    try {
      AsyncEventQueueFactory fact = cache.createAsyncEventQueueFactory();
      fact.setParallel(true);
      fact.setDispatcherThreads(-5);
      fact.setOrderPolicy(OrderPolicy.KEY);
      fact.create("id", new com.gemstone.gemfire.internal.cache.wan.MyAsyncEventListener());
      fail("Expected AsyncEventQueueConfigurationException.");
    } catch (AsyncEventQueueConfigurationException e) {
        assertTrue(e.getMessage()
            .contains(" can not be created with dispatcher threads less than 1"));
    }
  }
  
  
  @Test
  public void testConcurrentParallelAsyncEventQueueAttributesOrderPolicyThread() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    try {
      AsyncEventQueueFactory fact = cache.createAsyncEventQueueFactory();
      fact.setParallel(true);
      fact.setDispatcherThreads(5);
      fact.setOrderPolicy(OrderPolicy.THREAD);
      fact.create("id", new com.gemstone.gemfire.internal.cache.wan.MyAsyncEventListener());
      fail("Expected AsyncEventQueueConfigurationException.");
    } catch (AsyncEventQueueConfigurationException e) {
        assertTrue(e.getMessage()
            .contains("can not be created with OrderPolicy"));
    }
  }
  
  
}
