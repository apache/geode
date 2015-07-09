/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
