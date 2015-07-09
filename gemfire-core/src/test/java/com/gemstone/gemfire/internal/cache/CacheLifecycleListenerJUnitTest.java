/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import junit.framework.TestCase;

/**
 * Tests notifications of CacheLifecycleListener from GemFireCacheImpl.
 * 
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class CacheLifecycleListenerJUnitTest  {

  @Test
  public void testAddAndRemoveNull() {
    GemFireCacheImpl.addCacheLifecycleListener(null);
    GemFireCacheImpl.removeCacheLifecycleListener(null);
  }
  
  @Test
  public void testRemoveNonExistent() {
    final List<CacheLifecycleCallback> cacheCreatedCallbacks = new ArrayList<CacheLifecycleCallback>();
    final List<CacheLifecycleCallback> cacheClosedCallbacks = new ArrayList<CacheLifecycleCallback>();
    final TestCacheLifecycleListener listener = new TestCacheLifecycleListener(cacheCreatedCallbacks, cacheClosedCallbacks);
    GemFireCacheImpl.removeCacheLifecycleListener(listener);
  }
  
  @Test
  public void testCallbacks() {
    final List<CacheLifecycleCallback> cacheCreatedCallbacks = new ArrayList<CacheLifecycleCallback>();
    final List<CacheLifecycleCallback> cacheClosedCallbacks = new ArrayList<CacheLifecycleCallback>();

    final TestCacheLifecycleListener listener = new TestCacheLifecycleListener(cacheCreatedCallbacks, cacheClosedCallbacks);
    try {
      GemFireCacheImpl.addCacheLifecycleListener(listener);
  
      // assert no create callback
      assertTrue(cacheCreatedCallbacks.isEmpty());
      // assert no close callback
      assertTrue(cacheClosedCallbacks.isEmpty());
      
      final Properties props = new Properties();
      props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
      props.setProperty(DistributionConfig.LOCATORS_NAME, "");
      
      final GemFireCacheImpl cache = (GemFireCacheImpl) new CacheFactory(props).create();
      try {
        // assert one create callback
        assertFalse(cacheCreatedCallbacks.isEmpty());
        assertEquals(1, cacheCreatedCallbacks.size());
        assertEquals(cache, cacheCreatedCallbacks.get(0).getCache());
        // assert no close callback
        assertTrue(cacheClosedCallbacks.isEmpty());
      } finally {
        cache.close();
      }
      
      // assert one create callback
      assertFalse(cacheCreatedCallbacks.isEmpty());
      assertEquals(1, cacheCreatedCallbacks.size());
      assertEquals(cache, cacheCreatedCallbacks.get(0).getCache());
      // assert one close callback
      assertFalse(cacheClosedCallbacks.isEmpty());
      assertEquals(1, cacheClosedCallbacks.size());
      assertEquals(cache, cacheClosedCallbacks.get(0).getCache());
    } finally {
      GemFireCacheImpl.removeCacheLifecycleListener(listener);
    }
  }

  @Test
  public void testRemoveBeforeCreate() {
    final List<CacheLifecycleCallback> cacheCreatedCallbacks = new ArrayList<CacheLifecycleCallback>();
    final List<CacheLifecycleCallback> cacheClosedCallbacks = new ArrayList<CacheLifecycleCallback>();

    final TestCacheLifecycleListener listener = new TestCacheLifecycleListener(cacheCreatedCallbacks, cacheClosedCallbacks);
    try {
      GemFireCacheImpl.addCacheLifecycleListener(listener);
      GemFireCacheImpl.removeCacheLifecycleListener(listener);

      // assert no create callback
      assertTrue(cacheCreatedCallbacks.isEmpty());
      // assert no close callback
      assertTrue(cacheClosedCallbacks.isEmpty());
      
      final Properties props = new Properties();
      props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
      props.setProperty(DistributionConfig.LOCATORS_NAME, "");
      
      final GemFireCacheImpl cache = (GemFireCacheImpl) new CacheFactory(props).create();
      try {
        // assert no create callback
        assertTrue(cacheCreatedCallbacks.isEmpty());
        // assert no close callback
        assertTrue(cacheClosedCallbacks.isEmpty());
      } finally {
        cache.close();
      }
      
      // assert no create callback
      assertTrue(cacheCreatedCallbacks.isEmpty());
      // assert no close callback
      assertTrue(cacheClosedCallbacks.isEmpty());
    } finally {
      GemFireCacheImpl.removeCacheLifecycleListener(listener);
    }
  }
  
  @Test
  public void testRemoveBeforeClose() {
    final List<CacheLifecycleCallback> cacheCreatedCallbacks = new ArrayList<CacheLifecycleCallback>();
    final List<CacheLifecycleCallback> cacheClosedCallbacks = new ArrayList<CacheLifecycleCallback>();

    final TestCacheLifecycleListener listener = new TestCacheLifecycleListener(cacheCreatedCallbacks, cacheClosedCallbacks);
    try {
      GemFireCacheImpl.addCacheLifecycleListener(listener);
  
      // assert no create callback
      assertTrue(cacheCreatedCallbacks.isEmpty());
      // assert no close callback
      assertTrue(cacheClosedCallbacks.isEmpty());
      
      final Properties props = new Properties();
      props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
      props.setProperty(DistributionConfig.LOCATORS_NAME, "");
      
      final GemFireCacheImpl cache = (GemFireCacheImpl) new CacheFactory(props).create();
      try {
        // assert one create callback
        assertFalse(cacheCreatedCallbacks.isEmpty());
        assertEquals(1, cacheCreatedCallbacks.size());
        assertEquals(cache, cacheCreatedCallbacks.get(0).getCache());
        // assert no close callback
        assertTrue(cacheClosedCallbacks.isEmpty());
      } finally {
        GemFireCacheImpl.removeCacheLifecycleListener(listener);
        cache.close();
      }
    
      // assert one create callback
      assertFalse(cacheCreatedCallbacks.isEmpty());
      assertEquals(1, cacheCreatedCallbacks.size());
      assertEquals(cache, cacheCreatedCallbacks.get(0).getCache());
      // assert no close callback
      assertTrue(cacheClosedCallbacks.isEmpty());
    } finally {
      GemFireCacheImpl.removeCacheLifecycleListener(listener);
    }
  }

  @Test
  public void testCallbacksRepeat() {
    final List<CacheLifecycleCallback> cacheCreatedCallbacks = new ArrayList<CacheLifecycleCallback>();
    final List<CacheLifecycleCallback> cacheClosedCallbacks = new ArrayList<CacheLifecycleCallback>();

    final TestCacheLifecycleListener listener = new TestCacheLifecycleListener(cacheCreatedCallbacks, cacheClosedCallbacks);
    try {
      GemFireCacheImpl.addCacheLifecycleListener(listener);
  
      // assert no create callback
      assertTrue(cacheCreatedCallbacks.isEmpty());
      // assert no close callback
      assertTrue(cacheClosedCallbacks.isEmpty());
      
      final Properties props = new Properties();
      props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
      props.setProperty(DistributionConfig.LOCATORS_NAME, "");
      
      final GemFireCacheImpl cache1 = (GemFireCacheImpl) new CacheFactory(props).create();
      try {
        // assert one create callback
        assertFalse(cacheCreatedCallbacks.isEmpty());
        assertEquals(1, cacheCreatedCallbacks.size());
        assertEquals(cache1, cacheCreatedCallbacks.get(0).getCache());
        // assert no close callback
        assertTrue(cacheClosedCallbacks.isEmpty());
      } finally {
        cache1.close();
      }
      
      // assert one create callback
      assertFalse(cacheCreatedCallbacks.isEmpty());
      assertEquals(1, cacheCreatedCallbacks.size());
      assertEquals(cache1, cacheCreatedCallbacks.get(0).getCache());
      // assert one close callback
      assertFalse(cacheClosedCallbacks.isEmpty());
      assertEquals(1, cacheClosedCallbacks.size());
      assertEquals(cache1, cacheClosedCallbacks.get(0).getCache());
      
      final GemFireCacheImpl cache2 = (GemFireCacheImpl) new CacheFactory(props).create();
      try {
        // assert two create callback
        assertFalse(cacheCreatedCallbacks.isEmpty());
        assertEquals(2, cacheCreatedCallbacks.size());
        assertEquals(cache1, cacheCreatedCallbacks.get(0).getCache());
        assertEquals(cache2, cacheCreatedCallbacks.get(1).getCache());
        // assert one close callback
        assertFalse(cacheClosedCallbacks.isEmpty());
        assertEquals(1, cacheClosedCallbacks.size());
        assertEquals(cache1, cacheClosedCallbacks.get(0).getCache());
      } finally {
        cache2.close();
      }
      
      // assert two create callbacks
      assertFalse(cacheCreatedCallbacks.isEmpty());
      assertEquals(2, cacheCreatedCallbacks.size());
      assertEquals(cache1, cacheCreatedCallbacks.get(0).getCache());
      assertEquals(cache2, cacheCreatedCallbacks.get(1).getCache());
      // assert two close callbacks
      assertFalse(cacheClosedCallbacks.isEmpty());
      assertEquals(2, cacheClosedCallbacks.size());
      assertEquals(cache1, cacheClosedCallbacks.get(0).getCache());
      assertEquals(cache2, cacheClosedCallbacks.get(1).getCache());
    } finally {
      GemFireCacheImpl.removeCacheLifecycleListener(listener);
    }
  }

  @Test
  public void testAddAfterCreate() {
    final List<CacheLifecycleCallback> cacheCreatedCallbacks = new ArrayList<CacheLifecycleCallback>();
    final List<CacheLifecycleCallback> cacheClosedCallbacks = new ArrayList<CacheLifecycleCallback>();

    final TestCacheLifecycleListener listener = new TestCacheLifecycleListener(cacheCreatedCallbacks, cacheClosedCallbacks);
    // assert no create callback
    assertTrue(cacheCreatedCallbacks.isEmpty());
    // assert no close callback
    assertTrue(cacheClosedCallbacks.isEmpty());
    
    final Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    
    final GemFireCacheImpl cache = (GemFireCacheImpl) new CacheFactory(props).create();
    try {
      try {
        // assert no create callback
        assertTrue(cacheCreatedCallbacks.isEmpty());
        // assert no close callback
        assertTrue(cacheClosedCallbacks.isEmpty());
        
        GemFireCacheImpl.addCacheLifecycleListener(listener);
      } finally {
        cache.close();
      }

      // assert no create callback
      assertTrue(cacheCreatedCallbacks.isEmpty());
      // assert one close callback
      assertFalse(cacheClosedCallbacks.isEmpty());
      assertEquals(1, cacheClosedCallbacks.size());
      assertEquals(cache, cacheClosedCallbacks.get(0).getCache());
    } finally {
      GemFireCacheImpl.removeCacheLifecycleListener(listener);
    }
  }

  static final class CacheLifecycleCallback {
    
    private final GemFireCacheImpl cache;
    private final long timeStamp;
    
    CacheLifecycleCallback(GemFireCacheImpl cache) {
      this.cache = cache;
      this.timeStamp = System.currentTimeMillis();
    }
    
    GemFireCacheImpl getCache() {
      return this.cache;
    }
  }
  
  static final class TestCacheLifecycleListener implements CacheLifecycleListener {

    private final List<CacheLifecycleCallback> cacheCreatedCallbacks;
    private final List<CacheLifecycleCallback> cacheClosedCallbacks;
    
    TestCacheLifecycleListener(List<CacheLifecycleCallback> cacheCreatedCallbacks, List<CacheLifecycleCallback> cacheClosedCallbacks) {
      this.cacheCreatedCallbacks = cacheCreatedCallbacks;
      this.cacheClosedCallbacks = cacheClosedCallbacks;
    }
    
    @Override
    public void cacheCreated(GemFireCacheImpl cache) {
      this.cacheCreatedCallbacks.add(new CacheLifecycleCallback(cache));
    }

    @Override
    public void cacheClosed(GemFireCacheImpl cache) {
      this.cacheClosedCallbacks.add(new CacheLifecycleCallback(cache));
    }
  }
}
