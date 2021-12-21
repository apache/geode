/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.CacheFactory;

/**
 * Tests notifications of CacheLifecycleListener from GemFireCacheImpl.
 */
public class CacheLifecycleListenerJUnitTest {

  @Test
  public void testAddAndRemoveNull() throws Exception {
    GemFireCacheImpl.addCacheLifecycleListener(null);
    GemFireCacheImpl.removeCacheLifecycleListener(null);
  }

  @Test
  public void testRemoveNonExistent() throws Exception {
    List<CacheLifecycleCallback> cacheCreatedCallbacks = new ArrayList<>();
    List<CacheLifecycleCallback> cacheClosedCallbacks = new ArrayList<>();
    TestCacheLifecycleListener listener =
        new TestCacheLifecycleListener(cacheCreatedCallbacks, cacheClosedCallbacks);
    GemFireCacheImpl.removeCacheLifecycleListener(listener);
  }

  @Test
  public void testCallbacks() throws Exception {
    List<CacheLifecycleCallback> cacheCreatedCallbacks = new ArrayList<>();
    List<CacheLifecycleCallback> cacheClosedCallbacks = new ArrayList<>();

    TestCacheLifecycleListener listener =
        new TestCacheLifecycleListener(cacheCreatedCallbacks, cacheClosedCallbacks);
    try {
      GemFireCacheImpl.addCacheLifecycleListener(listener);

      // assert no create callback
      assertTrue(cacheCreatedCallbacks.isEmpty());
      // assert no close callback
      assertTrue(cacheClosedCallbacks.isEmpty());

      Properties props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, "");

      InternalCache cache = (InternalCache) new CacheFactory(props).create();
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
  public void testRemoveBeforeCreate() throws Exception {
    List<CacheLifecycleCallback> cacheCreatedCallbacks = new ArrayList<>();
    List<CacheLifecycleCallback> cacheClosedCallbacks = new ArrayList<>();

    TestCacheLifecycleListener listener =
        new TestCacheLifecycleListener(cacheCreatedCallbacks, cacheClosedCallbacks);
    try {
      GemFireCacheImpl.addCacheLifecycleListener(listener);
      GemFireCacheImpl.removeCacheLifecycleListener(listener);

      // assert no create callback
      assertTrue(cacheCreatedCallbacks.isEmpty());
      // assert no close callback
      assertTrue(cacheClosedCallbacks.isEmpty());

      Properties props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, "");

      InternalCache cache = (InternalCache) new CacheFactory(props).create();
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
  public void testRemoveBeforeClose() throws Exception {
    List<CacheLifecycleCallback> cacheCreatedCallbacks = new ArrayList<>();
    List<CacheLifecycleCallback> cacheClosedCallbacks = new ArrayList<>();

    TestCacheLifecycleListener listener =
        new TestCacheLifecycleListener(cacheCreatedCallbacks, cacheClosedCallbacks);
    try {
      GemFireCacheImpl.addCacheLifecycleListener(listener);

      // assert no create callback
      assertTrue(cacheCreatedCallbacks.isEmpty());
      // assert no close callback
      assertTrue(cacheClosedCallbacks.isEmpty());

      Properties props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, "");

      InternalCache cache = (InternalCache) new CacheFactory(props).create();
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
  public void testCallbacksRepeat() throws Exception {
    List<CacheLifecycleCallback> cacheCreatedCallbacks = new ArrayList<>();
    List<CacheLifecycleCallback> cacheClosedCallbacks = new ArrayList<>();

    TestCacheLifecycleListener listener =
        new TestCacheLifecycleListener(cacheCreatedCallbacks, cacheClosedCallbacks);
    try {
      GemFireCacheImpl.addCacheLifecycleListener(listener);

      // assert no create callback
      assertTrue(cacheCreatedCallbacks.isEmpty());
      // assert no close callback
      assertTrue(cacheClosedCallbacks.isEmpty());

      Properties props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, "");

      InternalCache cache1 = (InternalCache) new CacheFactory(props).create();
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

      InternalCache cache2 = (InternalCache) new CacheFactory(props).create();
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
  public void testAddAfterCreate() throws Exception {
    List<CacheLifecycleCallback> cacheCreatedCallbacks = new ArrayList<>();
    List<CacheLifecycleCallback> cacheClosedCallbacks = new ArrayList<>();

    TestCacheLifecycleListener listener =
        new TestCacheLifecycleListener(cacheCreatedCallbacks, cacheClosedCallbacks);
    // assert no create callback
    assertTrue(cacheCreatedCallbacks.isEmpty());
    // assert no close callback
    assertTrue(cacheClosedCallbacks.isEmpty());

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");

    InternalCache cache = (InternalCache) new CacheFactory(props).create();
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

  private static class CacheLifecycleCallback {

    private final InternalCache cache;

    CacheLifecycleCallback(InternalCache cache) {
      this.cache = cache;
    }

    InternalCache getCache() {
      return cache;
    }
  }

  private static class TestCacheLifecycleListener implements CacheLifecycleListener {

    private final List<CacheLifecycleCallback> cacheCreatedCallbacks;
    private final List<CacheLifecycleCallback> cacheClosedCallbacks;

    TestCacheLifecycleListener(List<CacheLifecycleCallback> cacheCreatedCallbacks,
        List<CacheLifecycleCallback> cacheClosedCallbacks) {
      this.cacheCreatedCallbacks = cacheCreatedCallbacks;
      this.cacheClosedCallbacks = cacheClosedCallbacks;
    }

    @Override
    public void cacheCreated(InternalCache cache) {
      cacheCreatedCallbacks.add(new CacheLifecycleCallback(cache));
    }

    @Override
    public void cacheClosed(InternalCache cache) {
      cacheClosedCallbacks.add(new CacheLifecycleCallback(cache));
    }
  }
}
