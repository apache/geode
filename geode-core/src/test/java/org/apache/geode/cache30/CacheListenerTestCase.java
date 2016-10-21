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
package org.apache.geode.cache30;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.internal.cache.AbstractRegionMap;
import org.apache.geode.test.dunit.Wait;

/**
 * An abstract class whose test methods test the functionality of {@link CacheListener}s that are
 * invoked locally.
 *
 * @see MultiVMRegionTestCase#testRemoteCacheWriter
 *
 *
 * @since GemFire 3.0
 */
public abstract class CacheListenerTestCase extends CacheLoaderTestCase {

  public CacheListenerTestCase() {
    super();
  }

  /////////////////////// Test Methods ///////////////////////

  /**
   * Tests that the <code>CacheListener</code> is called after an entry is
   * {@linkplain CacheListener#afterCreate created}.
   */
  @Test
  public void testCacheListenerAfterCreate() throws CacheException {
    String name = this.getUniqueName();
    final Object key = this.getUniqueName();
    final Object value = new Integer(42);
    Object arg = "ARG";

    TestCacheListener listener = new TestCacheListener() {
      public void afterCreate2(EntryEvent event) {
        assertEquals(key, event.getKey());
        assertEquals(value, event.getNewValue());
        assertNull(event.getOldValue());
        assertFalse(event.isLoad());
        assertFalse(event.isLocalLoad());
        assertFalse(event.isNetLoad());
        assertFalse(event.isNetSearch());
      }

      public void afterDestroy2(EntryEvent event) {
        // This method will get invoked when the entry is destroyed
      }
    };

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setCacheListener(listener);
    Region region = createRegion(name, factory.create());

    region.create(key, value);
    assertTrue(listener.wasInvoked());
    region.destroy(key);
    assertTrue(listener.wasInvoked());

    region.put(key, value);
    assertTrue(listener.wasInvoked());
    region.destroy(key);
    assertTrue(listener.wasInvoked());

    region.create(key, value, arg);
    assertTrue(listener.wasInvoked());
    region.destroy(key);
    assertTrue(listener.wasInvoked());

    region.put(key, value, arg);
    assertTrue(listener.wasInvoked());
    region.destroy(key);
    assertTrue(listener.wasInvoked());
  }

  /**
   * Tests that the <code>CacheListener</code> is called after an entry is
   * {@linkplain CacheListener#afterUpdate updated}.
   */
  @Test
  public void testCacheListenerAfterUpdate() throws CacheException {
    String name = this.getUniqueName();
    final Object key = this.getUniqueName();
    final Object oldValue = new Integer(42);
    final Object newValue = new Integer(43);
    Object arg = "ARG";

    TestCacheListener listener = new TestCacheListener() {
      public void afterCreate2(EntryEvent event) {
        // This method will get invoked when the region is populated
      }

      public void afterDestroy2(EntryEvent event) {
        // This method will get invoked when an entry is destroyed
      }

      public void afterUpdate2(EntryEvent event) {
        assertEquals(key, event.getKey());
        assertEquals(newValue, event.getNewValue());
        assertEquals(oldValue, event.getOldValue());
        assertFalse(event.isLoad());
        assertFalse(event.isLocalLoad());
        assertFalse(event.isNetLoad());
        assertFalse(event.isNetSearch());
      }
    };

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setCacheListener(listener);
    Region region = createRegion(name, factory.create());

    region.create(key, oldValue);
    assertTrue(listener.wasInvoked());
    region.put(key, newValue);
    assertTrue(listener.wasInvoked());
    region.destroy(key);
    assertTrue(listener.wasInvoked());

    region.put(key, oldValue);
    assertTrue(listener.wasInvoked());
    region.put(key, newValue);
    assertTrue(listener.wasInvoked());
    region.destroy(key);
    assertTrue(listener.wasInvoked());

    region.create(key, oldValue);
    assertTrue(listener.wasInvoked());
    region.put(key, newValue, arg);
    assertTrue(listener.wasInvoked());
    region.destroy(key);
    assertTrue(listener.wasInvoked());

    region.put(key, oldValue);
    assertTrue(listener.wasInvoked());
    region.put(key, newValue, arg);
    assertTrue(listener.wasInvoked());
    region.destroy(key);
    assertTrue(listener.wasInvoked());
  }

  /**
   * Tests that the <code>CacheListener</code> is called after an entry is
   * {@linkplain CacheListener#afterDestroy destroyed}.
   */
  @Test
  public void testCacheListenerAfterDestroy() throws CacheException {
    String name = this.getUniqueName();
    final Object key = this.getUniqueName();
    final Object value = new Integer(42);
    Object arg = "ARG";
    // final boolean localScope = getRegionAttributes().getScope().isLocal();

    TestCacheListener listener = new TestCacheListener() {
      public void afterCreate2(EntryEvent event) {
        // This method will get invoked when the region is populated
      }

      public void afterDestroy2(EntryEvent event) {
        assertEquals(key, event.getKey());
        assertEquals(value, event.getOldValue());
        assertNull(event.getNewValue());
        assertFalse(event.isLoad());
        assertFalse(event.isLocalLoad());
        assertFalse(event.isNetLoad());
        assertFalse(event.isNetSearch());
      }
    };

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setCacheListener(listener);
    Region region = createRegion(name, factory.create());

    region.create(key, value);
    assertTrue(listener.wasInvoked());
    region.destroy(key);
    assertTrue(listener.wasInvoked());

    region.create(key, value);
    assertTrue(listener.wasInvoked());
    region.destroy(key, arg);
    assertTrue(listener.wasInvoked());
  }

  /**
   * Tests that the <code>CacheListener</code> is called after an entry is
   * {@linkplain CacheListener#afterInvalidate invalidated}.
   */
  @Test
  public void testCacheListenerAfterInvalidate() throws CacheException {
    String name = this.getUniqueName();
    final Object key = this.getUniqueName();
    final Object value = new Integer(42);
    // Object arg = "ARG";

    TestCacheListener listener = new TestCacheListener() {
      public void afterCreate2(EntryEvent event) {
        // This method will get invoked when the region is populated
      }

      public void afterInvalidate2(EntryEvent event) {
        assertEquals(key, event.getKey());
        assertEquals(value, event.getOldValue());
        assertNull(event.getNewValue());
        assertFalse(event.isLoad());
        assertFalse(event.isLocalLoad());
        assertFalse(event.isNetLoad());
        assertFalse(event.isNetSearch());
      }
    };

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setCacheListener(listener);
    Region region = createRegion(name, factory.create());

    // Does not exist so should not invoke listener
    try {
      region.invalidate(key);
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException expected) {
    }
    assertFalse(listener.wasInvoked());

    region.create(key, value);
    assertTrue(listener.wasInvoked());
    region.invalidate(key);
    assertTrue(listener.wasInvoked());

    // already invalid so should not invoke listener
    region.invalidate(key);
    assertFalse(listener.wasInvoked());
  }

  @Test
  public void testCacheListenerAfterInvalidateWithForce() throws CacheException {
    AbstractRegionMap.FORCE_INVALIDATE_EVENT = true;
    try {
      String name = this.getUniqueName();
      final Object key = this.getUniqueName();
      final Object value = new Integer(42);

      TestCacheListener listener = new TestCacheListener() {
        int invalidateCount = 0;

        public void afterCreate2(EntryEvent event) {
          // This method will get invoked when the region is populated
        }

        public void afterInvalidate2(EntryEvent event) {
          invalidateCount++;
          assertEquals(key, event.getKey());
          if (invalidateCount == 2) {
            assertEquals(value, event.getOldValue());
          } else {
            assertNull(event.getOldValue());
          }
          assertNull(event.getNewValue());
          assertFalse(event.isLoad());
          assertFalse(event.isLocalLoad());
          assertFalse(event.isNetLoad());
          assertFalse(event.isNetSearch());
        }
      };

      AttributesFactory factory = new AttributesFactory(getRegionAttributes());
      factory.setCacheListener(listener);
      Region region = createRegion(name, factory.create());

      // Does not exist but should still invoke listener
      try {
        region.invalidate(key);
        fail("expected EntryNotFoundException");
      } catch (EntryNotFoundException expected) {
      }
      assertTrue(listener.wasInvoked());

      region.create(key, value);
      assertTrue(listener.wasInvoked());
      region.invalidate(key);
      assertTrue(listener.wasInvoked());
      // already invalid but should still invoke listener
      region.invalidate(key);
      assertTrue(listener.wasInvoked());
    } finally {
      AbstractRegionMap.FORCE_INVALIDATE_EVENT = false;
    }
  }


  /**
   * Tests that the <code>CacheListener</code> is called after a region is destroyed.
   *
   * @see CacheListener#afterRegionDestroy
   * @see CacheListener#close
   */
  @Test
  public void testCacheListenerAfterRegionDestroy() throws CacheException, InterruptedException {

    final String name = this.getUniqueName();
    Object arg = "ARG";
    // final String exception = "EXCEPTION";
    // final boolean localScope = getRegionAttributes().getScope().isLocal();

    TestCacheListener listener = new TestCacheListener() {
      private boolean closed = false;
      private boolean destroyed = false;

      public boolean wasInvoked() {
        boolean value = closed && destroyed;
        super.wasInvoked();
        return value;
      }

      public void close2() {
        this.closed = true;
      }

      public void afterRegionDestroy2(RegionEvent event) {
        assertEquals(name, event.getRegion().getName());
        // this should be a distributed destroy unless the region
        // is local scope
        assertFalse(event.isExpiration());
        assertFalse(event.isOriginRemote());

        this.destroyed = true;
      }
    };

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setCacheListener(listener);
    RegionAttributes attrs = factory.create();
    Region region;

    region = createRegion(name, attrs);
    assertTrue(region.getAttributes().getCacheListener() != null);
    // org.apache.geode.internal.util.DebuggerSupport.waitForJavaDebugger(getLogWriter());
    region.destroyRegion();
    Wait.pause(100); // extra pause
    assertTrue(region.isDestroyed());
    assertTrue(listener.wasInvoked());

    region = createRegion(name, attrs);
    region.destroyRegion(arg);
    assertTrue(listener.wasInvoked());
    assertTrue(region.isDestroyed());
  }

  /**
   * Tests that the <code>CacheListener</code> is called after a region is invalidated.
   *
   * @see CacheListener#afterRegionInvalidate
   * @see CacheListener#close
   */
  @Test
  public void testCacheListenerAfterRegionInvalidate() throws CacheException, InterruptedException {

    final String name = this.getUniqueName();
    // Object arg = "ARG";
    // final String exception = "EXCEPTION";

    TestCacheListener listener = new TestCacheListener() {
      private boolean closed = false;
      private boolean invalidated = false;

      public boolean wasInvoked() {
        boolean value = invalidated;
        super.wasInvoked();
        return value;
      }

      public void close2() {
        this.closed = true;
      }

      public void afterRegionInvalidate2(RegionEvent event) {
        assertEquals(name, event.getRegion().getName());
        assertFalse(event.isExpiration());
        assertFalse(event.isOriginRemote());

        this.invalidated = true;
      }
    };

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setCacheListener(listener);
    RegionAttributes attrs = factory.create();
    Region region;

    region = createRegion(name, attrs);
    region.invalidateRegion();
    Wait.pause(500);
    assertTrue(listener.wasInvoked());
    assertEquals(0, region.values().size());
  }

}
