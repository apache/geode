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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;

/**
 * An abstract class whose test methods test the functionality of {@link CacheWriter}s that are
 * invoked locally.
 *
 * @see MultiVMRegionTestCase#testRemoteCacheWriter
 *
 *
 * @since GemFire 3.0
 */
public abstract class CacheWriterTestCase extends RegionAttributesTestCase {

  public CacheWriterTestCase() {
    super();
  }

  /////////////////////// Test Methods ///////////////////////

  /**
   * Tests that the <code>CacheWriter</code> is called before an entry is
   * {@linkplain CacheWriter#beforeCreate created}.
   */
  @Test
  public void testCacheWriterBeforeCreate() throws CacheException {
    String name = getUniqueName();
    final Object key = getUniqueName();
    final Object value = 42;
    final Object arg = "ARG";
    final String exception = "EXCEPTION";

    TestCacheWriter writer = new TestCacheWriter() {
      @Override
      public void beforeCreate2(EntryEvent event) throws CacheWriterException {

        assertEquals(key, event.getKey());
        assertEquals(value, event.getNewValue());
        assertNull(event.getOldValue());
        assertTrue(event.getOperation().isCreate());
        assertFalse(event.getOperation().isLoad());
        assertFalse(event.getOperation().isLocalLoad());
        assertFalse(event.getOperation().isNetLoad());
        assertFalse(event.getOperation().isNetSearch());

        Object argument = event.getCallbackArgument();
        if (argument != null) {
          if (argument.equals(exception)) {
            String s = "Test CacheWriterException";
            throw new CacheWriterException(s);

          } else {
            assertEquals(arg, argument);
          }
        }
      }

      @Override
      public void beforeDestroy2(EntryEvent event) throws CacheWriterException {
        // This method will get invoked when the region is populated
      }
    };

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setCacheWriter(writer);
    Region region = createRegion(name, factory.create());

    region.create(key, value);
    assertTrue(writer.wasInvoked());
    region.destroy(key);
    assertTrue(writer.wasInvoked());

    region.put(key, value);
    assertTrue(writer.wasInvoked());
    region.destroy(key);
    assertTrue(writer.wasInvoked());

    region.create(key, value, arg);
    assertTrue(writer.wasInvoked());
    region.destroy(key);
    assertTrue(writer.wasInvoked());

    region.put(key, value, arg);
    assertTrue(writer.wasInvoked());
    region.destroy(key);
    assertTrue(writer.wasInvoked());

    try {
      region.create(key, value, exception);
      fail("Should have thrown a CacheWriterException");

    } catch (CacheWriterException ex) {
      // pass...
      assertTrue(writer.wasInvoked());
    }

    try {
      region.put(key, value, exception);
      fail("Should have thrown a CacheWriterException");

    } catch (CacheWriterException ex) {
      // pass...
      assertTrue(writer.wasInvoked());
    }

  }

  /**
   * Tests that the <code>CacheWriter</code> is called before an entry is
   * {@linkplain CacheWriter#beforeUpdate updated}.
   */
  @Test
  public void testCacheWriterBeforeUpdate() throws CacheException {
    String name = getUniqueName();
    final Object key = getUniqueName();
    final Object oldValue = 42;
    final Object newValue = 43;
    final Object arg = "ARG";
    final String exception = "EXCEPTION";

    TestCacheWriter writer = new TestCacheWriter() {
      @Override
      public void beforeCreate2(EntryEvent event) throws CacheWriterException {
        // This method will get invoked when the region is populated
      }

      @Override
      public void beforeDestroy2(EntryEvent event) throws CacheWriterException {
        // This method will get invoked when the region is populated
      }

      @Override
      public void beforeUpdate2(EntryEvent event) throws CacheWriterException {

        assertEquals(key, event.getKey());
        assertEquals(newValue, event.getNewValue());
        assertEquals(oldValue, event.getOldValue());
        assertTrue(event.getOperation().isUpdate());
        assertFalse(event.getOperation().isLoad());
        assertFalse(event.getOperation().isLocalLoad());
        assertFalse(event.getOperation().isNetLoad());
        assertFalse(event.getOperation().isNetSearch());

        Object argument = event.getCallbackArgument();
        if (argument != null) {
          if (argument.equals(exception)) {
            String s = "Test CacheWriterException";
            throw new CacheWriterException(s);

          } else {
            assertEquals(arg, argument);
          }
        }
      }
    };

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setCacheWriter(writer);
    Region region = createRegion(name, factory.create());

    region.create(key, oldValue);
    assertTrue(writer.wasInvoked());
    region.put(key, newValue);
    assertTrue(writer.wasInvoked());
    region.destroy(key);
    assertTrue(writer.wasInvoked());

    region.put(key, oldValue);
    assertTrue(writer.wasInvoked());
    region.put(key, newValue);
    assertTrue(writer.wasInvoked());
    region.destroy(key);
    assertTrue(writer.wasInvoked());

    region.create(key, oldValue);
    assertTrue(writer.wasInvoked());
    region.put(key, newValue, arg);
    assertTrue(writer.wasInvoked());
    region.destroy(key);
    assertTrue(writer.wasInvoked());

    region.put(key, oldValue);
    assertTrue(writer.wasInvoked());
    region.put(key, newValue, arg);
    assertTrue(writer.wasInvoked());
    region.destroy(key);
    assertTrue(writer.wasInvoked());

    region.create(key, oldValue);
    assertTrue(writer.wasInvoked());
    try {
      region.put(key, newValue, exception);
      fail("Should have thrown a CacheWriterException");

    } catch (CacheWriterException ex) {
      // pass...
      assertTrue(writer.wasInvoked());
    }
    region.destroy(key);
    assertTrue(writer.wasInvoked());

    region.create(key, oldValue);
    assertTrue(writer.wasInvoked());
    try {
      region.put(key, newValue, exception);
      fail("Should have thrown a CacheWriterException");

    } catch (CacheWriterException ex) {
      // pass...
      assertTrue(writer.wasInvoked());
    }

  }

  /**
   * Tests that the <code>CacheWriter</code> is called before an entry is
   * {@linkplain CacheWriter#beforeDestroy destroyed}.
   */
  @Test
  public void testCacheWriterBeforeDestroy() throws CacheException {
    String name = getUniqueName();
    final Object key = getUniqueName();
    final Object value = 42;
    final Object arg = "ARG";
    final String exception = "EXCEPTION";

    TestCacheWriter writer = new TestCacheWriter() {
      @Override
      public void beforeCreate2(EntryEvent event) throws CacheWriterException {
        // This method will get invoked when the region is populated
      }

      @Override
      public void beforeDestroy2(EntryEvent event) throws CacheWriterException {

        assertEquals(key, event.getKey());
        assertEquals(value, event.getOldValue());
        assertNull(event.getNewValue());
        assertTrue(event.getOperation().isDestroy());
        assertFalse(event.getOperation().isLoad());
        assertFalse(event.getOperation().isLocalLoad());
        assertFalse(event.getOperation().isNetLoad());
        assertFalse(event.getOperation().isNetSearch());

        Object argument = event.getCallbackArgument();
        if (argument != null) {
          if (argument.equals(exception)) {
            String s = "Test CacheWriterException";
            throw new CacheWriterException(s);

          } else {
            assertEquals(arg, argument);
          }
        }
      }
    };

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setCacheWriter(writer);
    Region region = createRegion(name, factory.create());

    region.create(key, value);
    assertTrue(writer.wasInvoked());
    region.destroy(key);
    assertTrue(writer.wasInvoked());

    region.create(key, value);
    assertTrue(writer.wasInvoked());
    region.destroy(key, arg);
    assertTrue(writer.wasInvoked());

    region.create(key, value);
    assertTrue(writer.wasInvoked());
    try {
      region.destroy(key, exception);
      fail("Should have thrown a CacheWriterException");

    } catch (CacheWriterException ex) {
      // pass...
      assertTrue(writer.wasInvoked());
    }
  }

  /**
   * Tests that the <code>CacheWriter</code> is called before a region is destroyed.
   *
   * @see CacheWriter#beforeRegionDestroy
   * @see CacheWriter#close
   */
  @Test
  public void testCacheWriterBeforeRegionDestroy() throws CacheException {

    final String name = getUniqueName();
    final Object arg = "ARG";
    final String exception = "EXCEPTION";

    TestCacheWriter writer = new TestCacheWriter() {
      private boolean closed = false;
      private boolean destroyed = false;

      @Override
      public boolean wasInvoked() {
        boolean value = closed && destroyed;
        super.wasInvoked();
        return value;
      }

      @Override
      public void close2() {
        closed = true;
      }

      @Override
      public void beforeRegionDestroy2(RegionEvent event) throws CacheWriterException {

        assertEquals(name, event.getRegion().getName());
        // this should be a distributed destroy unless the region
        // is local scope
        assertTrue(event.getOperation().isRegionDestroy());
        assertFalse(event.getOperation().isExpiration());
        assertFalse(event.isOriginRemote());

        Object argument = event.getCallbackArgument();
        if (argument != null) {
          if (argument.equals(exception)) {
            String s = "Test CacheWriterException";
            throw new CacheWriterException(s);

          } else {
            assertEquals(arg, argument);
          }
        }

        destroyed = true;
      }
    };

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setCacheWriter(writer);
    RegionAttributes attrs = factory.create();
    Region region;

    region = createRegion(name, attrs);
    region.destroyRegion();
    assertTrue(region.isDestroyed());
    assertTrue(writer.wasInvoked());


    region = createRegion(name, attrs);
    region.destroyRegion(arg);
    assertTrue(writer.wasInvoked());
    assertTrue(region.isDestroyed());


    try {
      region = createRegion(name, attrs);
      region.destroyRegion(exception);
      fail("Should have thrown a CacheWriterException");

    } catch (CacheWriterException ex) {
      // pass...
      assertTrue(writer.wasInvoked());
      assertFalse(region.isDestroyed());
      assertNull(region.getSubregion(name));
    }
  }

  /**
   * Tests that a <code>CacheWriter</code> is <I>not</I> invoked on a
   * {@linkplain Region#localDestroyRegion local destroy}.
   */
  @Test
  public void testCacheWriterLocalDestroy() throws CacheException {
    final String name = getUniqueName();

    // If any of the writer's callback methods are invoked
    TestCacheWriter writer = new TestCacheWriter() {};

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setCacheWriter(writer);
    RegionAttributes attrs = factory.create();
    Region region = createRegion(name, attrs);
    region.localDestroyRegion();
  }

  /**
   * Tests that a {@link CacheWriter} throwing a {@link CacheWriterException} aborts the operation.
   */
  @Test
  public void testCacheWriterExceptionAborts() throws CacheException {
    final String name = getUniqueName();
    final String exception = "EXCEPTION";

    TestCacheWriter writer = new TestCacheWriter() {
      private void handleEvent(Object argument) throws CacheWriterException {

        if (exception.equals(argument)) {
          String s = "Test Exception";
          throw new CacheWriterException(s);
        }
      }

      @Override
      public void beforeCreate2(EntryEvent event) throws CacheWriterException {

        handleEvent(event.getCallbackArgument());
      }

      @Override
      public void beforeUpdate2(EntryEvent event) throws CacheWriterException {

        handleEvent(event.getCallbackArgument());
      }

      @Override
      public void beforeDestroy2(EntryEvent event) throws CacheWriterException {

        handleEvent(event.getCallbackArgument());
      }

      @Override
      public void beforeRegionDestroy2(RegionEvent event) throws CacheWriterException {

        handleEvent(event.getCallbackArgument());
      }
    };

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setCacheWriter(writer);
    RegionAttributes attrs = factory.create();
    Region region;

    region = createRegion(name, attrs);
    Object value = 42;

    String p1 = "Test Exception";
    getCache().getLogger().info("<ExpectedException action=add>" + p1 + "</ExpectedException>");
    try {
      region.put(name, value, exception);
      fail("Should have thrown a CacheWriterException");

    } catch (CacheWriterException ex) {
      assertNull(region.getEntry(name));
    } finally {
      getCache().getLogger()
          .info("<ExpectedException action=remove>" + p1 + "</ExpectedException>");
    }

    region.put(name, value);

    try {
      region.put(name, "NEVER SEEN", exception);
      fail("Should have thrown a CacheWriterException");

    } catch (CacheWriterException ex) {
      Region.Entry entry = region.getEntry(name);
      assertNotNull(entry);
      assertEquals(value, entry.getValue());
    }

    try {
      region.destroy(name, exception);
      fail("Should have thrown a CacheWriterException");

    } catch (CacheWriterException ex) {
      Region.Entry entry = region.getEntry(name);
      assertNotNull(entry);
      assertEquals(value, entry.getValue());
    }

    try {
      region.destroyRegion(exception);

    } catch (CacheWriterException ex) {
      assertTrue(!region.isDestroyed());
      assertNotNull(region.getParentRegion().getSubregion(name));
    }
  }

}
