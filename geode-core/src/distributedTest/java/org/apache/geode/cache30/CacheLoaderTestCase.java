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

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Wait;

/**
 * An abstract class whose test methods test the functionality of {@link CacheLoader}s that are
 * invoked locally.
 *
 * @see MultiVMRegionTestCase#testRemoteCacheLoader
 *
 * @since GemFire 3.0
 */
public abstract class CacheLoaderTestCase extends CacheWriterTestCase {

  public CacheLoaderTestCase() {
    super();
  }

  /////////////////////// Test Methods ///////////////////////

  @Test
  public void testCacheLoader() throws CacheException {
    final String name = getUniqueName();
    final Object key = getUniqueName();
    final Object value = new Integer(42);
    final Object arg = "ARG";
    final String exception = "EXCEPTION";

    TestCacheLoader loader = new TestCacheLoader() {
      @Override
      public Object load2(LoaderHelper helper) throws CacheLoaderException {

        assertEquals(key, helper.getKey());
        assertEquals(name, helper.getRegion().getName());

        try {
          RegionAttributes attrs = helper.getRegion().getAttributes();
          if (attrs.getScope().isDistributed()) {
            assertNull(helper.netSearch(false));
            assertNull(helper.netSearch(true));
          }

        } catch (TimeoutException ex) {
          Assert.fail("Why did I time out?", ex);
        }

        Object argument = helper.getArgument();
        if (argument != null) {
          if (argument.equals(exception)) {
            String s = "Test Exception";
            throw new CacheLoaderException(s);

          } else {
            assertEquals(arg, argument);
          }
        }

        return value;
      }
    };

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setCacheLoader(loader);
    Region region = createRegion(name, factory.create());
    loader.wasInvoked();

    Region.Entry entry = region.getEntry(key);
    assertNull(entry);
    region.create(key, null);

    entry = region.getEntry(key);
    assertNotNull(entry);
    assertNull(entry.getValue());

    assertEquals(value, region.get(key));
    assertTrue(loader.wasInvoked());
    assertEquals(value, region.getEntry(key).getValue());
  }


  // public void testCacheLoaderWithNetSearch() throws CacheException {
  // final String name = this.getUniqueName();
  // final Object key = this.getUniqueName();
  // final Object value = new Integer(42);
  // final Object arg = "ARG";
  // final String exception = "EXCEPTION";
  //
  // final TestCacheLoader loader = new TestCacheLoader() {
  // public Object load2(LoaderHelper helper)
  // throws CacheLoaderException {
  //
  // assertIndexDetailsEquals(key, helper.getKey());
  // assertIndexDetailsEquals(name, helper.getRegion().getName());
  //
  // try {
  // RegionAttributes attrs =
  // helper.getRegion().getAttributes();
  // if (attrs.getScope().isDistributed()) {
  // Object result = helper.netSearch(false);
  // assertIndexDetailsEquals(value, result);
  // return result;
  // }
  //
  // } catch (TimeoutException ex) {
  // fail("Why did I time out?", ex);
  // }
  // return value;
  // }
  // };
  //
  // Region region =
  // createRegion(name);
  // loader.wasInvoked();
  //
  // Region.Entry entry = region.getEntry(key);
  // assertNull(entry);
  // region.create(key, null);
  //
  // entry = region.getEntry(key);
  // assertNotNull(entry);
  // assertNull(entry.getValue());
  //
  // Host host = Host.getHost(0);
  // VM vm0 = host.getVM(0);
  // vm0.invoke(new CacheSerializableRunnable("set remote value") {
  // public void run2() throws CacheException {
  //// final TestCacheLoader remoteloader = new TestCacheLoader() {
  //// public Object load2(LoaderHelper helper)
  //// throws CacheLoaderException {
  ////
  //// assertIndexDetailsEquals(key, helper.getKey());
  //// assertIndexDetailsEquals(name, helper.getRegion().getName());
  //// return value;
  //// }
  //// };
  ////
  //// AttributesFactory factory =
  //// new AttributesFactory(getRegionAttributes());
  //// factory.setCacheLoader(remoteloader);
  // Region rgn = createRegion(name);
  // rgn.put(key, value);
  // flushIfNecessary(rgn);
  // }
  // });
  //
  //
  // assertIndexDetailsEquals(value, region.get(key));
  // assertTrue(loader.wasInvoked());
  // assertIndexDetailsEquals(value, region.getEntry(key).getValue());
  // }

  /**
   * Tests what happens when a {@link CacheLoader} returns <code>null</code> from its
   * {@link CacheLoader#load load} method.
   */
  @Test
  public void testCacheLoaderNull() throws CacheException {
    TestCacheLoader loader = new TestCacheLoader() {
      @Override
      public Object load2(LoaderHelper helper) throws CacheLoaderException {

        return null;
      }
    };

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setCacheLoader(loader);
    String name = getUniqueName();
    Region region = createRegion(name, factory.create());
    loader.wasInvoked();

    assertNull(region.get("KEY"));
    assertTrue(loader.wasInvoked());
  }

  /**
   * Tests that a <code>CacheWriter</code> gets invoked on a <code>load</code>.
   */
  @Test
  public void testCacheWriterOnLoad() throws CacheException {
    final String name = getUniqueName();
    final Object key = getUniqueName();
    final Object oldValue = new Integer(42);
    final Object newValue = new Integer(43);

    TestCacheLoader loader = new TestCacheLoader() {
      @Override
      public Object load2(LoaderHelper helper) throws CacheLoaderException {
        return oldValue;
      }
    };

    TestCacheWriter writer = new TestCacheWriter() {
      @Override
      public void beforeCreate2(EntryEvent event) throws CacheWriterException {

        assertEquals(oldValue, event.getNewValue());
        assertTrue(event.getOperation().isLoad());
        assertTrue(event.getOperation().isLocalLoad());
        assertFalse(event.getOperation().isNetLoad());
        assertFalse(event.getOperation().isNetSearch());
      }
    };

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setCacheLoader(loader);
    factory.setCacheWriter(writer);
    Region region = createRegion(name, factory.create());
    loader.wasInvoked();

    assertEquals(oldValue, region.get(key));
    assertTrue(loader.wasInvoked());
    assertTrue(writer.wasInvoked());

    writer = new TestCacheWriter() {
      @Override
      public void beforeUpdate2(EntryEvent event) throws CacheWriterException {

        assertEquals(oldValue, event.getOldValue());
        assertEquals(newValue, event.getNewValue());
        assertFalse(event.getOperation().isLoad());
        assertFalse(event.getOperation().isLocalLoad());
        assertFalse(event.getOperation().isNetLoad());
        assertFalse(event.getOperation().isNetSearch());
      }
    };

    region.getAttributesMutator().setCacheWriter(writer);

    region.put(key, newValue);
    assertFalse(loader.wasInvoked());
    assertTrue(writer.wasInvoked());
  }

  /**
   * Tests that a <code>CacheListener</code> gets invoked on a <code>load</code>.
   */
  @Test
  public void testCacheListenerOnLoad() throws CacheException, InterruptedException {

    final String name = getUniqueName();
    final Object key = getUniqueName();
    final Object oldValue = new Integer(42);
    final Object newValue = new Integer(43);

    TestCacheLoader loader = new TestCacheLoader() {
      @Override
      public Object load2(LoaderHelper helper) throws CacheLoaderException {
        return oldValue;
      }
    };

    TestCacheListener listener = new TestCacheListener() {
      @Override
      public void afterCreate2(EntryEvent event) {
        assertEquals(oldValue, event.getNewValue());
        assertTrue(event.getOperation().isLoad());
        assertTrue(event.getOperation().isLocalLoad());
        assertFalse(event.getOperation().isNetLoad());
        assertFalse(event.getOperation().isNetSearch());
      }
    };

    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    factory.setCacheLoader(loader);
    factory.setCacheListener(listener);
    Region region = createRegion(name, factory.create());
    loader.wasInvoked();

    assertEquals(oldValue, region.get(key));
    assertTrue(loader.wasInvoked());
    Wait.pause(500);
    assertTrue(listener.wasInvoked());

    listener = new TestCacheListener() {
      @Override
      public void afterUpdate2(EntryEvent event) {
        assertEquals(oldValue, event.getOldValue());
        assertEquals(newValue, event.getNewValue());
        assertFalse(event.getOperation().isLoad());
        assertFalse(event.getOperation().isLocalLoad());
        assertFalse(event.getOperation().isNetLoad());
        assertFalse(event.getOperation().isNetSearch());
      }
    };

    region.getAttributesMutator().addCacheListener(listener);

    region.put(key, newValue);
    Wait.pause(500);
    assertFalse(loader.wasInvoked());
    assertTrue(listener.wasInvoked());
  }

}
