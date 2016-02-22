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
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.test.dunit.Assert;

/**
 * Tests the functionality of a {@link Scope#LOCAL locally scoped}
 * cache {@link Region} including its callbacks.  Note that even
 * though this test is a {@link com.gemstone.gemfire.test.dunit.DistributedTestCase}, it does
 * not perform any distribution.
 *
 * @author David Whitlock
 * @since 3.0
 */
public class LocalRegionDUnitTest extends CacheListenerTestCase {

  public LocalRegionDUnitTest(String name) {
    super(name);
  }

  /**
   * Returns the attributes of a region to be tested.
   */
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(false);
    return factory.create();
  }

  /////////////////  Local Region specific tests  /////////////////

  /**
   * Tests the compatibility of creating certain kinds of subregions
   * of a local region.
   *
   * @see Region#createSubregion
   */
  public void testIncompatibleSubregions() throws CacheException {
    Region region = createRegion(this.getUniqueName());
    assertEquals(Scope.LOCAL, region.getAttributes().getScope());

    // A region with Scope.LOCAL can only have subregions with
    // Scope.LOCAL.
    try {
      AttributesFactory factory =
        new AttributesFactory(region.getAttributes());
      factory.setScope(Scope.DISTRIBUTED_NO_ACK);
      RegionAttributes attrs = factory.create();
      region.createSubregion(this.getUniqueName(), attrs);
      fail("Should have thrown an IllegalStateException");

    } catch (IllegalStateException ex) {
      // pass...
    }

    try {
      AttributesFactory factory =
        new AttributesFactory(region.getAttributes());
      factory.setScope(Scope.DISTRIBUTED_ACK);
      RegionAttributes attrs = factory.create();
      region.createSubregion(this.getUniqueName(), attrs);
      fail("Should have thrown an IllegalStateException");

    } catch (IllegalStateException ex) {
      // pass...
    }

    try {
      AttributesFactory factory =
        new AttributesFactory(region.getAttributes());
      factory.setScope(Scope.GLOBAL);
      RegionAttributes attrs = factory.create();
      region.createSubregion(this.getUniqueName(), attrs);
      fail("Should have thrown an IllegalStateException");

    } catch (IllegalStateException ex) {
      // pass...
    }


  }

  /**
   * Tests that if a <code>CacheLoader</code> for a local region
   * invokes {@link LoaderHelper#netSearch}, a {@link
   * CacheLoaderException} is thrown.
   */
  public void testLocalLoaderNetSearch() throws CacheException {
    assertEquals(Scope.LOCAL, getRegionAttributes().getScope());

    final String name = this.getUniqueName();
    final Object key = this.getUniqueName();

    TestCacheLoader loader = new TestCacheLoader() {
        public Object load2(LoaderHelper helper)
          throws CacheLoaderException {

          try {
            helper.netSearch(true);

          } catch (TimeoutException ex) {
            Assert.fail("Why did I timeout?", ex);
          }

          return null;
        }
      };

    AttributesFactory factory =
      new AttributesFactory(getRegionAttributes());
    factory.setCacheLoader(loader);
    Region region =
      createRegion(name, factory.create());
    assertEquals(Scope.LOCAL, region.getAttributes().getScope());

    try {
      region.get(key);
      fail("Should have thrown a CacheLoaderException");

    } catch (CacheLoaderException ex) {
      String expected =
        com.gemstone.gemfire.internal.cache.LoaderHelperImpl.NET_SEARCH_LOCAL.toLocalizedString();
      String message = ex.getMessage();
      assertTrue("Unexpected message \"" + message + "\"",
                 message.indexOf(expected) != -1);
    }
  }

  /**
   * Tests that a local writer receives a modified version of the
   * callback argument on a create.
   */
  public void testLocalCreateModifiedCallbackArgument()
    throws CacheException {

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";
    final Object one = "ONE";
    final Object two = "TWO";

    TestCacheLoader loader = new TestCacheLoader() {
        public Object load2(LoaderHelper helper)
          throws CacheLoaderException {

          Object[] array = (Object[]) helper.getArgument();
          assertEquals(one, array[0]);
          array[0] = two;

          return value;
        }
      };

    TestCacheWriter writer = new TestCacheWriter() {
        public void beforeCreate2(EntryEvent event)
          throws CacheWriterException {

          Object[] array = (Object[]) event.getCallbackArgument();
          assertEquals(two, array[0]);
        }
      };

    AttributesFactory factory =
      new AttributesFactory(getRegionAttributes());
    factory.setCacheLoader(loader);
    factory.setCacheWriter(writer);
    Region region =
      createRegion(name, factory.create());

    Object[] array = new Object[] { one };
    assertEquals(value, region.get(key, array));
    assertTrue(loader.wasInvoked());
    assertTrue(writer.wasInvoked());
  }

  /**
   * Tests that a local writer receives a modified version of the
   * callback argument on an update.
   */
  public void testLocalUpdateModifiedCallbackArgument()
    throws CacheException {

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";
    final Object one = "ONE";
    final Object two = "TWO";

    TestCacheLoader loader = new TestCacheLoader() {
        public Object load2(LoaderHelper helper)
          throws CacheLoaderException {

          Object[] array = (Object[]) helper.getArgument();
          assertEquals(one, array[0]);
          array[0] = two;

          return value;
        }
      };

    TestCacheWriter writer = new TestCacheWriter() {
        public void beforeCreate2(EntryEvent event)
          throws CacheWriterException {

        }

        public void beforeUpdate2(EntryEvent event)
          throws CacheWriterException {

          Object[] array = (Object[]) event.getCallbackArgument();
          assertEquals(two, array[0]);
        }
      };

    AttributesFactory factory =
      new AttributesFactory(getRegionAttributes());
    factory.setCacheLoader(loader);
    factory.setCacheWriter(writer);
    Region region =
      createRegion(name, factory.create());

    region.create(key, null);
    assertFalse(loader.wasInvoked());
    assertTrue(writer.wasInvoked());

    Object[] array = new Object[] { one };
    assertEquals(value, region.get(key, array));
    assertTrue(loader.wasInvoked());
    assertTrue(writer.wasInvoked());
  }

  ////////  Expiration





}
