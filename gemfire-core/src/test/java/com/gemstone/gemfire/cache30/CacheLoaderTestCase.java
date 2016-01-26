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
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.TimeoutException;

/**
 * An abstract class whose test methods test the functionality of
 * {@link CacheLoader}s that are invoked locally.
 *
 * @see MultiVMRegionTestCase#testRemoteCacheLoader
 *
 * @author David Whitlock
 * @since 3.0
 */
public abstract class CacheLoaderTestCase
  extends CacheWriterTestCase {

  public CacheLoaderTestCase(String name) {
    super(name);
  }

  ///////////////////////  Test Methods  ///////////////////////

  public void testCacheLoader() throws CacheException {
    final String name = this.getUniqueName();
    final Object key = this.getUniqueName();
    final Object value = new Integer(42);
    final Object arg = "ARG";
    final String exception = "EXCEPTION";

    TestCacheLoader loader = new TestCacheLoader() {
        public Object load2(LoaderHelper helper)
          throws CacheLoaderException {

          assertEquals(key, helper.getKey());
          assertEquals(name, helper.getRegion().getName());

          try {
            RegionAttributes attrs =
              helper.getRegion().getAttributes();
            if (attrs.getScope().isDistributed()) {
              assertNull(helper.netSearch(false));
              assertNull(helper.netSearch(true));
            }

          } catch (TimeoutException ex) {
            fail("Why did I time out?", ex);
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

    AttributesFactory factory =
      new AttributesFactory(getRegionAttributes());
    factory.setCacheLoader(loader);
    Region region =
      createRegion(name, factory.create());
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
  
  
//  public void testCacheLoaderWithNetSearch() throws CacheException {
//    final String name = this.getUniqueName();
//    final Object key = this.getUniqueName();
//    final Object value = new Integer(42);
//    final Object arg = "ARG";
//    final String exception = "EXCEPTION";
//
//    final TestCacheLoader loader = new TestCacheLoader() {
//        public Object load2(LoaderHelper helper)
//          throws CacheLoaderException {
//
//          assertEquals(key, helper.getKey());
//          assertEquals(name, helper.getRegion().getName());
//
//          try {
//            RegionAttributes attrs =
//              helper.getRegion().getAttributes();
//            if (attrs.getScope().isDistributed()) {
//              Object result = helper.netSearch(false);
//              assertEquals(value, result);
//              return result;
//            }
//
//          } catch (TimeoutException ex) {
//            fail("Why did I time out?", ex);
//          }
//          return value;
//        }
//      };
//
//    Region region =
//      createRegion(name);
//    loader.wasInvoked();
//
//    Region.Entry entry = region.getEntry(key);
//    assertNull(entry);
//    region.create(key, null);
//
//    entry = region.getEntry(key);
//    assertNotNull(entry);
//    assertNull(entry.getValue());
//   
//    Host host = Host.getHost(0);
//    VM vm0 = host.getVM(0);
//    vm0.invoke(new CacheSerializableRunnable("set remote value") {
//      public void run2() throws CacheException {
////        final TestCacheLoader remoteloader = new TestCacheLoader() {
////            public Object load2(LoaderHelper helper)
////              throws CacheLoaderException {
////
////              assertEquals(key, helper.getKey());
////              assertEquals(name, helper.getRegion().getName());
////              return value;
////            }
////          };
////        
////        AttributesFactory factory =
////          new AttributesFactory(getRegionAttributes());
////        factory.setCacheLoader(remoteloader);
//        Region rgn = createRegion(name);
//        rgn.put(key, value);
//        flushIfNecessary(rgn);
//      }
//    });
//
//
//    assertEquals(value, region.get(key));
//    assertTrue(loader.wasInvoked());
//    assertEquals(value, region.getEntry(key).getValue());
//  }

  /**
   * Tests what happens when a {@link CacheLoader} returns
   * <code>null</code> from its {@link CacheLoader#load load} method.
   */
  public void testCacheLoaderNull() throws CacheException {
    TestCacheLoader loader = new TestCacheLoader() {
        public Object load2(LoaderHelper helper)
          throws CacheLoaderException {

          return null;
        }
      };

    AttributesFactory factory =
      new AttributesFactory(getRegionAttributes());
    factory.setCacheLoader(loader);
    String name = this.getUniqueName();
    Region region =
      createRegion(name, factory.create());
    loader.wasInvoked();

    assertNull(region.get("KEY"));
    assertTrue(loader.wasInvoked());
  }

  /**
   * Tests that a <code>CacheWriter</code> gets invoked on a
   * <code>load</code>.
   */
  public void testCacheWriterOnLoad() throws CacheException {
    final String name = this.getUniqueName();
    final Object key = this.getUniqueName();
    final Object oldValue = new Integer(42);
    final Object newValue = new Integer(43);

    TestCacheLoader loader = new TestCacheLoader() {
        public Object load2(LoaderHelper helper)
          throws CacheLoaderException {
          return oldValue;
        }
      };

    TestCacheWriter writer = new TestCacheWriter() {
        public void beforeCreate2(EntryEvent event)
          throws CacheWriterException {

          assertEquals(oldValue, event.getNewValue());
          assertTrue(event.isLoad());
          assertTrue(event.isLocalLoad());
          assertFalse(event.isNetLoad());
          assertFalse(event.isNetSearch());
        }
      };

    AttributesFactory factory =
      new AttributesFactory(getRegionAttributes());
    factory.setCacheLoader(loader);
    factory.setCacheWriter(writer);
    Region region =
      createRegion(name, factory.create());
    loader.wasInvoked();

    assertEquals(oldValue, region.get(key));
    assertTrue(loader.wasInvoked());
    assertTrue(writer.wasInvoked());

    writer = new TestCacheWriter() {
        public void beforeUpdate2(EntryEvent event)
          throws CacheWriterException {

          assertEquals(oldValue, event.getOldValue());
          assertEquals(newValue, event.getNewValue());
          assertFalse(event.isLoad());
          assertFalse(event.isLocalLoad());
          assertFalse(event.isNetLoad());
          assertFalse(event.isNetSearch());
        }
      };

    region.getAttributesMutator().setCacheWriter(writer);

    region.put(key, newValue);
    assertFalse(loader.wasInvoked());
    assertTrue(writer.wasInvoked());
  }

  /**
   * Tests that a <code>CacheListener</code> gets invoked on a
   * <code>load</code>.
   */
  public void testCacheListenerOnLoad()
    throws CacheException, InterruptedException {

    final String name = this.getUniqueName();
    final Object key = this.getUniqueName();
    final Object oldValue = new Integer(42);
    final Object newValue = new Integer(43);

    TestCacheLoader loader = new TestCacheLoader() {
        public Object load2(LoaderHelper helper)
          throws CacheLoaderException {
          return oldValue;
        }
      };

    TestCacheListener listener = new TestCacheListener() {
        public void afterCreate2(EntryEvent event) {
          assertEquals(oldValue, event.getNewValue());
          assertTrue(event.isLoad());
          assertTrue(event.isLocalLoad());
          assertFalse(event.isNetLoad());
          assertFalse(event.isNetSearch());
        }
      };

    AttributesFactory factory =
      new AttributesFactory(getRegionAttributes());
    factory.setCacheLoader(loader);
    factory.setCacheListener(listener);
    Region region =
      createRegion(name, factory.create());
    loader.wasInvoked();

    assertEquals(oldValue, region.get(key));
    assertTrue(loader.wasInvoked());
    pause(500);
    assertTrue(listener.wasInvoked());

    listener = new TestCacheListener() {
        public void afterUpdate2(EntryEvent event) {
          assertEquals(oldValue, event.getOldValue());
          assertEquals(newValue, event.getNewValue());
          assertFalse(event.isLoad());
          assertFalse(event.isLocalLoad());
          assertFalse(event.isNetLoad());
          assertFalse(event.isNetSearch());
        }
      };

    region.getAttributesMutator().setCacheListener(listener);

    region.put(key, newValue);
    pause(500);
    assertFalse(loader.wasInvoked());
    assertTrue(listener.wasInvoked());
  }

}
