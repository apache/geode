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

import org.apache.geode.cache.*;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * TestCase that emulates the conditions that entry destroy with concurrent destroy region or cache
 * close event will get expected Exception.
 */
@Category(IntegrationTest.class)
public class DestroyEntryWithConcurrentOperationJUnitTest {
  /**
   * A region entry key.
   */
  private static final String KEY = "KEY";

  /**
   * A region entry value.
   */
  private static final String VALUE =
      " Vestibulum quis lobortis risus. Cras cursus eget dolor in facilisis. Curabitur purus arcu, dignissim ac lorem non, venenatis condimentum tellus. Praesent at erat dapibus, bibendum nunc sed, congue nulla";

  /**
   * A cache.
   */
  private GemFireCacheImpl cache = null;


  @Before
  public void setUp() throws Exception {
    // Create our cache
    this.cache = createCache();
  }

  @After
  public void tearDown() throws Exception {
    // Cleanup our cache
    closeCache(this.cache);
  }

  /**
   * @return the test's cache.
   */
  protected GemFireCacheImpl getCache() {
    return this.cache;
  }

  /**
   * Close a cache.
   * 
   * @param gfc the cache to close.
   */
  protected void closeCache(GemFireCacheImpl gfc) {
    gfc.close();
  }

  /**
   * @return the test's off heap memory size.
   */
  protected String getOffHeapMemorySize() {
    return "2m";
  }

  /**
   * @return the type of region for the test.
   */
  protected RegionShortcut getRegionShortcut() {
    return RegionShortcut.REPLICATE;
  }

  /**
   * @return the region containing our test data.
   */
  protected String getRegionName() {
    return "region1";
  }

  /**
   * Creates and returns the test region with concurrency checks enabled.
   */
  protected Region<Object, Object> createRegion(boolean isOffHeap) {
    return createRegion(true, isOffHeap);
  }

  /**
   * Creates and returns the test region.
   * 
   * @param concurrencyChecksEnabled concurrency checks will be enabled if true.
   */
  protected Region<Object, Object> createRegion(boolean concurrencyChecksEnabled,
      boolean isOffHeap) {
    return getCache().createRegionFactory(getRegionShortcut()).setOffHeap(isOffHeap)
        .setConcurrencyChecksEnabled(concurrencyChecksEnabled).create(getRegionName());
  }

  /**
   * Creates and returns the test cache.
   */
  protected GemFireCacheImpl createCache() {
    Properties props = new Properties();
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(ConfigurationProperties.OFF_HEAP_MEMORY_SIZE, getOffHeapMemorySize());
    GemFireCacheImpl result = (GemFireCacheImpl) new CacheFactory(props).create();
    return result;
  }

  @Test
  public void testEntryDestroyWithCacheClose() throws Exception {
    testEntryDestroyWithCacheClose(false);
  }

  @Test
  public void testOffHeapRegionEntryDestroyWithCacheClose() throws Exception {
    testEntryDestroyWithCacheClose(true);
  }

  /**
   * Simulates the conditions setting a test hook boolean in {@link AbstractRegionMap}. This test
   * hook forces a cache close during a destroy in a region. This test asserts that a
   * CacheClosedException is thrown rather than an EntryNotFoundException (or any other exception
   * type for that matter).
   */
  private void testEntryDestroyWithCacheClose(boolean isOffHeap) {
    AbstractRegionMap.testHookRunnableForConcurrentOperation = new Runnable() {
      @Override
      public void run() {
        getCache().close();
      }
    };

    // True when the correct exception has been triggered.
    boolean correctException = false;

    Region<Object, Object> region = createRegion(isOffHeap);
    region.put(KEY, VALUE);

    try {
      region.destroy(KEY);
    } catch (CacheClosedException e) {
      correctException = true;
      // e.printStackTrace();
    } catch (Exception e) {
      // e.printStackTrace();
      fail("Did not receive a CacheClosedException.  Received a " + e.getClass().getName()
          + " instead.");
    } finally {
      AbstractRegionMap.testHookRunnableForConcurrentOperation = null;
    }

    assertTrue("A CacheClosedException was not triggered", correctException);
  }

  @Test
  public void testEntryDestroyWithRegionDestroy() throws Exception {
    verifyConcurrentRegionDestroyWithEntryDestroy(false);
  }

  @Test
  public void testEntryDestroyWithOffHeapRegionDestroy() throws Exception {
    verifyConcurrentRegionDestroyWithEntryDestroy(true);
  }

  /**
   * Simulates the conditions by setting a test hook boolean in {@link AbstractRegionMap}. This test
   * hook forces a region destroy during a destroy operation in a region. This test asserts that a
   * RegionDestroyedException is thrown rather than an EntryNotFoundException (or any other
   * exception type for that matter).
   */
  private void verifyConcurrentRegionDestroyWithEntryDestroy(boolean isOffHeap) {
    AbstractRegionMap.testHookRunnableForConcurrentOperation = new Runnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(getRegionName());
        region.destroyRegion();
        assertTrue("Region " + getRegionName() + " is not destroyed.", region.isDestroyed());
      }
    };

    // True when the correct exception has been triggered.
    boolean correctException = false;

    Region<Object, Object> region = createRegion(isOffHeap);
    region.put(KEY, VALUE);

    try {
      region.destroy(KEY);
    } catch (RegionDestroyedException e) {
      correctException = true;
      // e.printStackTrace();
    } catch (Exception e) {
      // e.printStackTrace();
      fail("Did not receive a RegionDestroyedException.  Received a " + e.getClass().getName()
          + " instead.");
    } finally {
      AbstractRegionMap.testHookRunnableForConcurrentOperation = null;
    }

    assertTrue("A RegionDestroyedException was not triggered", correctException);
  }
}
