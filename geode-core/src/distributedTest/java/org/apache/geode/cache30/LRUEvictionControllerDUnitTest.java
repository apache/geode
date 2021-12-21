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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.eviction.EvictionCounters;
import org.apache.geode.internal.cache.eviction.HeapEvictor;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/**
 * Tests the basic functionality of the lru eviction controller and its statistics.
 *
 *
 * @since GemFire 3.2
 */

public class LRUEvictionControllerDUnitTest extends JUnit4CacheTestCase {

  private static boolean usingMain = false;

  /**
   * Creates a new <code>LRUEvictionControllerDUnitTest</code>
   */
  public LRUEvictionControllerDUnitTest() {
    super();
  }

  /**
   * Returns the <code>EvictionStatistics</code> for the given region
   */
  private EvictionCounters getLRUStats(Region region) {
    final LocalRegion l = (LocalRegion) region;
    return l.getEvictionController().getCounters();
  }

  //////// Test Methods

  /**
   * Carefully verifies that region operations effect the {@link EvictionCounters} as expected.
   */
  @Test
  public void testRegionOperations() throws CacheException {

    int threshold = 10;

    final String name = getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(threshold));

    Region region;
    if (usingMain) {
      DistributedSystem system = DistributedSystem.connect(new Properties());
      Cache cache = CacheFactory.create(system);
      region = cache.createRegion("Test", factory.create());

    } else {
      region = createRegion(name, factory.create());
    }

    EvictionCounters lruStats = getLRUStats(region);
    assertNotNull(lruStats);

    for (int i = 1; i <= 10; i++) {
      Object key = new Integer(i);
      Object value = String.valueOf(i);
      region.put(key, value);
      assertEquals(i, lruStats.getCounter());
      assertEquals(0, lruStats.getEvictions());
    }

    for (int i = 11; i <= 20; i++) {
      Object key = new Integer(i);
      Object value = String.valueOf(i);
      region.put(key, value);
      assertEquals(10, lruStats.getCounter());
      assertEquals(i - 10, lruStats.getEvictions());
    }

  }

  /**
   * Carefully verifies that region operations effect the {@link EvictionCounters} as expected in
   * the presense of a {@link CacheLoader}.
   */
  @Test
  public void testCacheLoader() throws CacheException {

    int threshold = 10;

    final String name = getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(threshold));
    factory.setCacheLoader(new CacheLoader() {
      @Override
      public Object load(LoaderHelper helper) throws CacheLoaderException {
        return "LOADED VALUE";
      }

      @Override
      public void close() {}

    });

    Region region;
    if (usingMain) {
      DistributedSystem system = DistributedSystem.connect(new Properties());
      Cache cache = CacheFactory.create(system);
      region = cache.createRegion("Test", factory.create());

    } else {
      region = createRegion(name, factory.create());
    }

    EvictionCounters lruStats = getLRUStats(region);
    assertNotNull(lruStats);

    for (int i = 1; i <= 10; i++) {
      Object key = new Integer(i);
      Object value = String.valueOf(i);
      region.put(key, value);
      assertEquals(i, lruStats.getCounter());
      assertEquals(0, lruStats.getEvictions());
    }

    for (int i = 11; i <= 20; i++) {
      Object key = new Integer(i);
      // Object value = String.valueOf(i);
      // Invoke loader
      region.get(key);
      assertEquals(10, lruStats.getCounter());
      assertEquals(i - 10, lruStats.getEvictions());
    }

  }

  /**
   * Tests an <code>LRUCapacityController</code> of size 1.
   */
  @Test
  public void testSizeOne() throws CacheException {
    int threshold = 1;

    final String name = getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setOffHeap(isOffHeapEnabled());
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(threshold));

    factory.setCacheLoader(new CacheLoader() {
      @Override
      public Object load(LoaderHelper helper) throws CacheLoaderException {
        return "LOADED VALUE";
      }

      @Override
      public void close() {}

    });

    Region region;
    if (usingMain) {
      DistributedSystem system = DistributedSystem.connect(new Properties());
      Cache cache = CacheFactory.create(system);
      region = cache.createRegion("Test", factory.create());

    } else {
      region = createRegion(name, factory.create());
    }

    EvictionCounters lruStats = getLRUStats(region);
    assertNotNull(lruStats);

    for (int i = 1; i <= 1; i++) {
      Object key = new Integer(i);
      Object value = String.valueOf(i);
      region.put(key, value);
      assertEquals(1, lruStats.getCounter());
      assertEquals(0, lruStats.getEvictions());
    }

    for (int i = 2; i <= 10; i++) {
      Object key = new Integer(i);
      Object value = String.valueOf(i);
      region.put(key, value);
      assertEquals(1, lruStats.getCounter());
      assertEquals(i - 1, lruStats.getEvictions());
    }

    for (int i = 11; i <= 20; i++) {
      Object key = new Integer(i);
      // Object value = String.valueOf(i);
      // Invoke loader
      region.get(key);
      assertEquals(1, lruStats.getCounter());
      assertEquals(i - 1, lruStats.getEvictions());
    }
  }

  /**
   * Tests that a Region with an LRU capacity controller can be accessed from inside a cache
   * listener.
   */
  @Test
  public void testBug31592() throws Exception {
    final String name = getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";
    final Object key2 = "KEY2";
    final Object value2 = "VALUE2";

    AttributesFactory factory = new AttributesFactory();
    factory.setOffHeap(isOffHeapEnabled());
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(10));
    final Region region = createRegion(name, factory.create());

    region.put(key, value);

    final Throwable[] errors = new Throwable[1];
    region.getAttributesMutator().addCacheListener(new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {
        try {
          LogWriterUtils.getLogWriter().info("AFTER CREATE");
          region.put(key, value2);

        } catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        } catch (Throwable ex) {
          region.getCache().getLogger().severe("failed to access cache from listener", ex);
          errors[0] = ex;
        }
      }
    });
    region.put(key2, value2);

    assertNull(errors[0]);
    assertEquals(value2, region.get(key));
    assertEquals(value2, region.get(key2));
  }


  /**
   * Tests that a capacity controller with LOCAL_DESTROY eviction action cannot be installed into a
   * region
   */
  @Test
  public void testCCMirrored() throws Exception {
    final String name = getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setOffHeap(isOffHeapEnabled());
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(10));
    factory.setDataPolicy(DataPolicy.REPLICATE);

    Region r = createRegion(name, factory.create());
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PRELOADED, ra.getDataPolicy());
    assertEquals(new SubscriptionAttributes(InterestPolicy.ALL), ra.getSubscriptionAttributes());
    r.destroyRegion();
  }

  /**
   * Create two regions, one a "feed" that performs transactions which are replicated to a region
   * with an Entry LRU set to one Asserts that the LRU rules are observed
   *
   */
  @Test
  public void testReplicationAndTransactions() throws Exception {
    final String r1 = getUniqueName() + "-1";
    final String r2 = getUniqueName() + "-2";
    final String r3 = getUniqueName() + "-3";

    VM feeder = Host.getHost(0).getVM(3);
    VM repl = Host.getHost(0).getVM(2);

    final int maxEntries = 1;
    final int numEntries = 10000;
    final int txBatchSize = 10;
    assertTrue(numEntries > txBatchSize); // need at least one batch

    CacheSerializableRunnable createRegion =
        new CacheSerializableRunnable("Create Replicate Region") {
          @Override
          public void run2() throws CacheException {
            AttributesFactory factory = new AttributesFactory();
            factory.setOffHeap(isOffHeapEnabled());
            factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maxEntries,
                EvictionAction.OVERFLOW_TO_DISK));
            factory.setDataPolicy(DataPolicy.REPLICATE);

            File[] diskDirs = new File[1];
            diskDirs[0] = new File("overflowDir/" + OSProcess.getId());
            diskDirs[0].mkdirs();
            factory.setDiskStoreName(getCache().createDiskStoreFactory().setDiskDirs(diskDirs)
                .create("LRUEvictionControllerDUnitTest").getName());
            factory.setDiskSynchronous(true);

            factory.setScope(Scope.DISTRIBUTED_ACK);
            RegionAttributes a = factory.create();
            createRegion(r1, a);
            createRegion(r2, a);
            createRegion(r3, a);
          }
        };
    feeder.invoke(createRegion);
    repl.invoke(createRegion);

    feeder.invoke(new CacheSerializableRunnable(
        "put " + numEntries + " entries and assert " + maxEntries + " max entries") {
      @Override
      public void run2() throws CacheException {
        Cache c = getCache();
        CacheTransactionManager txm = c.getCacheTransactionManager();

        Region reg1 = getRootRegion().getSubregion(r1);
        assertNotNull(reg1);

        Region reg2 = getRootRegion().getSubregion(r2);
        assertNotNull(reg2);

        Region reg3 = getRootRegion().getSubregion(r3);
        assertNotNull(reg3);

        boolean startTx = false;
        final Region[] r = {reg1, reg2, reg3};
        for (int i = 0; i < numEntries; i++) {
          if (i % txBatchSize == 0) {
            txm.begin();
            startTx = true;
          }
          reg1.create("r1-key-" + i, "r1-value-" + i);
          reg2.create("r2-key-" + i, "r2-value-" + i);
          reg3.create("r3-key-" + i, "r3-value-" + i);

          if (i % txBatchSize == (txBatchSize - 1)) {
            txm.commit();
            try { // allow stats to get a sample in
              Thread.sleep(20);
            } catch (InterruptedException ie) {
              fail("interrupted");
            }
            startTx = false;
          }
        }
        if (startTx) {
          txm.commit();
        }

        for (int i = 0; i < r.length; i++) {
          assertEquals(numEntries, r[i].size());
          {
            LocalRegion lr = (LocalRegion) r[i];
            assertEquals(maxEntries, lr.getEvictionController().getCounters().getLimit());
            assertEquals(maxEntries, lr.getEvictionController().getCounters().getCounter());
          }
        }
      }
    });

    repl.invoke(new CacheSerializableRunnable("Replicate asserts " + maxEntries + " max entries") {
      @Override
      public void run2() throws CacheException {
        getCache();
        Region reg1 = getRootRegion().getSubregion(r1);
        Region reg2 = getRootRegion().getSubregion(r2);
        Region reg3 = getRootRegion().getSubregion(r3);

        final Region[] r = {reg1, reg2, reg3};
        for (int i = 0; i < r.length; i++) {
          assertNotNull(r[i]);
          assertEquals(numEntries, r[i].size());
          {
            LocalRegion lr = (LocalRegion) r[i];
            assertEquals(maxEntries, lr.getEvictionController().getCounters().getLimit());
            assertEquals(maxEntries, lr.getEvictionController().getCounters().getCounter());
          }
        }

      }
    });
  }

  protected boolean isOffHeapEnabled() {
    return false;
  }

  protected HeapEvictor getEvictor() {
    return getCache().getHeapEvictor();
  }

  protected ResourceType getResourceType() {
    return ResourceType.HEAP_MEMORY;
  }

  public static void main(String[] args) throws Exception {
    usingMain = true;
    (new LRUEvictionControllerDUnitTest()).testSizeOne();
  }

}
