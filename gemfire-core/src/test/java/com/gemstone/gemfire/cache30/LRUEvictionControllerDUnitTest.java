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

import java.io.File;
import java.util.Properties;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceType;
import com.gemstone.gemfire.internal.cache.lru.HeapEvictor;
import com.gemstone.gemfire.internal.cache.lru.LRUStatistics;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Tests the basic functionality of the lru eviction 
 * controller and its statistics.
 *
 * @author David Whitlock
 *
 * @since 3.2
 */
public class LRUEvictionControllerDUnitTest extends CacheTestCase {

  private static boolean usingMain = false;

  /**
   * Creates a new <code>LRUEvictionControllerDUnitTest</code>
   */
  public LRUEvictionControllerDUnitTest(String name) {
    super(name);
  }

  /**
   * Returns the <code>LRUStatistics</code> for the given region
   */
  private LRUStatistics getLRUStats(Region region) {
    final LocalRegion l = (LocalRegion) region;
    return l.getEvictionController().getLRUHelper().getStats();
  }

  ////////  Test Methods

  /**
   * Carefully verifies that region operations effect the {@link
   * LRUStatistics} as expected.
   */
  public void testRegionOperations() throws CacheException {

    int threshold = 10;

    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(threshold));

    Region region;
    if (usingMain) {
      DistributedSystem system =
        DistributedSystem.connect(new Properties());
      Cache cache = CacheFactory.create(system);
      region = cache.createRegion("Test",
                                    factory.create());

    } else {
      region = createRegion(name, factory.create());
    }

    LRUStatistics lruStats = getLRUStats(region);
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
   * Carefully verifies that region operations effect the {@link
   * LRUStatistics} as expected in the presense of a {@link
   * CacheLoader}.
   */
  public void testCacheLoader() throws CacheException {

    int threshold = 10;

    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(threshold));
    factory.setCacheLoader(new CacheLoader() {
        public Object load(LoaderHelper helper)
          throws CacheLoaderException {
          return "LOADED VALUE";
        }

        public void close() { }

      });

    Region region;
    if (usingMain) {
      DistributedSystem system =
        DistributedSystem.connect(new Properties());
      Cache cache = CacheFactory.create(system);
      region = cache.createRegion("Test",
                                    factory.create());

    } else {
      region = createRegion(name, factory.create());
    }

    LRUStatistics lruStats = getLRUStats(region);
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
//      Object value = String.valueOf(i);
      // Invoke loader
      region.get(key);
      assertEquals(10, lruStats.getCounter());
      assertEquals(i - 10, lruStats.getEvictions());
    }

  }

  /**
   * Tests an <code>LRUCapacityController</code> of size 1.
   */
  public void testSizeOne() throws CacheException {
    int threshold = 1;

    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setOffHeap(isOffHeapEnabled());
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(threshold));
    
    factory.setCacheLoader(new CacheLoader() {
        public Object load(LoaderHelper helper)
          throws CacheLoaderException {
          return "LOADED VALUE";
        }

        public void close() { }

      });

    Region region;
    if (usingMain) {
      DistributedSystem system =
        DistributedSystem.connect(new Properties());
      Cache cache = CacheFactory.create(system);
      region = cache.createRegion("Test",
                                    factory.create());

    } else {
      region = createRegion(name, factory.create());
    }

    LRUStatistics lruStats = getLRUStats(region);
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
//      Object value = String.valueOf(i);
      // Invoke loader
      region.get(key);
      assertEquals(1, lruStats.getCounter());
      assertEquals(i - 1, lruStats.getEvictions());
    }
  }

  /**
   * Tests that a single set of eviction attributes can be used multiple times
   * (and does the right thing).
   */
  public void testMultipleUsesOfEvictionAttributes()
    throws CacheException, CloneNotSupportedException {

    int threshold = 42;

    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(threshold));
    Region region =
      createRegion(name, factory.create());

    RegionAttributes ra = region.getAttributes();
    Region r2 = createRegion(name + 2, ra);
    
    factory = new AttributesFactory(ra);
    Region r3 = createRegion(name + 3, factory.create());
    
    assertEquals(region.getAttributes().getEvictionAttributes(), r2.getAttributes().getEvictionAttributes());
    assertEquals(r2.getAttributes().getEvictionAttributes(), r3.getAttributes().getEvictionAttributes());
    {
      LocalRegion lRegion = (LocalRegion) region;
      LocalRegion lr2  = (LocalRegion) r2;
      LocalRegion lr3  = (LocalRegion) r3;
      assertNotSame(lRegion.getEvictionController(), lr2.getEvictionController());
      assertEquals(lRegion.getEvictionController(), lr2.getEvictionController());
      assertNotSame(lr2.getEvictionController(), lr3.getEvictionController());
      assertEquals(lr2.getEvictionController(), lr3.getEvictionController());
    }
  }

  /**
   * Tests that a Region with an LRU capacity controller can be
   * accessed from inside a cache listener.
   */
  public void testBug31592() throws Exception {
    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";
    final Object key2 = "KEY2";
    final Object value2 = "VALUE2";

    AttributesFactory factory = new AttributesFactory();
    factory.setOffHeap(isOffHeapEnabled());
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(10));
    final Region region =
      createRegion(name, factory.create());

    region.put(key, value);

    final Throwable[] errors = new Throwable[1];
    region.getAttributesMutator().addCacheListener(new
      CacheListenerAdapter() {
        public void afterCreate(EntryEvent event) {
          try {
            getLogWriter().info("AFTER CREATE");
            region.put(key, value2);

          } 
          catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          }
          catch (Throwable ex) {
            region.getCache().getLogger().severe(
                "failed to access cache from listener", ex);
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
   * Tests that a capacity controller with LOCAL_DESTROY eviction action
   * cannot be installed into a region
   */
  public void testCCMirrored() throws Exception {
    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setOffHeap(isOffHeapEnabled());
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(10));
    factory.setDataPolicy(DataPolicy.REPLICATE);
    
    Region r = createRegion(name, factory.create());
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PRELOADED, ra.getDataPolicy());
    assertEquals(new SubscriptionAttributes(InterestPolicy.ALL),
                 ra.getSubscriptionAttributes());
    r.destroyRegion();
  }
  
  /**
   * Create two regions, one a "feed" that performs  transactions
   * which are replicated to a region with an Entry LRU set to one
   * Asserts that the LRU rules are observed
   * @throws Exception
   */
  public void testReplicationAndTransactions() throws Exception {
    final String r1 = this.getUniqueName() + "-1";
    final String r2 = this.getUniqueName() + "-2";
    final String r3 = this.getUniqueName() + "-3";
    
    VM feeder = Host.getHost(0).getVM(3); 
    VM repl = Host.getHost(0).getVM(2);
    
    final int maxEntries = 1;
    final int numEntries = 10000;
    final int txBatchSize = 10;
    assertTrue(numEntries > txBatchSize);  // need at least one batch
    
    CacheSerializableRunnable createRegion = 
      new CacheSerializableRunnable("Create Replicate Region") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setOffHeap(isOffHeapEnabled());
        factory.setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(maxEntries, 
                EvictionAction.OVERFLOW_TO_DISK));
        factory.setDataPolicy(DataPolicy.REPLICATE);
        
        File[] diskDirs = new File[1];
        diskDirs[0] = new File("overflowDir/" + OSProcess.getId());
        diskDirs[0].mkdirs();
        factory.setDiskStoreName(getCache().createDiskStoreFactory()
                                 .setDiskDirs(diskDirs)
                                 .create("LRUEvictionControllerDUnitTest")
                                 .getName());
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
    
    feeder.invoke(new CacheSerializableRunnable("put " + numEntries + 
        " entries and assert " + maxEntries + " max entries") {
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
        for (int i=0; i<numEntries; i++){
          if (i%txBatchSize == 0) {
            txm.begin();
            startTx = true;
          }
          reg1.create("r1-key-" + i, "r1-value-" +i);
          reg2.create("r2-key-" + i, "r2-value-" +i);
          reg3.create("r3-key-" + i, "r3-value-" +i);

          if (i%txBatchSize == (txBatchSize-1)) {
            txm.commit();
            try { // allow stats to get a sample in 
              Thread.sleep(20);
            } catch (InterruptedException ie){
              fail("interrupted");
            }
            startTx = false;
          }
        }
        if (startTx) {
          txm.commit();
        }
        
        for(int i =0; i<r.length; i++) {
          assertEquals(numEntries, r[i].size());
          { 
            LocalRegion lr = (LocalRegion) r[i];
            assertEquals(maxEntries, lr.getEvictionController().getLRUHelper().getStats().getLimit());
            assertEquals(maxEntries, lr.getEvictionController().getLRUHelper().getStats().getCounter());
          }
        }
      }
    });

    repl.invoke(new CacheSerializableRunnable("Replicate asserts " + 
        maxEntries + " max entries" ) {
      public void run2() throws CacheException {
        getCache();
        Region reg1 = getRootRegion().getSubregion(r1);
        Region reg2 = getRootRegion().getSubregion(r2);
        Region reg3 = getRootRegion().getSubregion(r3);

        final Region[] r = {reg1, reg2, reg3};
        for(int i =0; i<r.length; i++) {
          assertNotNull(r[i]);
          assertEquals(numEntries, r[i].size());
          { 
            LocalRegion lr = (LocalRegion) r[i];
            assertEquals(maxEntries, lr.getEvictionController().getLRUHelper().getStats().getLimit());
            assertEquals(maxEntries, lr.getEvictionController().getLRUHelper().getStats().getCounter());
          }
        }

      }
    });
  }

  protected boolean isOffHeapEnabled() {
    return false;
  }

  protected HeapEvictor getEvictor() {
    return ((GemFireCacheImpl) getCache()).getHeapEvictor();
  }
  
  protected ResourceType getResourceType() {
    return ResourceType.HEAP_MEMORY;
  }
  
  public static void main(String[] args) throws Exception {
    usingMain = true;
    (new LRUEvictionControllerDUnitTest("test")).testSizeOne();
  }

}
