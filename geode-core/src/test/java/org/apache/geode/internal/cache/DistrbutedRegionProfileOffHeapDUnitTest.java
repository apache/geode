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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class DistrbutedRegionProfileOffHeapDUnitTest extends JUnit4CacheTestCase {

  @Override
  public final void preTearDownAssertions() throws Exception {
    SerializableRunnable checkOrphans = new SerializableRunnable() {

      @Override
      public void run() {
        if(hasCache()) {
          OffHeapTestUtil.checkOrphans();
        }
      }
    };
    Invoke.invokeInEveryVM(checkOrphans);
    checkOrphans.run();
  }

  /**
   * Asserts that creating a region on two members, with one member having the
   * region as on-heap and the other as having the region as off-heap, will
   * cause an exception and the region will not be created.
   */
  @Test
  public void testPartitionedRegionProfileWithConflict() throws Exception {
    final String regionName = getTestMethodName() + "Region";

    Host.getHost(0).getVM(0).invoke(new CacheSerializableRunnable("createRegionNoException") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
        Properties properties = new Properties();
        properties.put(OFF_HEAP_MEMORY_SIZE, "2m");
        getSystem(properties);

        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
        regionFactory.setOffHeap(true);
        Region region = regionFactory.create(regionName);
        
        assertNotNull("Region is null", region);
        assertNotNull("Cache does not contain region", cache.getRegion(regionName));
      }
    });

    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("createRegionException") {
      private static final long serialVersionUID = 1L;

      @SuppressWarnings("synthetic-access")
      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
        final GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        final RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
        Region region = null;

        try {
          IgnoredException.addIgnoredException("IllegalStateException");
          region = regionFactory.create(regionName);
          fail("Expected exception upon creation with invalid off-heap state");
        } catch (IllegalStateException expected) {
          // An exception is expected.
        } finally {
          removeExceptionTag1("IllegalStateException");
        }
        
        assertNull("Region is not null", region);
        assertNull("Cache contains region", cache.getRegion(regionName));
      }
    });
  }

  /**
   * Asserts that creating a region on two members, with both regions having the
   * same off-heap status, will not cause an exception and the region will be
   * created.
   */
  @Test
  public void testPartitionedRegionProfileWithoutConflict() throws Exception {
    final String offHeapRegionName = getTestMethodName() + "OffHeapRegion";
    final String onHeapRegionName = getTestMethodName() + "OnHeapRegion";

    for (int vmId = 0; vmId <= 1; vmId++) {
      Host.getHost(0).getVM(vmId).invoke(new CacheSerializableRunnable("createRegionNoException") {
        private static final long serialVersionUID = 1L;

        @Override
        public void run2() throws CacheException {
          disconnectFromDS();
          Properties properties = new Properties();
          properties.put(OFF_HEAP_MEMORY_SIZE, "2m");
          getSystem(properties);

          GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
          RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
          regionFactory.setOffHeap(true);
          Region region = regionFactory.create(offHeapRegionName);

          assertNotNull("Region is null", region);
          assertNotNull("Cache does not contain region", cache.getRegion(offHeapRegionName));

          regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
          regionFactory.create(onHeapRegionName);

          assertNotNull("Region is null", region);
          assertNotNull("Cache does not contain region", cache.getRegion(onHeapRegionName));
        }
      });
    }
  }
  
  /**
   * Asserts that creating a region on two members, with one being off-heap with local
   * storage and the other being on-heap without local storage, will not cause an
   * exception.
   */
  @Test
  public void testPartitionedRegionProfileWithAccessor() throws Exception {
    final String regionName = getTestMethodName() + "Region";

    // Create a region using off-heap
    Host.getHost(0).getVM(0).invoke(new CacheSerializableRunnable("createRegionNoException") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
        Properties properties = new Properties();
        properties.put(OFF_HEAP_MEMORY_SIZE, "2m");
        getSystem(properties);

        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
        regionFactory.setOffHeap(true);
        Region region = regionFactory.create(regionName);
        
        assertNotNull("Region is null", region);
        assertNotNull("Cache does not contain region", cache.getRegion(regionName));
      }
    });

    // Create an accessor (no local storage) for the same region
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("createRegionNoException") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
        Properties properties = new Properties();
        properties.put(OFF_HEAP_MEMORY_SIZE, "2m");
        getSystem(properties);

        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
        
        PartitionAttributes partitionAttributes = new PartitionAttributesFactory().setLocalMaxMemory(0).create();
        regionFactory.setPartitionAttributes(partitionAttributes);
        
        Region region = regionFactory.create(regionName);
        
        assertNotNull("Region is null", region);
        assertNotNull("Cache does not contain region", cache.getRegion(regionName));
      }
    });
  }
  
  /**
   * Asserts that creating a region on two members, with one being off-heap with local
   * storage and the other being a proxy will not cause an exception.
   */
  @Test
  public void testPartitionedRegionProfileWithProxy() throws Exception {
    final String regionName = getTestMethodName() + "Region";

    // Create a region using off-heap
    Host.getHost(0).getVM(0).invoke(new CacheSerializableRunnable("createRegionNoException") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
        Properties properties = new Properties();
        properties.put(OFF_HEAP_MEMORY_SIZE, "2m");
        getSystem(properties);

        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
        regionFactory.setOffHeap(true);
        Region region = regionFactory.create(regionName);
        
        assertNotNull("Region is null", region);
        assertNotNull("Cache does not contain region", cache.getRegion(regionName));
      }
    });

    // Create a proxy for the same region
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("createRegionNoException") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
        Properties properties = new Properties();
        properties.put(OFF_HEAP_MEMORY_SIZE, "2m");
        getSystem(properties);

        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION_PROXY);
        Region region = regionFactory.create(regionName);
        
        assertNotNull("Region is null", region);
        assertNotNull("Cache does not contain region", cache.getRegion(regionName));
      }
    });
  }
}
