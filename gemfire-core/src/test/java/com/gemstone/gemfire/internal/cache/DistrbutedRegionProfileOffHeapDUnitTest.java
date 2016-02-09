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
package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;

public class DistrbutedRegionProfileOffHeapDUnitTest extends CacheTestCase {
  private static final long serialVersionUID = 1L;

  public DistrbutedRegionProfileOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
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
  public void testPartitionedRegionProfileWithConflict() throws Exception {
    final String regionName = getTestMethodName() + "Region";

    Host.getHost(0).getVM(0).invoke(new CacheSerializableRunnable("createRegionNoException") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
        Properties properties = new Properties();
        properties.put(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "2m");
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
          properties.put(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "2m");
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
  public void testPartitionedRegionProfileWithAccessor() throws Exception {
    final String regionName = getTestMethodName() + "Region";

    // Create a region using off-heap
    Host.getHost(0).getVM(0).invoke(new CacheSerializableRunnable("createRegionNoException") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
        Properties properties = new Properties();
        properties.put(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "2m");
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
        properties.put(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "2m");
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
  public void testPartitionedRegionProfileWithProxy() throws Exception {
    final String regionName = getTestMethodName() + "Region";

    // Create a region using off-heap
    Host.getHost(0).getVM(0).invoke(new CacheSerializableRunnable("createRegionNoException") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
        Properties properties = new Properties();
        properties.put(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "2m");
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
        properties.put(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "2m");
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
