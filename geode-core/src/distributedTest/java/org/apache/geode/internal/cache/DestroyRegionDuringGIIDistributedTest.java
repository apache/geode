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

import static java.util.Arrays.asList;
import static org.apache.geode.distributed.ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.test.dunit.rules.DistributedRule.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.compression.Compressor;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheXmlRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class DestroyRegionDuringGIIDistributedTest implements Serializable {

  private static final int CHUNK_SIZE = 500 * 1024;
  private static final int ENTRIES_COUNT = 1000;
  private static final int VALUE_SIZE = CHUNK_SIZE * 10 / ENTRIES_COUNT;

  private static final InternalCache DUMMY_CACHE = mock(InternalCache.class);
  private static final Runnable DUMMY_RUNNABLE = () -> {
  };

  private static final AtomicReference<CacheDefinition> CACHE_DEFINITION = new AtomicReference<>();
  private static final AtomicReference<Runnable> TEAR_DOWN = new AtomicReference<>();
  private static final AtomicReference<InternalCache> CACHE = new AtomicReference<>(DUMMY_CACHE);
  private static final AtomicReference<File> DISK_DIR = new AtomicReference<>();

  private final byte[][] values = new byte[ENTRIES_COUNT][];

  private String rootRegionName;
  private String regionName;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();
  @Rule
  public SerializableTestName testName = new SerializableTestName();
  @Rule
  public CacheXmlRule cacheXmlRule = new CacheXmlRule()
      .cacheBuilder(() -> CACHE_DEFINITION.get().createCache());

  @Before
  public void setUp() {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);

    regionName = getUniqueName() + "_region";
    rootRegionName = getUniqueName() + "_rootRegion";

    for (int i = 0; i < ENTRIES_COUNT; i++) {
      values[i] = new byte[VALUE_SIZE];
      Arrays.fill(values[i], (byte) 0x42);
    }

    for (VM memberVM : asList(vm0, vm1, vm2, vm3)) {
      memberVM.invoke(() -> {
        TEAR_DOWN.set(DUMMY_RUNNABLE);
        CACHE.set(DUMMY_CACHE);
        DISK_DIR.set(temporaryFolder.newFolder("diskDir-" + getVMId()).getAbsoluteFile());
      });
    }
  }

  @After
  public void tearDown() {
    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> {
        TEAR_DOWN.getAndSet(DUMMY_RUNNABLE).run();
        InternalResourceManager.setResourceObserver(null);
        closeCache();
        CACHE_DEFINITION.set(null);
        DISK_DIR.set(null);
        InitialImageOperation.slowImageProcessing = 0;
      });
    }
  }

  @SuppressWarnings("unused")
  private Object[] getDefinitionParameters() {
    return new Object[] {
        /* DistributedAckRegionDUnitTest */
        new Object[] {"HEAP", "DISTRIBUTED_ACK"},
        /* DistributedAckRegionOffHeapDUnitTest */
        new Object[] {"OFF_HEAP", "DISTRIBUTED_ACK"},

        /* DistributedAckRegionCCEDUnitTest CONSERVE_SOCKETS="false" */
        new Object[] {"HEAP", "DISTRIBUTED_ACK_CCE"},
        /* DistributedAckRegionCCEOffHeapDUnitTest */
        new Object[] {"OFF_HEAP", "DISTRIBUTED_ACK_CCE"},

        /* DistributedAckRegionCompressionDUnitTest */
        new Object[] {"HEAP", "DISTRIBUTED_ACK_COMPRESSION"},

        /* DistributedAckOverflowRegionCCEDUnitTest */
        new Object[] {"HEAP", "DISTRIBUTED_ACK_EVICTION_OVERFLOW_CCE"},
        /* DistributedAckOverflowRegionCCEOffHeapDUnitTest */
        new Object[] {"OFF_HEAP", "DISTRIBUTED_ACK_EVICTION_OVERFLOW_CCE"},

        /* DistributedNoAckRegionDUnitTest */
        new Object[] {"HEAP", "DISTRIBUTED_NO_ACK"},
        /* DistributedNoAckRegionOffHeapDUnitTest */
        new Object[] {"OFF_HEAP", "DISTRIBUTED_NO_ACK"},

        /* DistributedNoAckRegionCCEDUnitTest */
        new Object[] {"HEAP", "DISTRIBUTED_NO_ACK_CCE"},
        /* DistributedNoAckRegionCCEOffHeapDUnitTest */
        new Object[] {"OFF_HEAP", "DISTRIBUTED_NO_ACK_CCE"},

        /* DiskDistributedNoAckAsyncRegionDUnitTest */
        new Object[] {"OFF_HEAP", "DISTRIBUTED_NO_ACK_PERSISTENT_REPLICATE_ASYNC"},

        /* DistributedAckPersistentRegionCCEDUnitTest */
        new Object[] {"HEAP", "DISTRIBUTED_NO_ACK_PERSISTENT_REPLICATE_CCE"},
        /* DistributedAckPersistentRegionCCEOffHeapDUnitTest */
        new Object[] {"OFF_HEAP", "DISTRIBUTED_NO_ACK_PERSISTENT_REPLICATE_CCE"},

        /* DiskDistributedNoAckAsyncOverflowRegionDUnitTest */
        new Object[] {"OFF_HEAP",
            "DISTRIBUTED_NO_ACK_PERSISTENT_REPLICATE_EVICTION_OVERFLOW_ASYNC"},

        /* DiskDistributedNoAckSyncOverflowRegionDUnitTest */
        new Object[] {"OFF_HEAP", "DISTRIBUTED_NO_ACK_PERSISTENT_REPLICATE_EVICTION_OVERFLOW_SYNC"},

        /* GlobalRegionDUnitTest */
        new Object[] {"HEAP", "GLOBAL"},
        /* GlobalRegionOffHeapDUnitTest */
        new Object[] {"OFF_HEAP", "GLOBAL"},

        /* GlobalRegionCCEDUnitTest CONSERVE_SOCKETS="false" */
        new Object[] {"HEAP", "GLOBAL_CCE"},
        /* GlobalRegionCCEOffHeapDUnitTest */
        new Object[] {"OFF_HEAP", "GLOBAL_CCE"},

        /* PartitionedRegionDUnitTest */
        new Object[] {"HEAP", "PARTITIONED_REGION"},
        /* PartitionedRegionOffHeapDUnitTest */
        new Object[] {"OFF_HEAP", "PARTITIONED_REGION"},

        /* PartitionedRegionCompressionDUnitTest */
        new Object[] {"HEAP", "PARTITIONED_REGION_COMPRESSION"},
    };
  }

  @Test
  @Parameters(method = "getDefinitionParameters")
  @TestCaseName("{method}_{0}_{1}")
  public void testNBRegionDestructionDuringGetInitialImage(CacheDefinition cacheDefinition,
      RegionDefinition regionDefinition) throws Exception {
    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> CACHE_DEFINITION.set(cacheDefinition));
    }

    assumeThat(regionDefinition.supportsReplication()).isTrue();

    vm0.invoke(() -> {
      cacheDefinition.createCache();

      createRootRegion(getCache().createRegionFactory()
          .setDataPolicy(DataPolicy.EMPTY)
          .setScope(Scope.DISTRIBUTED_ACK));

      regionDefinition.createRegionFactory(getCache())
          .setOffHeap(cacheDefinition.isOffHeap())
          .create(regionName);

      // reset slow
      InitialImageOperation.slowImageProcessing = 0;

      Region<Integer, byte[]> region = getCache().getRegion(regionName);
      for (int i = 0; i < ENTRIES_COUNT; i++) {
        region.put(i, values[i]);
      }
      assertThat(region.keySet().size()).isEqualTo(ENTRIES_COUNT);
    });

    // start asynchronous process that does updates to the data
    AsyncInvocation updateDataInVM0 = vm0.invokeAsync(() -> {
      await().until(() -> getCache().getCachePerfStats().getGetInitialImagesCompleted() < 1);

      Region<Object, Object> region = getCache().getRegion(regionName);

      // wait for profile of getInitialImage cache to show up
      awaitRegionProfiles(region, 1);

      // since we want to force a GII while updates are flying, make sure the other VM gets its
      // CreateRegionResponse and starts its GII before falling into the update loop
      doOperationsForDestroy(region);

      region.destroyRegion();

      flushRootRegion();
    });

    addIgnoredException(RegionDestroyedException.class);

    // in the meantime, do the get initial image in vm2
    AsyncInvocation getInitialImageInVM2 = vm2.invokeAsync(() -> {
      if (!regionDefinition.getScope().isGlobal()) {
        InitialImageOperation.slowImageProcessing = 200;
      }

      cacheXmlRule.beginCacheXml();

      // root region must be DACK because its used to sync up async subregions
      createRootRegion(cacheXmlRule.getCache().createRegionFactory()
          .setDataPolicy(DataPolicy.NORMAL)
          .setScope(Scope.DISTRIBUTED_ACK)
          .setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL)));

      regionDefinition.createRegionFactory(cacheXmlRule.getCache())
          .setDataPolicy(DataPolicy.REPLICATE)
          .setOffHeap(cacheDefinition.isOffHeap())
          .create(regionName);

      cacheXmlRule.finishCacheXml(regionName);

      // reset slow
      InitialImageOperation.slowImageProcessing = 0;

      // if global scope, the region doesn't get destroyed until after region creation
      await().until(
          () -> getCache().getRegion(regionName) == null || regionDefinition.getScope().isGlobal());

    });

    if (regionDefinition.getScope().isGlobal()) {
      // wait for nonblocking operations to complete
      updateDataInVM0.await();

      vm2.invoke(() -> InitialImageOperation.slowImageProcessing = 0);
    }

    // wait for GII to complete
    getInitialImageInVM2.await();
    if (regionDefinition.getScope().isGlobal()) {
      // wait for nonblocking operations to complete
      updateDataInVM0.await();
    }
  }

  @Ignore("TODO: test is disabled for 51542")
  @Test
  @Parameters(method = "getDefinitionParameters")
  @TestCaseName("{method}_{0}_{1}")
  public void testNBRegionInvalidationDuringGetInitialImage(CacheDefinition cacheDefinition,
      RegionDefinition regionDefinition)
      throws Exception {
    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> CACHE_DEFINITION.set(cacheDefinition));
    }

    assumeThat(regionDefinition.supportsReplication()).isTrue();
    assumeThat(regionDefinition.getScope().isDistributedNoAck()).isFalse();

    vm0.invoke(() -> {
      cacheDefinition.createCache();

      // root region must be DACK because its used to sync up async subregions
      createRootRegion(getCache().createRegionFactory()
          .setDataPolicy(DataPolicy.NORMAL)
          .setScope(Scope.DISTRIBUTED_ACK));

      regionDefinition.createRegionFactory(getCache())
          .setDataPolicy(DataPolicy.REPLICATE)
          .setOffHeap(cacheDefinition.isOffHeap())
          .create(regionName);

      // reset slow
      InitialImageOperation.slowImageProcessing = 0;

      Region<Integer, byte[]> region = getCache().getRegion(regionName);
      for (int i = 0; i < ENTRIES_COUNT; i++) {
        region.put(i, values[i]);
      }
      assertThat(region.keySet().size()).isEqualTo(ENTRIES_COUNT);
    });

    // start asynchronous process that does updates to the data
    AsyncInvocation updateDataInVM0 = vm0.invokeAsync("Do Nonblocking Operations", () -> {
      Region<Object, Object> region = getCache().getRegion(regionName);

      // wait for profile of getInitialImage cache to show up
      awaitRegionProfiles(region, 1);

      doOperationsForInvalidate(region);

      flushRootRegion();
    });

    // in the meantime, do the get initial image in vm2
    // slow down image processing to make it more likely to get async updates
    if (!regionDefinition.getScope().isGlobal()) {
      vm2.invoke("Set slow image processing", () -> {
        // make sure the cache is set up before turning on slow image processing
        getRootRegion();
        // if this is a no_ack test, then we need to slow down more because of the pauses in the
        // nonblocking operations
        InitialImageOperation.slowImageProcessing = 100;
      });
    }

    AsyncInvocation getInitialImageInVM2 = vm2.invokeAsync("Create Mirrored Region", () -> {
      cacheXmlRule.beginCacheXml();

      // root region must be DACK because its used to sync up async subregions
      createRootRegion(cacheXmlRule.getCache().createRegionFactory()
          .setDataPolicy(DataPolicy.NORMAL)
          .setScope(Scope.DISTRIBUTED_ACK)
          .setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL)));

      regionDefinition.createRegionFactory(cacheXmlRule.getCache())
          .setDataPolicy(DataPolicy.REPLICATE)
          .setOffHeap(cacheDefinition.isOffHeap())
          .create(regionName);

      cacheXmlRule.finishCacheXml(regionName);

      // reset slow
      InitialImageOperation.slowImageProcessing = 0;
    });

    if (!regionDefinition.getScope().isGlobal()) {
      // wait for nonblocking operations to complete
      updateDataInVM0.await();
      vm2.invoke(() -> InitialImageOperation.slowImageProcessing = 0);
    }

    // wait for GII to complete
    getInitialImageInVM2.await();

    long giiCompletedMillis = System.currentTimeMillis();

    if (regionDefinition.getScope().isGlobal()) {
      // wait for nonblocking operations to complete
      updateDataInVM0.await();
    }

    // Locally destroy the region in vm0 so we know that they are not found by a netSearch
    vm0.invoke(() -> getCache().getRegion(regionName).localDestroyRegion());

    // invoke repeating so noack regions wait for all updates to get processed
    vm2.invoke(() -> {
      await().untilAsserted(() -> {
        Region<Object, Object> region = getCache().getRegion(regionName);
        // expected entry count (subtract entries destroyed)
        int entryCount = ENTRIES_COUNT - ENTRIES_COUNT / 6;
        assertThat(region.entrySet(false)).hasSize(entryCount);
      });
    });

    vm2.invoke(() -> {
      Region<Integer, Object> region = getCache().getRegion(regionName);

      // expected entry count (subtract entries destroyed)
      int entryCount = ENTRIES_COUNT - ENTRIES_COUNT / 6;
      assertThat(region.entrySet(false).size()).isEqualTo(entryCount);

      // determine how many entries were updated before getInitialImage was complete
      int entriesUpdatedConcurrently = 0;

      entriesUpdatedConcurrently =
          validateEntriesForInvalidate(giiCompletedMillis, region, entriesUpdatedConcurrently);

      // Looks like some random expectations that will always be a hit/miss.

      // make sure at least some of them were concurrent
      if (regionDefinition.getScope().isGlobal()) {
        assertThat(entriesUpdatedConcurrently < 300)
            .as("Too many concurrent updates when expected to block: " + entriesUpdatedConcurrently)
            .isTrue();
      } else {
        assertThat(entriesUpdatedConcurrently >= 30)
            .as("Not enough updates concurrent with getInitialImage occurred to my liking. " +
                entriesUpdatedConcurrently + " entries out of " + entryCount +
                " were updated concurrently with getInitialImage, and I'd expect at least 50 or so")
            .isTrue();
      }
    });
  }

  private void doOperationsForDestroy(Region<Object, Object> region) {
    // operate on every odd entry with different value, alternating between updates, invalidates,
    // and destroys. These operations are likely to be nonblocking if a sufficient number of updates
    // get through before the get initial image is complete.
    for (int i = 1; i < 301; i += 2) {
      Object key = i;
      switch (i % 6) {
        case 1: // UPDATE
          // use the current timestamp so we know when it happened we could have used last
          // modification timestamps, but this works without enabling statistics
          region.put(key, System.currentTimeMillis());
          break;
        case 3: // INVALIDATE
          region.invalidate(key);
          if (region.getAttributes().getScope().isDistributedAck()) {
            assertThat(region.get(key)).isNull();
          }
          break;
        case 5: // DESTROY
          region.destroy(key);
          if (region.getAttributes().getScope().isDistributedAck()) {
            assertThat(region.get(key)).isNull();
          }
          break;
        default:
          fail("Unknown region operation: " + i);
          break;
      }
    }
  }

  private void doOperationsForInvalidate(Region<Object, Object> region) {
    // operate on every odd entry with different value, alternating between updates, invalidates,
    // and destroys. These operations are likely to be nonblocking if a sufficient number of
    // updates get through before the get initial image is complete.
    for (int i = 1; i < ENTRIES_COUNT; i += 2) {

      // at magical number 301, do a region invalidation, then continue as before
      if (i == 301) {
        // wait for previous updates to be processed
        flushIfNecessary(region);
        region.invalidateRegion();
        flushIfNecessary(region);
      }

      Object key = i;
      switch (i % 6) {
        case 1: // UPDATE
          // use the current timestamp so we know when it happened we could have used last
          // modification timestamps, but this works without enabling statistics
          region.put(key, System.currentTimeMillis());
          break;
        case 3: // INVALIDATE
          region.invalidate(key);
          if (region.getAttributes().getScope().isDistributedAck()) {
            // do a nonblocking netSearch
            assertThat(region.get(key)).isNull();
          }
          break;
        case 5: // DESTROY
          region.destroy(key);
          if (region.getAttributes().getScope().isDistributedAck()) {
            // do a nonblocking netSearch
            assertThat(region.get(key)).isNull();
          }
          break;
        default:
          fail("Unknown region operation: " + i);
          break;
      }
    }
  }

  private int validateEntriesForInvalidate(long giiCompletedMillis, Region<Integer, Object> region,
      int entriesUpdatedConcurrently) {
    for (int i = 0; i < ENTRIES_COUNT; i++) {
      Entry<Integer, Object> entry = region.getEntry(i);

      if (i < 301) {
        if (i % 6 == 5) {
          // destroyed
          assertThat(entry).as("Entry for #" + i).isNull();
        } else {
          assertThat(entry).as("Entry for #" + i).isNotNull();
          assertThat(entry.getValue()).isNull();
        }
        continue;
      }

      switch (i % 6) {
        // even keys are originals
        case 0:
        case 2:
        case 4:
          assertThat(entry).as("Entry for #" + i).isNotNull();
          assertThat(entry.getValue()).isNull();
          break;
        case 1: // updated
          assertThat(entry).as("Entry for #" + i).isNotNull();
          assertThat(entry.getValue()).isNotNull().isInstanceOf(Long.class);

          long timestamp = (long) entry.getValue();
          if (timestamp < giiCompletedMillis) {
            entriesUpdatedConcurrently++;
          }
          break;
        case 3: // invalidated
          assertThat(entry).as("Entry for #" + i).isNotNull();
          assertThat(entry.getValue()).isNull();
          break;
        case 5: // destroyed
          assertThat(entry).as("Entry for #" + i).isNull();
          break;
        default:
          fail("unexpected modulus result: " + i % 6);
          break;
      }
    }
    return entriesUpdatedConcurrently;
  }

  private String getUniqueName() {
    return getClass().getSimpleName() + "_" + testName.getMethodName();
  }

  private InternalCache getCache() {
    return CACHE.get();
  }

  private void closeCache() {
    CACHE.getAndSet(DUMMY_CACHE).close();
  }

  private static File[] getDiskDirs() {
    return new File[] {DISK_DIR.get()};
  }

  private void createRootRegion(RegionFactory regionFactory) {
    regionFactory.create(rootRegionName);
  }

  private Region<String, String> getRootRegion() {
    return getCache().getRegion(rootRegionName);
  }

  private void flushRootRegion() {
    // now do a put and our DACK root region which will not complete
    // until processed on otherside which means everything done before this
    // point has been processed
    Region<String, String> rootRegion = getRootRegion();
    if (rootRegion != null) {
      rootRegion.put("DONE", "FLUSH_OPS");
    }
  }

  /**
   * Make sure all messages done on region r have been processed on the remote side.
   */
  private void flushIfNecessary(Region r) {
    // Only needed for no-ack regions
  }

  /**
   * awaitRegionProfiles(region, 1);
   */
  private void awaitRegionProfiles(Region<?, ?> region, int expectedProfileCount) {
    CacheDistributionAdvisor regionAdvisor =
        ((CacheDistributionAdvisee) region).getCacheDistributionAdvisor();

    await("Awaiting '" + expectedProfileCount + "' profiles for '" + region + "'")
        .untilAsserted(
            () -> assertThat(regionAdvisor.adviseReplicates()).hasSize(expectedProfileCount));
  }

  private enum CacheDefinition {
    HEAP(() -> {
      return getDistributedSystemProperties();
    }, () -> {
      // nothing
    }),

    OFF_HEAP(() -> {
      Properties properties = getDistributedSystemProperties();
      properties.setProperty(OFF_HEAP_MEMORY_SIZE, "10m");
      return properties;
    }, () -> {
      OffHeapTestUtil.checkOrphans(CACHE.get());
    });

    private final Supplier<Properties> configSupplier;
    private final Runnable tearDown;

    CacheDefinition(Supplier<Properties> configSupplier, Runnable tearDown) {
      this.configSupplier = configSupplier;
      this.tearDown = tearDown;
    }

    private void createCache() {
      TEAR_DOWN.set(tearDown);
      CACHE.set((InternalCache) new CacheFactory(configSupplier.get()).create());
    }

    private boolean isOffHeap() {
      return OFF_HEAP == this;
    }
  }

  private enum RegionDefinition {
    DISTRIBUTED_ACK(Scope.DISTRIBUTED_ACK, DataPolicy.PRELOADED, NO_EVICTION, NULL_COMPRESSOR,
        NULL_DISK_STORE_NAME, CONCURRENCY_CHECKS_DISABLED, NO_DISK, SUPPORTS_REPLICATION,
        SUPPORTS_TRANSACTIONS),
    DISTRIBUTED_ACK_CCE(Scope.DISTRIBUTED_ACK, DataPolicy.REPLICATE, NO_EVICTION, NULL_COMPRESSOR,
        NULL_DISK_STORE_NAME, CONCURRENCY_CHECKS_ENABLED, NO_DISK, SUPPORTS_REPLICATION,
        SUPPORTS_TRANSACTIONS),
    DISTRIBUTED_ACK_COMPRESSION(Scope.DISTRIBUTED_ACK, DataPolicy.PRELOADED, NO_EVICTION,
        SNAPPY_COMPRESSOR, NULL_DISK_STORE_NAME, CONCURRENCY_CHECKS_DISABLED, NO_DISK,
        SUPPORTS_REPLICATION, SUPPORTS_TRANSACTIONS),
    DISTRIBUTED_ACK_EVICTION_OVERFLOW_CCE(Scope.DISTRIBUTED_ACK, DataPolicy.REPLICATE,
        EVICTION_OVERFLOW_TO_DISK, NULL_COMPRESSOR, NULL_DISK_STORE_NAME,
        CONCURRENCY_CHECKS_ENABLED, NO_DISK, SUPPORTS_REPLICATION, SUPPORTS_TRANSACTIONS),
    DISTRIBUTED_NO_ACK(Scope.DISTRIBUTED_NO_ACK, DataPolicy.PRELOADED, NO_EVICTION, NULL_COMPRESSOR,
        NULL_DISK_STORE_NAME, CONCURRENCY_CHECKS_DISABLED, NO_DISK, SUPPORTS_REPLICATION,
        SUPPORTS_TRANSACTIONS),
    DISTRIBUTED_NO_ACK_CCE(Scope.DISTRIBUTED_ACK, DataPolicy.REPLICATE, NO_EVICTION,
        NULL_COMPRESSOR, NULL_DISK_STORE_NAME, CONCURRENCY_CHECKS_ENABLED, NO_DISK,
        SUPPORTS_REPLICATION, SUPPORTS_TRANSACTIONS),
    DISTRIBUTED_NO_ACK_PERSISTENT_REPLICATE_ASYNC(Scope.DISTRIBUTED_NO_ACK,
        DataPolicy.PERSISTENT_REPLICATE, NO_EVICTION, NULL_COMPRESSOR, NULL_DISK_STORE_NAME,
        CONCURRENCY_CHECKS_ENABLED, NO_DISK, SUPPORTS_REPLICATION, SUPPORTS_TRANSACTIONS),
    DISTRIBUTED_NO_ACK_PERSISTENT_REPLICATE_CCE(Scope.DISTRIBUTED_NO_ACK,
        DataPolicy.PERSISTENT_REPLICATE, NO_EVICTION, NULL_COMPRESSOR, NULL_DISK_STORE_NAME,
        CONCURRENCY_CHECKS_ENABLED, NO_DISK, SUPPORTS_REPLICATION, SUPPORTS_TRANSACTIONS),
    DISTRIBUTED_NO_ACK_PERSISTENT_REPLICATE_EVICTION_OVERFLOW_ASYNC(Scope.DISTRIBUTED_NO_ACK,
        DataPolicy.PERSISTENT_REPLICATE, EVICTION_OVERFLOW_TO_DISK, NULL_COMPRESSOR,
        DISK_STORE_NAME, CONCURRENCY_CHECKS_ENABLED, DISK_ASYNCHRONOUS, SUPPORTS_REPLICATION,
        SUPPORTS_TRANSACTIONS),
    DISTRIBUTED_NO_ACK_PERSISTENT_REPLICATE_EVICTION_OVERFLOW_SYNC(Scope.DISTRIBUTED_NO_ACK,
        DataPolicy.PERSISTENT_REPLICATE, EVICTION_OVERFLOW_TO_DISK, NULL_COMPRESSOR,
        DISK_STORE_NAME, CONCURRENCY_CHECKS_ENABLED, DISK_SYNCHRONOUS, SUPPORTS_REPLICATION,
        SUPPORTS_TRANSACTIONS),
    GLOBAL(Scope.GLOBAL, DataPolicy.PRELOADED, NO_EVICTION, NULL_COMPRESSOR, NULL_DISK_STORE_NAME,
        CONCURRENCY_CHECKS_DISABLED, NO_DISK, SUPPORTS_REPLICATION, SUPPORTS_TRANSACTIONS),
    GLOBAL_CCE(Scope.GLOBAL, DataPolicy.REPLICATE, NO_EVICTION, NULL_COMPRESSOR,
        NULL_DISK_STORE_NAME, CONCURRENCY_CHECKS_ENABLED, NO_DISK, SUPPORTS_REPLICATION,
        SUPPORTS_TRANSACTIONS),
    PARTITIONED_REGION(Scope.DISTRIBUTED_ACK, DataPolicy.PRELOADED, NO_EVICTION, NULL_COMPRESSOR,
        NULL_DISK_STORE_NAME, CONCURRENCY_CHECKS_DISABLED, NO_DISK, NO_REPLICATION,
        NO_TRANSACTIONS),
    PARTITIONED_REGION_COMPRESSION(Scope.DISTRIBUTED_ACK, DataPolicy.PRELOADED, NO_EVICTION,
        SNAPPY_COMPRESSOR, NULL_DISK_STORE_NAME, CONCURRENCY_CHECKS_DISABLED, NO_DISK,
        NO_REPLICATION, NO_TRANSACTIONS);

    private final Scope scope;
    private final DataPolicy dataPolicy;
    private final EvictionAttributes evictionAttributes;
    private final String diskStoreName;
    private final boolean concurrencyChecksEnabled;
    private final boolean diskSynchronous;
    private final boolean supportsReplication;
    private final boolean supportsTransactions;
    private final Compressor compressor;

    RegionDefinition(Scope scope,
        DataPolicy dataPolicy,
        EvictionAttributes evictionAttributes,
        Compressor compressor,
        String diskStoreName,
        boolean concurrencyChecksEnabled,
        boolean diskSynchronous,
        boolean supportsReplication,
        boolean supportsTransactions) {
      this.scope = scope;
      this.dataPolicy = dataPolicy;
      this.evictionAttributes = evictionAttributes;
      this.compressor = compressor;
      this.diskStoreName = diskStoreName;
      this.concurrencyChecksEnabled = concurrencyChecksEnabled;
      this.diskSynchronous = diskSynchronous;
      this.supportsReplication = supportsReplication;
      this.supportsTransactions = supportsTransactions;
    }

    private <K, V> RegionFactory<K, V> createRegionFactory(Cache cache) {
      RegionFactory<K, V> regionFactory = cache.<K, V>createRegionFactory()
          .setScope(scope)
          .setDataPolicy(dataPolicy)
          .setCompressor(compressor)
          .setConcurrencyChecksEnabled(concurrencyChecksEnabled)
          .setEvictionAttributes(evictionAttributes);
      if (diskStoreName != null) {
        cache.createDiskStoreFactory()
            .setDiskDirs(getDiskDirs())
            .setQueueSize(0)
            .setTimeInterval(1000)
            .create(diskStoreName);
        regionFactory
            .setDiskStoreName(diskStoreName)
            .setDiskSynchronous(diskSynchronous);
      }
      return regionFactory;
    }

    private Scope getScope() {
      return scope;
    }

    private DataPolicy getDataPolicy() {
      return dataPolicy;
    }

    private boolean hasCompression() {
      return compressor != null;
    }

    private boolean hasConcurrencyChecksEnabled() {
      return concurrencyChecksEnabled;
    }

    private boolean supportsReplication() {
      return supportsReplication;
    }

    private boolean supportsTransactions() {
      return supportsTransactions;
    }
  }

  private static final EvictionAttributes EVICTION_OVERFLOW_TO_DISK =
      EvictionAttributes.createLRUEntryAttributes(5, EvictionAction.OVERFLOW_TO_DISK);
  private static final EvictionAttributes NO_EVICTION = new EvictionAttributesImpl();
  private static final Compressor SNAPPY_COMPRESSOR = new SnappyCompressor();
  private static final Compressor NULL_COMPRESSOR = null;
  private static final String DISK_STORE_NAME = "diskStore";
  private static final String NULL_DISK_STORE_NAME = null;
  private static final boolean CONCURRENCY_CHECKS_ENABLED = true;
  private static final boolean CONCURRENCY_CHECKS_DISABLED = false;
  private static final boolean DISK_SYNCHRONOUS = true;
  private static final boolean DISK_ASYNCHRONOUS = false;
  private static final boolean NO_DISK = false;
  private static final boolean SUPPORTS_REPLICATION = true;
  private static final boolean NO_REPLICATION = false;
  private static final boolean SUPPORTS_TRANSACTIONS = true;
  private static final boolean NO_TRANSACTIONS = false;
}
