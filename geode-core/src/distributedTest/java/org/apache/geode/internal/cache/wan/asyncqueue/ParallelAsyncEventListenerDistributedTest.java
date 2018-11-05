/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.wan.asyncqueue;

import static java.util.Collections.synchronizedSet;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.geode.cache.FixedPartitionAttributes.createFixedPartition;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getCurrentVMNum;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.FixedPartitionResolver;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueStats;
import org.apache.geode.cache.asyncqueue.internal.InternalAsyncEventQueue;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserver;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import org.apache.geode.internal.cache.partitioned.BecomePrimaryBucketMessage;
import org.apache.geode.internal.cache.partitioned.BecomePrimaryBucketMessage.BecomePrimaryBucketResponse;
import org.apache.geode.internal.cache.wan.AsyncEventQueueConfigurationException;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

/**
 * Extracted from {@link AsyncEventListenerDistributedTest}.
 */
@Category(AEQTest.class)
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
@SuppressWarnings("serial")
public class ParallelAsyncEventListenerDistributedTest implements Serializable {

  @Parameter
  public int dispatcherThreadCount;

  @Parameters(name = "dispatcherThreadCount={0}")
  public static Iterable<Integer> dispatcherThreadCounts() {
    return Arrays.asList(1, 3);
  }

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  private String partitionedRegionName;
  private String childPartitionedRegionName;
  private String replicateRegionName;
  private String asyncEventQueueId;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Before
  public void setUp() throws Exception {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);

    String className = getClass().getSimpleName();
    partitionedRegionName = className + "_PR";
    childPartitionedRegionName = className + "_PR_child";
    replicateRegionName = className + "_RR";

    asyncEventQueueId = className;
  }

  /**
   * Override as needed to add to the configuration, such as off-heap-memory-size.
   */
  protected Properties getDistributedSystemProperties() {
    return new Properties();
  }

  /**
   * Override as needed to add to the configuration, such as regionFactory.setOffHeap(boolean).
   */
  protected RegionFactory<?, ?> configureRegion(RegionFactory<?, ?> regionFactory) {
    return regionFactory;
  }

  @Test // parallel, PartitionedRegion, FixedPartitionAttributes
  public void testParallelAsyncEventQueueWithFixedPartition() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100, true));
    vm1.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100, true));

    String partition1 = "partition1";
    String partition2 = "partition2";
    List<String> allPartitions = Arrays.asList(partition1, partition2);

    vm0.invoke(() -> createFixedPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16,
        partition1, new SimpleFixedPartitionResolver(allPartitions)));
    vm1.invoke(() -> createFixedPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16,
        partition2, new SimpleFixedPartitionResolver(allPartitions)));

    vm0.invoke(() -> doPuts(partitionedRegionName, 256));

    vm0.invoke(() -> waitForAsyncQueueToEmpty());
    vm1.invoke(() -> waitForAsyncQueueToEmpty());

    int sizeInVM0 = vm0.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());
    int sizeInVM1 = vm1.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());

    String description = "sizeInVM0=" + sizeInVM0 + ", sizeInVM1=" + sizeInVM1;
    assertThat(sizeInVM0 + sizeInVM1).as(description).isEqualTo(256);
  }

  @Test // parallel, conflation, ReplicateRegion, IntegrationTest
  public void testParallelAsyncEventQueueWithReplicatedRegion() {
    vm0.invoke(() -> {
      createCache();

      createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), true, 100,
          dispatcherThreadCount, 100, true);

      assertThatThrownBy(() -> createReplicateRegion(replicateRegionName, asyncEventQueueId))
          .isInstanceOf(AsyncEventQueueConfigurationException.class)
          .hasMessageContaining("can not be used with replicated region");
    });
  }

  @Test // parallel, PartitionedRegion
  public void testParallelAsyncEventQueue() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());
    vm3.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100, true));
    vm1.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100, true));
    vm2.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100, true));
    vm3.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100, true));

    vm0.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm1.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm2.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm3.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));

    vm0.invoke(() -> doPuts(partitionedRegionName, 256));

    vm0.invoke(() -> waitForAsyncQueueToEmpty());
    vm1.invoke(() -> waitForAsyncQueueToEmpty());
    vm2.invoke(() -> waitForAsyncQueueToEmpty());
    vm3.invoke(() -> waitForAsyncQueueToEmpty());

    int sizeInVM0 = vm0.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());
    int sizeInVM1 = vm1.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());
    int sizeInVM2 = vm2.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());
    int sizeInVM3 = vm3.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());

    String description = "sizeInVM0=" + sizeInVM0 + ", sizeInVM1=" + sizeInVM1 + ", sizeInVM2="
        + sizeInVM2 + ", sizeInVM3=" + sizeInVM3;
    assertThat(sizeInVM0 + sizeInVM1 + sizeInVM2 + sizeInVM3).as(description).isEqualTo(256);
  }

  @Test // parallel, PartitionedRegion
  public void testParallelAsyncEventQueueSize() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());
    vm3.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100, true));
    vm1.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100, true));
    vm2.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100, true));
    vm3.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100, true));

    vm0.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm1.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm2.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm3.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));

    vm0.invoke(() -> getInternalGatewaySender().pause());
    vm1.invoke(() -> getInternalGatewaySender().pause());
    vm2.invoke(() -> getInternalGatewaySender().pause());
    vm3.invoke(() -> getInternalGatewaySender().pause());

    vm0.invoke(() -> waitForDispatcherToPause());
    vm1.invoke(() -> waitForDispatcherToPause());
    vm2.invoke(() -> waitForDispatcherToPause());
    vm3.invoke(() -> waitForDispatcherToPause());

    vm0.invoke(() -> doPuts(partitionedRegionName, 1000));

    assertThat(vm0.invoke(() -> getAsyncEventQueue().size())).isEqualTo(1000);
    assertThat(vm1.invoke(() -> getAsyncEventQueue().size())).isEqualTo(1000);
  }

  @Test // parallel, conflation, PartitionedRegion
  public void testParallelAsyncEventQueueWithConflationEnabled() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());
    vm3.invoke(() -> createCache());

    vm0.invoke(
        () -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), true, 100,
            dispatcherThreadCount, 100, true));
    vm1.invoke(
        () -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), true, 100,
            dispatcherThreadCount, 100, true));
    vm2.invoke(
        () -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), true, 100,
            dispatcherThreadCount, 100, true));
    vm3.invoke(
        () -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), true, 100,
            dispatcherThreadCount, 100, true));

    vm0.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm1.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm2.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm3.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));

    vm0.invoke(() -> getInternalGatewaySender().pause());
    vm1.invoke(() -> getInternalGatewaySender().pause());
    vm2.invoke(() -> getInternalGatewaySender().pause());
    vm3.invoke(() -> getInternalGatewaySender().pause());

    vm0.invoke(() -> waitForDispatcherToPause());
    vm1.invoke(() -> waitForDispatcherToPause());
    vm2.invoke(() -> waitForDispatcherToPause());
    vm3.invoke(() -> waitForDispatcherToPause());

    Map<Integer, Integer> keyValues = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm0.invoke(() -> {
      putGivenKeyValue(partitionedRegionName, keyValues);

      validateParallelAsyncEventQueueSize(keyValues.size());
    });

    Map<Integer, String> updateKeyValues = new HashMap<>();
    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    vm0.invoke(() -> {
      putGivenKeyValue(partitionedRegionName, updateKeyValues);

      // no conflation of creates
      waitForParallelAsyncEventQueueSize(keyValues.size() + updateKeyValues.size());

      putGivenKeyValue(partitionedRegionName, updateKeyValues);

      // conflation of updates
      waitForParallelAsyncEventQueueSize(keyValues.size() + updateKeyValues.size());
    });

    vm0.invoke(() -> getInternalGatewaySender().resume());
    vm1.invoke(() -> getInternalGatewaySender().resume());
    vm2.invoke(() -> getInternalGatewaySender().resume());
    vm3.invoke(() -> getInternalGatewaySender().resume());

    vm0.invoke(() -> waitForAsyncQueueToEmpty());
    vm1.invoke(() -> waitForAsyncQueueToEmpty());
    vm2.invoke(() -> waitForAsyncQueueToEmpty());
    vm3.invoke(() -> waitForAsyncQueueToEmpty());

    int sizeInVM0 = vm0.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());
    int sizeInVM1 = vm1.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());
    int sizeInVM2 = vm2.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());
    int sizeInVM3 = vm3.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());

    String description = "sizeInVM0=" + sizeInVM0 + ", sizeInVM1=" + sizeInVM1 + ", sizeInVM2="
        + sizeInVM2 + ", sizeInVM3=" + sizeInVM3;
    assertThat(sizeInVM0 + sizeInVM1 + sizeInVM2 + sizeInVM3).as(description)
        .isEqualTo(keyValues.size());
  }

  /**
   * Added to reproduce defect #47213
   *
   * TRAC #47213:
   */
  @Test // parallel, conflation, PartitionedRegion, RegressionTest
  public void testParallelAsyncEventQueueWithConflationEnabled_bug47213() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());
    vm3.invoke(() -> createCache());

    vm0.invoke(
        () -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), true, 100,
            dispatcherThreadCount, 100, true));
    vm1.invoke(
        () -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), true, 100,
            dispatcherThreadCount, 100, true));
    vm2.invoke(
        () -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), true, 100,
            dispatcherThreadCount, 100, true));
    vm3.invoke(
        () -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), true, 100,
            dispatcherThreadCount, 100, true));

    vm0.invoke(() -> createPartitionedRegionAndAwaitRecovery(partitionedRegionName,
        asyncEventQueueId, 1, 16));
    vm1.invoke(() -> createPartitionedRegionAndAwaitRecovery(partitionedRegionName,
        asyncEventQueueId, 1, 16));
    vm2.invoke(() -> createPartitionedRegionAndAwaitRecovery(partitionedRegionName,
        asyncEventQueueId, 1, 16));
    vm3.invoke(() -> createPartitionedRegionAndAwaitRecovery(partitionedRegionName,
        asyncEventQueueId, 1, 16));

    vm0.invoke(() -> getInternalGatewaySender().pause());
    vm1.invoke(() -> getInternalGatewaySender().pause());
    vm2.invoke(() -> getInternalGatewaySender().pause());
    vm3.invoke(() -> getInternalGatewaySender().pause());

    vm0.invoke(() -> waitForDispatcherToPause());
    vm1.invoke(() -> waitForDispatcherToPause());
    vm2.invoke(() -> waitForDispatcherToPause());
    vm3.invoke(() -> waitForDispatcherToPause());

    Map<Integer, Integer> keyValues = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm0.invoke(() -> {
      putGivenKeyValue(partitionedRegionName, keyValues);

      waitForParallelAsyncEventQueueSize(keyValues.size());
    });

    Map<Integer, String> updateKeyValues = new HashMap<>();
    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    vm0.invoke(() -> {
      putGivenKeyValue(partitionedRegionName, updateKeyValues);
      putGivenKeyValue(partitionedRegionName, updateKeyValues);

      waitForParallelAsyncEventQueueSize(keyValues.size() + updateKeyValues.size());
    });

    vm0.invoke(() -> getInternalGatewaySender().resume());
    vm1.invoke(() -> getInternalGatewaySender().resume());
    vm2.invoke(() -> getInternalGatewaySender().resume());
    vm3.invoke(() -> getInternalGatewaySender().resume());

    vm0.invoke(() -> waitForAsyncQueueToEmpty());
    vm1.invoke(() -> waitForAsyncQueueToEmpty());
    vm2.invoke(() -> waitForAsyncQueueToEmpty());
    vm3.invoke(() -> waitForAsyncQueueToEmpty());

    int sizeInVM0 = vm0.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());
    int sizeInVM1 = vm1.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());
    int sizeInVM2 = vm2.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());
    int sizeInVM3 = vm3.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());

    String description = "sizeInVM0=" + sizeInVM0 + ", sizeInVM1=" + sizeInVM1 + ", sizeInVM2="
        + sizeInVM2 + ", sizeInVM3=" + sizeInVM3;
    assertThat(sizeInVM0 + sizeInVM1 + sizeInVM2 + sizeInVM3).as(description)
        .isEqualTo(keyValues.size());
  }

  @Test // parallel, PartitionedRegion, accessor
  public void testParallelAsyncEventQueueWithOneAccessor() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());
    vm3.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100, true));
    vm1.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100, true));
    vm2.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100, true));
    vm3.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false,
        100, dispatcherThreadCount, 100, true));

    vm0.invoke(
        () -> createPartitionedRegionAccessor(partitionedRegionName, asyncEventQueueId, 0, 16));

    vm1.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm2.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm3.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));

    vm0.invoke(() -> doPuts(partitionedRegionName, 256));

    vm1.invoke(() -> waitForAsyncQueueToEmpty());
    vm2.invoke(() -> waitForAsyncQueueToEmpty());
    vm3.invoke(() -> waitForAsyncQueueToEmpty());

    vm0.invoke(() -> validateSpyAsyncEventListenerEventsMap(0));

    int sizeInVM1 = vm1.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());
    int sizeInVM2 = vm2.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());
    int sizeInVM3 = vm3.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());

    String description =
        "sizeInVM1=" + sizeInVM1 + ", sizeInVM2=" + sizeInVM2 + ", sizeInVM3=" + sizeInVM3;
    assertThat(sizeInVM1 + sizeInVM2 + sizeInVM3).as(description).isEqualTo(256);
  }

  @Test // parallel, persistent, PartitionedRegion
  public void testParallelAsyncEventQueueWithPersistence() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());
    vm2.invoke(() -> createCache());
    vm3.invoke(() -> createCache());

    vm0.invoke(() -> createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        false, 100, createDiskStoreName(asyncEventQueueId), false, dispatcherThreadCount, 100, true,
        true));
    vm1.invoke(() -> createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        false, 100, createDiskStoreName(asyncEventQueueId), false, dispatcherThreadCount, 100, true,
        true));
    vm2.invoke(() -> createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        false, 100, createDiskStoreName(asyncEventQueueId), false, dispatcherThreadCount, 100, true,
        true));
    vm3.invoke(() -> createPersistentAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(),
        false, 100, createDiskStoreName(asyncEventQueueId), false, dispatcherThreadCount, 100, true,
        true));

    vm0.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm1.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm2.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));
    vm3.invoke(() -> createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16));

    vm0.invoke(() -> doPuts(partitionedRegionName, 256));

    vm0.invoke(() -> waitForAsyncQueueToEmpty());
    vm1.invoke(() -> waitForAsyncQueueToEmpty());
    vm2.invoke(() -> waitForAsyncQueueToEmpty());
    vm3.invoke(() -> waitForAsyncQueueToEmpty());

    int sizeInVM0 = vm0.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());
    int sizeInVM1 = vm1.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());
    int sizeInVM2 = vm2.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());
    int sizeInVM3 = vm3.invoke(() -> getSpyAsyncEventListener().getEventsMap().size());

    String description = "sizeInVM0=" + sizeInVM0 + ", sizeInVM1=" + sizeInVM1 + ", sizeInVM2="
        + sizeInVM2 + ", sizeInVM3=" + sizeInVM3;
    assertThat(sizeInVM0 + sizeInVM1 + sizeInVM2 + sizeInVM3).as(description).isEqualTo(256);
  }

  /**
   * Test case to test possibleDuplicates. vm0 & vm1 are hosting the PR. vm1 is killed so the
   * buckets hosted by it are shifted to vm0.
   */
  @Test // parallel, PartitionedRegion, possibleDuplicates
  public void testParallelAsyncEventQueueHA_Scenario1() throws InterruptedException {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new GatewaySenderAsyncEventListener(),
        false, 5, dispatcherThreadCount, 100, true));
    vm1.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new GatewaySenderAsyncEventListener(),
        false, 5, dispatcherThreadCount, 100, true));

    vm0.invoke(() -> createPartitionedRegionAndAwaitRecovery(partitionedRegionName,
        asyncEventQueueId, 1, 16));
    vm1.invoke(() -> createPartitionedRegionAndAwaitRecovery(partitionedRegionName,
        asyncEventQueueId, 1, 16));

    vm0.invoke(() -> getInternalGatewaySender().pause());
    vm1.invoke(() -> getInternalGatewaySender().pause());

    vm0.invoke(() -> waitForDispatcherToPause());
    vm1.invoke(() -> waitForDispatcherToPause());

    vm0.invoke(() -> doPuts(partitionedRegionName, 80));

    Set<Integer> primaryBucketsInVM1 =
        vm0.invoke(() -> getAllLocalPrimaryBucketIds(partitionedRegionName));
    Set<Integer> primaryBucketsInVM2 =
        vm1.invoke(() -> getAllLocalPrimaryBucketIds(partitionedRegionName));

    assertThat(primaryBucketsInVM1.size() + primaryBucketsInVM2.size()).isEqualTo(16);

    vm1.invoke(() -> getCache().close());

    vm0.invoke(() -> {
      // give some time for rebalancing to happen
      SECONDS.sleep(2); // TODO: change to await rebalancing to complete

      getInternalGatewaySender().resume();

      waitForAsyncQueueToEmpty();

      validatePossibleDuplicateEvents(primaryBucketsInVM2, 5);
    });
  }

  /**
   * Test case to test possibleDuplicates. vm0 & vm1 are hosting the PR. vm1 is killed and
   * subsequently vm2 is brought up. Buckets are now rebalanced between vm0 & vm2.
   */
  @Test // parallel, PartitionedRegion, possibleDuplicates
  public void testParallelAsyncEventQueueHA_Scenario2() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new GatewaySenderAsyncEventListener(),
        false, 5, dispatcherThreadCount, 100, true));
    vm1.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new GatewaySenderAsyncEventListener(),
        false, 5, dispatcherThreadCount, 100, true));

    vm0.invoke(() -> createPartitionedRegionAndAwaitRecovery(partitionedRegionName,
        asyncEventQueueId, 1, 16));
    vm1.invoke(() -> createPartitionedRegionAndAwaitRecovery(partitionedRegionName,
        asyncEventQueueId, 1, 16));

    vm0.invoke(() -> getInternalGatewaySender().pause());
    vm1.invoke(() -> getInternalGatewaySender().pause());

    vm0.invoke(() -> doPuts(partitionedRegionName, 80));

    Set<Integer> primaryBucketsInVM1 =
        vm1.invoke(() -> getAllLocalPrimaryBucketIds(partitionedRegionName));
    assertThat(primaryBucketsInVM1).isNotEmpty();

    // before shutdown vm1, both vm0 and vm1 should have 40 events in primary queue
    vm0.invoke(() -> validateAsyncEventQueueStats(40, 40, 80, 80, 0));
    vm1.invoke(() -> validateAsyncEventQueueStats(40, 40, 80, 80, 0));

    vm1.invoke(() -> getCache().close());

    vm2.invoke(() -> {
      createCache();
      createAsyncEventQueue(asyncEventQueueId, new GatewaySenderAsyncEventListener(), false, 5,
          dispatcherThreadCount, 100, true);

      // vm2 will move some primary buckets from vm0, but vm0's primary queue size did not reduce
      getInternalGatewaySender().pause();
      createPartitionedRegionAndAwaitRecovery(partitionedRegionName, asyncEventQueueId, 1, 16);
    });

    Set<Integer> primaryBucketsInVM0 =
        vm0.invoke(() -> getAllLocalPrimaryBucketIds(partitionedRegionName));
    assertThat(primaryBucketsInVM0).isNotEmpty();

    Set<Integer> primaryBucketsInVM2 =
        vm2.invoke(() -> getAllLocalPrimaryBucketIds(partitionedRegionName));
    assertThat(primaryBucketsInVM2).isNotEmpty();

    vm0.invoke(() -> validateAsyncEventQueueStats(40, 40, 80, 80, 0));
    vm2.invoke(() -> validateAsyncEventQueueStats(40, 40, 0, 0, 0));

    vm0.invoke(() -> getInternalGatewaySender().resume());
    vm2.invoke(() -> getInternalGatewaySender().resume());

    vm0.invoke(() -> waitForAsyncQueueToEmpty());
    vm2.invoke(() -> waitForAsyncQueueToEmpty());

    vm2.invoke(() -> validatePossibleDuplicateEvents(primaryBucketsInVM2, 5));
  }

  /**
   * Test case to test possibleDuplicates. vm0 & vm1 are hosting the PR. vm2 is brought up and
   * rebalancing is triggered so the buckets get balanced among vm0, vm1 & vm2.
   */
  @Test // parallel, PartitionedRegion, possibleDuplicates
  public void testParallelAsyncEventQueueHA_Scenario3() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());

    vm0.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new GatewaySenderAsyncEventListener(),
        false, 5, dispatcherThreadCount, 100, true));
    vm1.invoke(() -> createAsyncEventQueue(asyncEventQueueId, new GatewaySenderAsyncEventListener(),
        false, 5, dispatcherThreadCount, 100, true));

    vm0.invoke(() -> createPartitionedRegionAndAwaitRecovery(partitionedRegionName,
        asyncEventQueueId, 1, 16));
    vm1.invoke(() -> createPartitionedRegionAndAwaitRecovery(partitionedRegionName,
        asyncEventQueueId, 1, 16));

    vm0.invoke(() -> getInternalGatewaySender().pause());
    vm1.invoke(() -> getInternalGatewaySender().pause());

    vm0.invoke(() -> waitForDispatcherToPause());
    vm1.invoke(() -> waitForDispatcherToPause());

    vm0.invoke(() -> doPuts(partitionedRegionName, 80));

    vm2.invoke(() -> {
      createCache();
      createAsyncEventQueue(asyncEventQueueId, new GatewaySenderAsyncEventListener(), false, 5,
          dispatcherThreadCount, 100, true);
      createPartitionedRegionAndAwaitRecovery(partitionedRegionName, asyncEventQueueId, 1, 16);
    });

    vm0.invoke(() -> doRebalance());

    Set<Integer> primaryBucketsInVM3 =
        vm2.invoke(() -> getAllLocalPrimaryBucketIds(partitionedRegionName));

    vm0.invoke(() -> getInternalGatewaySender().resume());
    vm1.invoke(() -> getInternalGatewaySender().resume());

    vm0.invoke(() -> waitForAsyncQueueToEmpty());
    vm1.invoke(() -> waitForAsyncQueueToEmpty());
    vm2.invoke(() -> waitForAsyncQueueToEmpty());

    vm2.invoke(() -> validatePossibleDuplicateEvents(primaryBucketsInVM3, 5));
  }

  /**
   * Added for defect #50364 Can't colocate region that has AEQ with a region that does not have
   * that same AEQ
   *
   * TRAC #50364:
   */
  @Test // parallel, PartitionedRegion, colocated, RegressionTest, IntegrationTest
  public void testParallelAsyncEventQueueAttachedToChildRegionButNotToParentRegion() {
    vm2.invoke(() -> {
      // create cache on node
      createCache();

      // create AsyncEventQueue on node
      createAsyncEventQueue(asyncEventQueueId, new SpyAsyncEventListener(), false, 10,
          dispatcherThreadCount, 100, true);

      // create leader (parent) PR on node
      createPartitionedRegionWithRecoveryDelay(partitionedRegionName, 0, 0, 100);

      // create colocated (child) PR on node
      createColocatedPartitionedRegion(childPartitionedRegionName, asyncEventQueueId,
          0, 100, partitionedRegionName);

      // do puts in colocated (child) PR on node
      doPuts(childPartitionedRegionName, 1000);

      // wait for AsyncEventQueue to get empty on node
      waitForAsyncQueueToEmpty();

      // verify the events in listener
      assertThat(getSpyAsyncEventListener().getEventsMap().size()).isEqualTo(1000);
    });
  }

  @Test // parallel, Rebalancing, PartitionedRegion
  public void testParallelAsyncEventQueueMoveBucketAndMoveItBackDuringDispatching() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());

    DistributedMember memberInVM0 =
        vm0.invoke(() -> getCache().getDistributedSystem().getDistributedMember());
    DistributedMember memberInVM1 =
        vm1.invoke(() -> getCache().getDistributedSystem().getDistributedMember());

    vm0.invoke(() -> {
      createAsyncEventQueue(asyncEventQueueId,
          new BucketMovingAsyncEventListener(memberInVM1, getCache(), partitionedRegionName),
          false, 10, dispatcherThreadCount, 100, true);
      createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16);

      getInternalGatewaySender().pause();
      doPuts(partitionedRegionName, 113);
    });

    vm1.invoke(() -> {
      createAsyncEventQueue(asyncEventQueueId,
          new BucketMovingAsyncEventListener(memberInVM0, getCache(), partitionedRegionName),
          false, 10, dispatcherThreadCount, 100, true);
      createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 0, 16);
    });

    vm0.invoke(() -> getInternalGatewaySender().resume());

    vm0.invoke(() -> waitForAsyncQueueToEmpty());
    vm1.invoke(() -> waitForAsyncQueueToEmpty());

    Set<Object> allKeys = new HashSet<>();
    allKeys.addAll(vm0.invoke(() -> getBucketMovingAsyncEventListener().getKeysSeen()));
    allKeys.addAll(vm1.invoke(() -> getBucketMovingAsyncEventListener().getKeysSeen()));

    Set<Integer> expectedKeys = IntStream.range(0, 113).boxed().collect(toSet());
    assertThat(allKeys).isEqualTo(expectedKeys);

    assertThat(vm0.invoke(() -> getBucketMovingAsyncEventListener().isMoved())).isTrue();

    vm1.invoke(() -> await()
        .untilAsserted(() -> assertThat(getBucketMovingAsyncEventListener().isMoved()).isTrue()));
  }

  @Test // parallel, possibleDuplicates, PartitionedRegion, try-finally
  public void testParallelAsyncEventQueueWithPossibleDuplicateEvents() {
    // Set disable move primaries on start up
    vm0.invoke(() -> setDisableMovePrimary());
    vm1.invoke(() -> setDisableMovePrimary());

    int numPuts = 30;

    vm0.invoke(() -> {
      // Create cache and async event queue in member 1
      createCache();
      createAsyncEventQueue(asyncEventQueueId, new PossibleDuplicateAsyncEventListener(),
          false, 1, dispatcherThreadCount, 100, true);

      // Create region with async event queue in member 1
      createPartitionedRegionAndAwaitRecovery(partitionedRegionName, asyncEventQueueId, 1, 16);

      // Do puts so that all primaries are in member 1
      doPuts(partitionedRegionName, numPuts);
    });

    vm1.invoke(() -> {
      // Create cache and async event queue in member 2
      createCache();
      createAsyncEventQueue(asyncEventQueueId, new PossibleDuplicateAsyncEventListener(),
          false, 1, dispatcherThreadCount, 100, true);

      // Create region with paused async event queue in member 2
      createPartitionedRegionAndAwaitRecovery(partitionedRegionName, asyncEventQueueId, 1, 16);
      getInternalGatewaySender().pause();
    });

    // Close cache in member 1 (all AEQ buckets will fail over to member 2)
    vm0.invoke(() -> getCache().close());

    vm1.invoke(() -> {
      // Start processing async event queue in member 2
      getInternalGatewaySender().resume();
      getPossibleDuplicateAsyncEventListener().startProcessingEvents();

      // Wait for queue to be empty
      waitForAsyncQueueToEmpty();

      // Verify all events were processed in member 2
      validatePossibleDuplicateEvents(numPuts);
    });
  }

  @Test // parallel, PartitionedRegion, Rebalancing
  public void testParallelAsyncEventQueueMovePrimaryAndMoveItBackDuringDispatching() {
    vm0.invoke(() -> createCache());
    vm1.invoke(() -> createCache());

    DistributedMember memberInVM0 =
        vm0.invoke(() -> getCache().getDistributedSystem().getDistributedMember());
    DistributedMember memberInVM1 =
        vm1.invoke(() -> getCache().getDistributedSystem().getDistributedMember());

    vm0.invoke(() -> {
      // Create a PR with 1 bucket in vm0. Pause the sender and put some data in it
      createAsyncEventQueue(asyncEventQueueId, new PrimaryMovingAsyncEventListener(memberInVM1),
          false, 10, dispatcherThreadCount, 100, true);
      createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 1, 1);

      getInternalGatewaySender().pause();
      doPuts(partitionedRegionName, 113);
    });

    vm1.invoke(() -> {
      // Create the PR in vm1. This will create a redundant copy, but will be the secondary
      createAsyncEventQueue(asyncEventQueueId, new PrimaryMovingAsyncEventListener(memberInVM0),
          false, 10, dispatcherThreadCount, 100, true);
      createPartitionedRegion(partitionedRegionName, asyncEventQueueId, 1, 1);

      // do a rebalance just to make sure we have restored redundancy
      getCache().getResourceManager().createRebalanceFactory().start().getResults();
    });

    // Resume the AEQ. This should trigger the primary to move to vm1, which will then move it back
    vm0.invoke(() -> getInternalGatewaySender().resume());

    vm0.invoke(() -> waitForPrimaryToMove());
    vm1.invoke(() -> waitForPrimaryToMove());

    vm0.invoke(() -> waitForAsyncQueueToEmpty());
    vm1.invoke(() -> waitForAsyncQueueToEmpty());

    Set<Object> allKeys = new HashSet<>();
    allKeys.addAll(vm0.invoke(() -> getPrimaryMovingAsyncEventListener().getKeysSeen()));
    allKeys.addAll(vm1.invoke(() -> getPrimaryMovingAsyncEventListener().getKeysSeen()));

    Set<Integer> expectedKeys = IntStream.range(0, 113).boxed().collect(toSet());
    assertThat(allKeys).isEqualTo(expectedKeys);
  }

  private void setDisableMovePrimary() {
    System.setProperty("gemfire.DISABLE_MOVE_PRIMARIES_ON_STARTUP", "true");
  }

  private InternalCache getCache() {
    return cacheRule.getOrCreateCache(getDistributedSystemProperties());
  }

  private void createCache() {
    cacheRule.createCache(getDistributedSystemProperties());
  }

  private void createPartitionedRegion(String regionName,
      String asyncEventQueueId,
      int redundantCopies,
      int totalNumBuckets) {
    assertThat(regionName).isNotEmpty();
    assertThat(asyncEventQueueId).isNotEmpty();

    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.addAsyncEventQueueId(asyncEventQueueId);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    configureRegion(regionFactory).create(regionName);
  }

  private void createPartitionedRegionAccessor(String regionName,
      String asyncEventQueueId,
      int redundantCopies,
      int totalNumBuckets) {
    assertThat(regionName).isNotEmpty();
    assertThat(asyncEventQueueId).isNotEmpty();

    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setLocalMaxMemory(0);
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.addAsyncEventQueueId(asyncEventQueueId);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    configureRegion(regionFactory).create(regionName);
  }

  private void createPartitionedRegionWithRecoveryDelay(String regionName,
      long recoveryDelay,
      int redundantCopies,
      int totalNumBuckets) {
    assertThat(regionName).isNotEmpty();

    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRecoveryDelay(recoveryDelay);
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    configureRegion(regionFactory).create(regionName);
  }

  private void createFixedPartitionedRegion(String regionName,
      String asyncEventQueueId,
      int redundantCopies,
      int totalNumBuckets,
      String partitionName,
      PartitionResolver partitionResolver) {
    assertThat(regionName).isNotEmpty();
    assertThat(asyncEventQueueId).isNotEmpty();
    assertThat(partitionName).isNotEmpty();
    assertThat(partitionResolver).isNotNull();

    FixedPartitionAttributes fixedPartitionAttributes = createFixedPartition(partitionName, true);

    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.addFixedPartitionAttributes(fixedPartitionAttributes);
    partitionAttributesFactory.setPartitionResolver(partitionResolver);
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.addAsyncEventQueueId(asyncEventQueueId);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    configureRegion(regionFactory).create(regionName);
  }

  private void createColocatedPartitionedRegion(String regionName,
      String asyncEventQueueId,
      int redundantCopies,
      int totalNumBuckets,
      String colocatedWith) {
    assertThat(regionName).isNotEmpty();
    assertThat(asyncEventQueueId).isNotEmpty();
    assertThat(colocatedWith).isNotEmpty();

    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setColocatedWith(colocatedWith);
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.addAsyncEventQueueId(asyncEventQueueId);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    configureRegion(regionFactory).create(regionName);
  }

  private void createPartitionedRegionAndAwaitRecovery(String regionName,
      String asyncEventQueueId,
      int redundantCopies,
      int totalNumBuckets)
      throws InterruptedException {
    CountDownLatch recoveryDone = new CountDownLatch(2);

    ResourceObserver resourceObserver = new ResourceObserverAdapter() {
      @Override
      public void recoveryFinished(Region region) {
        recoveryDone.countDown();
      }
    };
    InternalResourceManager.setResourceObserver(resourceObserver);

    createPartitionedRegion(regionName, asyncEventQueueId, redundantCopies, totalNumBuckets);

    recoveryDone.await(2, MINUTES);
  }

  private void createReplicateRegion(String regionName, String asyncEventQueueId) {
    assertThat(regionName).isNotEmpty();
    assertThat(asyncEventQueueId).isNotEmpty();

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(REPLICATE);
    regionFactory.addAsyncEventQueueId(asyncEventQueueId);

    configureRegion(regionFactory).create(regionName);
  }

  private void createDiskStore(String diskStoreName, String asyncEventQueueId) {
    assertThat(diskStoreName).isNotEmpty();
    assertThat(asyncEventQueueId).isNotEmpty();

    File directory = createDirectory(createDiskStoreName(asyncEventQueueId));

    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(new File[] {directory});

    diskStoreFactory.create(diskStoreName);
  }

  private File createDirectory(String name) {
    assertThat(name).isNotEmpty();

    File directory = new File(temporaryFolder.getRoot(), name);
    if (!directory.exists()) {
      try {
        return temporaryFolder.newFolder(name);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return directory;
  }

  private String createDiskStoreName(String asyncEventQueueId) {
    assertThat(asyncEventQueueId).isNotEmpty();

    return asyncEventQueueId + "_disk_" + getCurrentVMNum();
  }

  private void createPersistentAsyncEventQueue(String asyncEventQueueId,
      AsyncEventListener asyncEventListener,
      boolean isBatchConflationEnabled,
      int batchSize,
      String diskStoreName,
      boolean isDiskSynchronous,
      int dispatcherThreads,
      int maximumQueueMemory,
      boolean isParallel,
      boolean isPersistent) {
    assertThat(asyncEventQueueId).isNotEmpty();
    assertThat(asyncEventListener).isNotNull();
    assertThat(diskStoreName).isNotEmpty();

    createDiskStore(diskStoreName, asyncEventQueueId);

    AsyncEventQueueFactory asyncEventQueueFactory = getCache().createAsyncEventQueueFactory();
    asyncEventQueueFactory.setBatchConflationEnabled(isBatchConflationEnabled);
    asyncEventQueueFactory.setBatchSize(batchSize);
    asyncEventQueueFactory.setDiskStoreName(diskStoreName);
    asyncEventQueueFactory.setDiskSynchronous(isDiskSynchronous);
    asyncEventQueueFactory.setDispatcherThreads(dispatcherThreads);
    asyncEventQueueFactory.setMaximumQueueMemory(maximumQueueMemory);
    asyncEventQueueFactory.setParallel(isParallel);
    asyncEventQueueFactory.setPersistent(isPersistent);

    asyncEventQueueFactory.create(asyncEventQueueId, asyncEventListener);
  }

  private void createAsyncEventQueue(String asyncEventQueueId,
      AsyncEventListener asyncEventListener,
      boolean isBatchConflationEnabled,
      int batchSize,
      int dispatcherThreads,
      int maximumQueueMemory,
      boolean isParallel) {
    assertThat(asyncEventQueueId).isNotEmpty();
    assertThat(asyncEventListener).isNotNull();

    AsyncEventQueueFactory asyncEventQueueFactory = getCache().createAsyncEventQueueFactory();
    asyncEventQueueFactory.setBatchConflationEnabled(isBatchConflationEnabled);
    asyncEventQueueFactory.setBatchSize(batchSize);
    asyncEventQueueFactory.setDispatcherThreads(dispatcherThreads);
    asyncEventQueueFactory.setMaximumQueueMemory(maximumQueueMemory);
    asyncEventQueueFactory.setParallel(isParallel);
    asyncEventQueueFactory.setPersistent(false);

    asyncEventQueueFactory.create(asyncEventQueueId, asyncEventListener);
  }

  private void doPuts(String regionName, int numPuts) {
    Region<Integer, Integer> region = getCache().getRegion(regionName);
    for (int i = 0; i < numPuts; i++) {
      region.put(i, i);
    }
  }

  private void doRebalance() throws InterruptedException, TimeoutException {
    ResourceManager resourceManager = getCache().getResourceManager();
    RebalanceFactory rebalanceFactory = resourceManager.createRebalanceFactory();
    RebalanceOperation rebalanceOperation = rebalanceFactory.start();
    RebalanceResults rebalanceResults = rebalanceOperation.getResults(2, MINUTES);
    assertThat(rebalanceResults).isNotNull();
  }

  private Set<Integer> getAllLocalPrimaryBucketIds(String regionName) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(regionName);
    return region.getDataStore().getAllLocalPrimaryBucketIds();
  }

  private void putGivenKeyValue(String regionName, Map<Integer, ?> keyValues) {
    Region<Integer, Object> region = getCache().getRegion(regionName);
    for (int key : keyValues.keySet()) {
      region.put(key, keyValues.get(key));
    }
  }

  private boolean areRegionQueuesEmpty(InternalGatewaySender gatewaySender) {
    assertThat(gatewaySender).isNotNull();

    Set<RegionQueue> regionQueues = gatewaySender.getQueues();
    int totalSize = 0;
    for (RegionQueue regionQueue : regionQueues) {
      totalSize += regionQueue.size();
    }
    return totalSize == 0;
  }

  private void validateAsyncEventQueueStats(int queueSize,
      int secondaryQueueSize,
      int eventsReceived,
      int eventsQueued,
      int eventsDistributed) {
    InternalAsyncEventQueue asyncEventQueue = getInternalAsyncEventQueue();
    AsyncEventQueueStats asyncEventQueueStats = asyncEventQueue.getStatistics();
    assertThat(asyncEventQueueStats).isNotNull();

    await()
        .untilAsserted(
            () -> assertThat(asyncEventQueueStats.getEventQueueSize()).isEqualTo(queueSize));

    if (asyncEventQueue.isParallel()) {
      await()
          .untilAsserted(() -> assertThat(asyncEventQueueStats.getSecondaryEventQueueSize())
              .isEqualTo(secondaryQueueSize));

    } else {
      // for serial queue, secondaryQueueSize is not used
      assertThat(asyncEventQueueStats.getSecondaryEventQueueSize()).isEqualTo(0);
    }

    assertThat(asyncEventQueueStats.getEventQueueSize()).isEqualTo(queueSize);
    assertThat(asyncEventQueueStats.getEventsReceived()).isEqualTo(eventsReceived);
    assertThat(asyncEventQueueStats.getEventsQueued()).isEqualTo(eventsQueued);
    assertThat(asyncEventQueueStats.getEventsDistributed())
        .isGreaterThanOrEqualTo(eventsDistributed);
  }

  private void validateParallelAsyncEventQueueSize(int expectedRegionQueueSize) {
    InternalGatewaySender gatewaySender = getInternalGatewaySender();

    Set<RegionQueue> regionQueues = gatewaySender.getQueues();
    assertThat(regionQueues).isNotEmpty().hasSize(1);

    Region<?, ?> region = regionQueues.iterator().next().getRegion();
    assertThat(region.size()).isEqualTo(expectedRegionQueueSize);
  }

  private void validatePossibleDuplicateEvents(int numEvents) {
    PossibleDuplicateAsyncEventListener listener = getPossibleDuplicateAsyncEventListener();

    // Verify all events were processed
    assertThat(listener.getTotalEvents()).isEqualTo(numEvents);

    // Verify all events are possibleDuplicate
    assertThat(listener.getTotalPossibleDuplicateEvents()).isEqualTo(numEvents);
  }

  private void validatePossibleDuplicateEvents(Set<Integer> bucketIds, int batchSize) {
    assertThat(bucketIds.size()).isGreaterThan(1);

    Map<Integer, List<GatewaySenderEventImpl>> bucketToEventsMap =
        getGatewaySenderAsyncEventListener().getBucketToEventsMap();

    for (int bucketId : bucketIds) {
      List<GatewaySenderEventImpl> eventsForBucket = bucketToEventsMap.get(bucketId);
      assertThat(eventsForBucket).as("bucketToEventsMap: " + bucketToEventsMap).isNotNull()
          .hasSize(batchSize);

      for (int i = 0; i < batchSize; i++) {
        GatewaySenderEventImpl gatewaySenderEvent = eventsForBucket.get(i);
        assertThat(gatewaySenderEvent.getPossibleDuplicate()).isTrue();
      }
    }
  }

  private void validateSpyAsyncEventListenerEventsMap(int expectedSize) {
    Map eventsMap = (Map<?, ?>) getSpyAsyncEventListener().getEventsMap();
    await().untilAsserted(() -> assertThat(eventsMap).hasSize(expectedSize));
  }

  private void waitForAsyncQueueToEmpty() {
    InternalGatewaySender gatewaySender = getInternalGatewaySender();
    await().until(() -> areRegionQueuesEmpty(gatewaySender));
  }

  private void waitForDispatcherToPause() {
    getInternalGatewaySender().getEventProcessor().waitForDispatcherToPause();
  }

  private void waitForParallelAsyncEventQueueSize(int expectedRegionQueueSize) {
    InternalGatewaySender gatewaySender = getInternalGatewaySender();

    await().untilAsserted(() -> {
      Set<RegionQueue> regionQueues = gatewaySender.getQueues();
      assertThat(regionQueues).isNotEmpty().hasSize(1);

      Region<?, ?> region = regionQueues.iterator().next().getRegion();
      assertThat(region.size()).isEqualTo(expectedRegionQueueSize);
    });
  }

  private void waitForPrimaryToMove() {
    await().until(() -> getPrimaryMovingAsyncEventListener().isMoved());
  }

  private InternalGatewaySender getInternalGatewaySender() {
    InternalGatewaySender gatewaySender = getInternalAsyncEventQueue().getSender();
    assertThat(gatewaySender).isNotNull();
    return gatewaySender;
  }

  private PrimaryMovingAsyncEventListener getPrimaryMovingAsyncEventListener() {
    return (PrimaryMovingAsyncEventListener) getAsyncEventListener();
  }

  private BucketMovingAsyncEventListener getBucketMovingAsyncEventListener() {
    return (BucketMovingAsyncEventListener) getAsyncEventListener();
  }

  private SpyAsyncEventListener getSpyAsyncEventListener() {
    return (SpyAsyncEventListener) getAsyncEventListener();
  }

  private GatewaySenderAsyncEventListener getGatewaySenderAsyncEventListener() {
    return (GatewaySenderAsyncEventListener) getAsyncEventListener();
  }

  private PossibleDuplicateAsyncEventListener getPossibleDuplicateAsyncEventListener() {
    return (PossibleDuplicateAsyncEventListener) getAsyncEventListener();
  }

  private AsyncEventListener getAsyncEventListener() {
    AsyncEventListener asyncEventListener = getAsyncEventQueue().getAsyncEventListener();
    assertThat(asyncEventListener).isNotNull();
    return asyncEventListener;
  }

  private InternalAsyncEventQueue getInternalAsyncEventQueue() {
    return (InternalAsyncEventQueue) getAsyncEventQueue();
  }

  private AsyncEventQueue getAsyncEventQueue() {
    AsyncEventQueue value = null;

    Set<AsyncEventQueue> asyncEventQueues = getCache().getAsyncEventQueues();
    for (AsyncEventQueue asyncEventQueue : asyncEventQueues) {
      if (asyncEventQueueId.equals(asyncEventQueue.getId())) {
        value = asyncEventQueue;
      }
    }

    assertThat(value).isNotNull();
    return value;
  }

  private abstract static class AbstractMovingAsyncEventListener implements AsyncEventListener {

    private final DistributedMember destination;
    private final Set<Object> keysSeen = synchronizedSet(new HashSet<>());
    private volatile boolean moved;

    AbstractMovingAsyncEventListener(final DistributedMember destination) {
      this.destination = destination;
    }

    @Override
    public boolean processEvents(final List<AsyncEvent> events) {
      if (!moved) {

        AsyncEvent event1 = events.get(0);
        move(event1);
        moved = true;
        return false;
      }

      Set<Object> keysInThisBatch = events.stream()
          .map(AsyncEvent::getKey)
          .collect(toSet());
      keysSeen.addAll(keysInThisBatch);
      return true;
    }

    DistributedMember getDestination() {
      return destination;
    }

    Set<Object> getKeysSeen() {
      return keysSeen;
    }

    boolean isMoved() {
      return moved;
    }

    abstract void move(AsyncEvent event);
  }

  private static class BucketMovingAsyncEventListener extends AbstractMovingAsyncEventListener {

    private final Cache cache;
    private final String regionName;

    BucketMovingAsyncEventListener(final DistributedMember destination, final Cache cache,
        final String regionName) {
      super(destination);
      this.cache = cache;
      this.regionName = regionName;
    }

    @Override
    protected void move(final AsyncEvent event) {
      Object key = event.getKey();
      Region<Object, Object> region = cache.getRegion(regionName);
      DistributedMember source = cache.getDistributedSystem().getDistributedMember();
      PartitionRegionHelper.moveBucketByKey(region, source, getDestination(), key);
    }
  }

  private static class GatewaySenderAsyncEventListener implements AsyncEventListener {

    private final Map<Integer, List<GatewaySenderEventImpl>> bucketToEventsMap = new HashMap<>();

    Map<Integer, List<GatewaySenderEventImpl>> getBucketToEventsMap() {
      assertThat(bucketToEventsMap).isNotNull();
      return bucketToEventsMap;
    }

    @Override
    public synchronized boolean processEvents(List<AsyncEvent> events) {
      for (AsyncEvent event : events) {
        GatewaySenderEventImpl gatewayEvent = (GatewaySenderEventImpl) event;
        int bucketId = gatewayEvent.getBucketId();
        List<GatewaySenderEventImpl> bucketEvents = bucketToEventsMap.get(bucketId);
        if (bucketEvents == null) {
          bucketEvents = new ArrayList<>();
          bucketEvents.add(gatewayEvent);
          bucketToEventsMap.put(bucketId, bucketEvents);
        } else {
          bucketEvents.add(gatewayEvent);
        }
      }
      return true;
    }
  }

  private static class PossibleDuplicateAsyncEventListener implements AsyncEventListener {

    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicInteger numberOfEvents = new AtomicInteger();
    private final AtomicInteger numberOfPossibleDuplicateEvents = new AtomicInteger();

    private void process(final AsyncEvent<?, ?> event) {
      if (event.getPossibleDuplicate()) {
        incrementTotalPossibleDuplicateEvents();
      }
      incrementTotalEvents();
    }

    private void waitToStartProcessingEvents() throws InterruptedException {
      latch.await(2, MINUTES);
    }

    private void incrementTotalEvents() {
      numberOfEvents.incrementAndGet();
    }

    private void incrementTotalPossibleDuplicateEvents() {
      numberOfPossibleDuplicateEvents.incrementAndGet();
    }

    void startProcessingEvents() {
      latch.countDown();
    }

    int getTotalEvents() {
      return numberOfEvents.get();
    }

    int getTotalPossibleDuplicateEvents() {
      return numberOfPossibleDuplicateEvents.get();
    }

    @Override
    public boolean processEvents(final List<AsyncEvent> events) {
      try {
        waitToStartProcessingEvents();
      } catch (InterruptedException e) {
        throw new Error(e);
      }
      for (AsyncEvent<?, ?> event : events) {
        process(event);
      }
      return true;
    }
  }

  private static class PrimaryMovingAsyncEventListener extends AbstractMovingAsyncEventListener {

    PrimaryMovingAsyncEventListener(final DistributedMember destination) {
      super(destination);
    }

    @Override
    protected void move(final AsyncEvent event) {
      Object key = event.getKey();
      PartitionedRegion region = (PartitionedRegion) event.getRegion();

      BecomePrimaryBucketResponse response =
          BecomePrimaryBucketMessage.send((InternalDistributedMember) getDestination(), region,
              region.getKeyInfo(key).getBucketId(), true);
      assertThat(response).isNotNull();
      assertThat(response.waitForResponse()).isTrue();
    }
  }

  private static class SimpleFixedPartitionResolver implements FixedPartitionResolver {

    private final List<String> allPartitions;

    SimpleFixedPartitionResolver(final List<String> allPartitions) {
      this.allPartitions = allPartitions;
    }

    @Override
    public String getPartitionName(final EntryOperation opDetails,
        @Deprecated final Set targetPartitions) {
      int hash = Math.abs(opDetails.getKey().hashCode() % allPartitions.size());
      return allPartitions.get(hash);
    }

    @Override
    public Object getRoutingObject(final EntryOperation opDetails) {
      return opDetails.getKey();
    }

    @Override
    public String getName() {
      return getClass().getName();
    }
  }

  private static class SpyAsyncEventListener<K, V> implements AsyncEventListener, Declarable {

    private final Map<K, V> eventsMap = new ConcurrentHashMap<>();

    Map<K, V> getEventsMap() {
      assertThat(eventsMap).isNotNull();
      return eventsMap;
    }

    @Override
    public boolean processEvents(List<AsyncEvent> events) {
      for (AsyncEvent<K, V> event : events) {
        eventsMap.put(event.getKey(), event.getDeserializedValue());
      }
      return true;
    }
  }
}
