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
package org.apache.geode.internal.cache.partitioned;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.persistence.ConflictingPersistentDataException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserver;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import org.apache.geode.internal.cache.persistence.PersistenceAdvisorImpl;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Extracted from {@link PersistentPartitionedRegionDistributedTest}.
 */
@SuppressWarnings("serial")
public class PRDiskConflictWithColocationDistributedTest implements Serializable {

  private String partitionedRegionName;
  private String childRegionName;

  private VM vm0;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedDiskDirRule diskDirRule = new DistributedDiskDirRule();

  @Before
  public void setUp() {
    vm0 = getVM(0);

    String uniqueName = getClass().getSimpleName() + "-" + testName.getMethodName();
    partitionedRegionName = uniqueName + "-partitionedRegion";

    // NOTE: tests will fail if childRegionName contains "_" character (not sure why)
    childRegionName = uniqueName + "-childRegion";

    // Without this system property, CacheClosedException with cause of
    // ConflictingPersistentDataException is replaced by DistributedSystemDisconnectedException
    System.setProperty(GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE", "true");
  }

  @After
  public void tearDown() {
    invokeInEveryVM(() -> InternalResourceManager.setResourceObserver(null));
    System.clearProperty(GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE");
  }

  @Test
  public void closesCacheWhenColocatedRegionThrowsConflictingPersistentDataException()
      throws Exception {
    // create some buckets
    createColocatedPR(1);
    createData(0, 2, "a", partitionedRegionName);
    createData(0, 2, "a", childRegionName);

    getCache().getRegion(childRegionName).close();
    getCache().getRegion(partitionedRegionName).close();

    vm0.invoke(() -> {
      createColocatedPR(1);
      // create an overlapping bucket
      createData(2, 4, "a", partitionedRegionName);
      createData(2, 4, "a", childRegionName);
    });

    addIgnoredException(ConflictingPersistentDataException.class);
    addIgnoredException(CacheClosedException.class);

    // Cache should have closed due to ConflictingPersistentDataException
    createColocatedPRWithObserver(1);

    await().until(() -> basicGetCache().isClosed());

    Throwable throwable =
        catchThrowable(() -> basicGetCache().getCancelCriterion().checkCancelInProgress(null));
    assertThat(throwable).isInstanceOf(CacheClosedException.class)
        .hasCauseInstanceOf(ConflictingPersistentDataException.class);
  }

  private void createColocatedPR(final int redundantCopies) throws InterruptedException {
    // Wait for both nested PRs to be created
    CountDownLatch recoveryDone = new CountDownLatch(2);

    ResourceObserver observer = new ResourceObserverAdapter() {
      @Override
      public void recoveryFinished(final Region region) {
        recoveryDone.countDown();
      }
    };

    InternalResourceManager.setResourceObserver(observer);

    // Parent Region
    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(redundantCopies);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(partitionedRegionName);

    // Colocated region
    partitionAttributesFactory.setColocatedWith(partitionedRegionName);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(childRegionName);

    recoveryDone.await(2, MINUTES);
  }

  private void createColocatedPRWithObserver(final int redundantCopies)
      throws InterruptedException {
    // Wait for both nested PRs to be created
    CountDownLatch recoveryDone = new CountDownLatch(2);

    ResourceObserver observer = new ResourceObserverAdapter() {
      @Override
      public void recoveryFinished(final Region region) {
        recoveryDone.countDown();
      }
    };

    InternalResourceManager.setResourceObserver(observer);

    // Wait for parent and child region to be created.
    // And throw exception while region is getting initialized.
    CountDownLatch childRegionCreated = new CountDownLatch(1);

    PersistenceAdvisorImpl.setPersistenceAdvisorObserver(regionPath -> {
      if (regionPath.contains(childRegionName)) {
        try {
          childRegionCreated.await(2, MINUTES);
        } catch (InterruptedException e) {
          throw new Error(e);
        }
        throw new ConflictingPersistentDataException(
            "Testing Cache Close with ConflictingPersistentDataException for region " + regionPath);
      }
    });

    try {
      // Parent Region
      PartitionAttributesFactory<?, ?> partitionAttributesFactory =
          new PartitionAttributesFactory();
      partitionAttributesFactory.setRedundantCopies(redundantCopies);

      RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION_PERSISTENT);
      regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
      regionFactory.create(partitionedRegionName);

      // Colocated region
      partitionAttributesFactory.setColocatedWith(partitionedRegionName);
      regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

      regionFactory.create(childRegionName);

      // Count down on region create.
      childRegionCreated.countDown();

      recoveryDone.await(2, MINUTES);
    } finally {
      PersistenceAdvisorImpl.setPersistenceAdvisorObserver(null);
    }
  }

  private void createData(final int startKey, final int endKey, final String value,
      final String regionName) {
    Region<Integer, String> region = getCache().getRegion(regionName);
    for (int i = startKey; i < endKey; i++) {
      region.put(i, value);
    }
  }

  private InternalCache getCache() {
    return cacheRule.getOrCreateCache();
  }

  private InternalCache basicGetCache() {
    return cacheRule.getCache();
  }
}
