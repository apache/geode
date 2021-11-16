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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.partitioned.InternalPRInfo;
import org.apache.geode.internal.cache.partitioned.LoadProbe;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionRebalanceOp;
import org.apache.geode.internal.cache.partitioned.PersistentBucketRecoverer;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.partitioned.rebalance.RebalanceDirector;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class PRHARedundancyProviderTest {

  private InternalCache cache;
  private PartitionedRegion partitionedRegion;
  private InternalResourceManager resourceManager;

  private PRHARedundancyProvider prHaRedundancyProvider;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() {
    cache = mock(InternalCache.class);
    partitionedRegion = mock(PartitionedRegion.class);
    resourceManager = mock(InternalResourceManager.class);
  }

  @Test
  public void waitForPersistentBucketRecoveryProceedsWhenPersistentBucketRecovererLatchIsNotSet() {
    prHaRedundancyProvider = new PRHARedundancyProvider(partitionedRegion, resourceManager,
        (a, b) -> mock(PersistentBucketRecoverer.class));

    prHaRedundancyProvider.waitForPersistentBucketRecovery();
  }

  @Test
  public void waitForPersistentBucketRecoveryProceedsAfterLatchCountDown() {
    when(cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME, true))
        .thenReturn(mock(DistributedRegion.class));
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(partitionedRegion.getDataPolicy()).thenReturn(DataPolicy.PARTITION);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(mock(PartitionAttributes.class));
    prHaRedundancyProvider = new PRHARedundancyProvider(partitionedRegion, resourceManager,
        (a, b) -> spy(new ThreadlessPersistentBucketRecoverer(a, b)));
    prHaRedundancyProvider.createPersistentBucketRecoverer(1);
    prHaRedundancyProvider.getPersistentBucketRecoverer().countDown();

    prHaRedundancyProvider.waitForPersistentBucketRecovery();

    verify(prHaRedundancyProvider.getPersistentBucketRecoverer()).await();
  }

  @Test
  public void buildPartitionedRegionInfo() {
    prHaRedundancyProvider = new PRHARedundancyProvider(partitionedRegion, resourceManager,
        (a, b) -> mock(PersistentBucketRecoverer.class));
    when(partitionedRegion.getRedundancyTracker())
        .thenReturn(mock(PartitionedRegionRedundancyTracker.class));
    when(partitionedRegion.getRedundancyTracker().getActualRedundancy()).thenReturn(33);
    when(partitionedRegion.getRedundancyTracker().getLowRedundancyBuckets()).thenReturn(3);
    when(partitionedRegion.getRedundantCopies()).thenReturn(12);
    when(partitionedRegion.getRegionAdvisor()).thenReturn(mock(RegionAdvisor.class));
    when(partitionedRegion.getRegionAdvisor().adviseDataStore()).thenReturn(new HashSet<>());
    when(partitionedRegion.getRegionAdvisor().getCreatedBucketsCount()).thenReturn(17);
    when(partitionedRegion.getTotalNumberOfBuckets()).thenReturn(42);

    InternalPRInfo internalPRInfo =
        prHaRedundancyProvider.buildPartitionedRegionInfo(false, mock(LoadProbe.class));

    assertThat(internalPRInfo.getConfiguredBucketCount()).isEqualTo(42);
    assertThat(internalPRInfo.getCreatedBucketCount()).isEqualTo(17);
    assertThat(internalPRInfo.getLowRedundancyBucketCount()).isEqualTo(3);
    assertThat(internalPRInfo.getConfiguredRedundantCopies()).isEqualTo(12);
    assertThat(internalPRInfo.getActualRedundantCopies()).isEqualTo(33);
  }

  @Test
  public void reportsStartupTaskToResourceManager() {
    @SuppressWarnings("unchecked")
    CompletableFuture<Void> providerStartupTask = mock(CompletableFuture.class);
    when(partitionedRegion.getRegionAdvisor()).thenReturn(mock(RegionAdvisor.class));
    when(partitionedRegion.getPartitionAttributes()).thenReturn(mock(PartitionAttributes.class));
    when(partitionedRegion.isDataStore()).thenReturn(true);
    when(resourceManager.getExecutor()).thenReturn(mock(ScheduledExecutorService.class));

    prHaRedundancyProvider = new PRHARedundancyProvider(partitionedRegion, resourceManager,
        (a, b) -> mock(PersistentBucketRecoverer.class),
        PRHARedundancyProviderTest::createRebalanceOp, providerStartupTask);

    prHaRedundancyProvider.startRedundancyRecovery();

    verify(resourceManager).addStartupTask(same(providerStartupTask));
  }

  @Test
  public void doNotNotReportStartupTaskIfRecoveryDelayIsNegative() {
    when(partitionedRegion.getRegionAdvisor()).thenReturn(mock(RegionAdvisor.class));
    PartitionAttributes partitionAttributes = mock(PartitionAttributes.class);
    when(partitionAttributes.getStartupRecoveryDelay()).thenReturn(-1L);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(resourceManager.getExecutor()).thenReturn(mock(ScheduledExecutorService.class));

    prHaRedundancyProvider = new PRHARedundancyProvider(partitionedRegion, resourceManager,
        (a, b) -> mock(PersistentBucketRecoverer.class),
        PRHARedundancyProviderTest::createRebalanceOp, null);

    prHaRedundancyProvider.startRedundancyRecovery();

    verify(resourceManager, never()).addStartupTask(any());
  }

  @Test
  public void doNotNotReportStartupTaskIfPartitionIsNotDataStore() {
    when(partitionedRegion.getRegionAdvisor()).thenReturn(mock(RegionAdvisor.class));
    PartitionAttributes partitionAttributes = mock(PartitionAttributes.class);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(partitionedRegion.isDataStore()).thenReturn(false);
    when(resourceManager.getExecutor()).thenReturn(mock(ScheduledExecutorService.class));

    prHaRedundancyProvider = new PRHARedundancyProvider(partitionedRegion, resourceManager,
        (a, b) -> mock(PersistentBucketRecoverer.class),
        PRHARedundancyProviderTest::createRebalanceOp, null);

    prHaRedundancyProvider.startRedundancyRecovery();

    verify(resourceManager, never()).addStartupTask(any());
  }

  @Test
  public void doNotNotReportStartupTaskIfExecutorRejectsRebalanceTask() {
    when(partitionedRegion.getRegionAdvisor()).thenReturn(mock(RegionAdvisor.class));
    PartitionAttributes partitionAttributes = mock(PartitionAttributes.class);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(partitionedRegion.isDataStore()).thenReturn(true);
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    when(executor.schedule(any(Runnable.class), anyLong(), any()))
        .thenThrow(new RejectedExecutionException("Rejected for the test"));
    when(resourceManager.getExecutor()).thenReturn(executor);

    prHaRedundancyProvider = new PRHARedundancyProvider(partitionedRegion, resourceManager,
        (a, b) -> mock(PersistentBucketRecoverer.class),
        PRHARedundancyProviderTest::createRebalanceOp, null);

    prHaRedundancyProvider.startRedundancyRecovery();

    verify(resourceManager, never()).addStartupTask(any());
  }

  @Test
  public void doNotNotReportStartupTaskIfIsHasShutDown() {
    when(partitionedRegion.getRegionAdvisor()).thenReturn(mock(RegionAdvisor.class));
    PartitionAttributes partitionAttributes = mock(PartitionAttributes.class);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(partitionedRegion.isDataStore()).thenReturn(true);
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    when(resourceManager.getExecutor()).thenReturn(executor);

    prHaRedundancyProvider = new PRHARedundancyProvider(partitionedRegion, resourceManager,
        (a, b) -> mock(PersistentBucketRecoverer.class),
        PRHARedundancyProviderTest::createRebalanceOp, null);

    prHaRedundancyProvider.shutdown();
    prHaRedundancyProvider.startRedundancyRecovery();

    verify(resourceManager, never()).addStartupTask(any());
  }

  @Test
  public void completesStartupTaskWhenRedundancyRecovered() {
    DistributedSystem distributedSystem = mock(DistributedSystem.class);
    when(distributedSystem.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    InternalCache cache = mock(InternalCache.class);
    when(cache.getDistributedSystem()).thenReturn(distributedSystem);

    when(partitionedRegion.getGemFireCache()).thenReturn(cache);
    when(partitionedRegion.getRegionAdvisor()).thenReturn(mock(RegionAdvisor.class));
    when(partitionedRegion.getPartitionAttributes()).thenReturn(mock(PartitionAttributes.class));
    when(partitionedRegion.isDataStore()).thenReturn(true);
    when(partitionedRegion.getPrStats()).thenReturn(mock(PartitionedRegionStats.class));

    ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
    when(resourceManager.getExecutor()).thenReturn(executorService);

    when(executorService.schedule(any(Runnable.class), anyLong(), any()))
        .thenAnswer(runTheRunnable());

    @SuppressWarnings("unchecked")
    CompletableFuture<Void> providerStartupTask = mock(CompletableFuture.class);

    prHaRedundancyProvider = new PRHARedundancyProvider(partitionedRegion, resourceManager,
        (a, b) -> mock(PersistentBucketRecoverer.class),
        PRHARedundancyProviderTest::createRebalanceOp, providerStartupTask);

    prHaRedundancyProvider.startRedundancyRecovery();

    verify(providerStartupTask).complete(any());
  }

  @Test
  @Parameters({"RUNTIME", "CANCEL", "REGION_DESTROYED"})
  @TestCaseName("{method}[{index}]: {params}")
  public void startTaskCompletesExceptionallyIfExceptionIsThrown(
      ExceptionToThrow exceptionToThrow) {
    DistributedSystem distributedSystem = mock(DistributedSystem.class);
    when(distributedSystem.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    InternalCache cache = mock(InternalCache.class);
    when(cache.getDistributedSystem()).thenReturn(distributedSystem);

    when(partitionedRegion.getGemFireCache()).thenReturn(cache);
    when(partitionedRegion.getRegionAdvisor()).thenReturn(mock(RegionAdvisor.class));
    when(partitionedRegion.getPartitionAttributes()).thenReturn(mock(PartitionAttributes.class));
    when(partitionedRegion.isDataStore()).thenReturn(true);
    when(partitionedRegion.getPrStats()).thenReturn(mock(PartitionedRegionStats.class));

    ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
    when(resourceManager.getExecutor()).thenReturn(executorService);

    when(executorService.schedule(any(Runnable.class), anyLong(), any()))
        .thenAnswer(runTheRunnable());

    @SuppressWarnings("unchecked")
    CompletableFuture<Void> providerStartupTask = mock(CompletableFuture.class);

    Exception exception = exceptionToThrow.getException();

    prHaRedundancyProvider = new PRHARedundancyProvider(partitionedRegion, resourceManager,
        (a, b) -> mock(PersistentBucketRecoverer.class),
        (a, b, c, d, e) -> createThrowingRebalanceOp(exception), providerStartupTask);

    prHaRedundancyProvider.startRedundancyRecovery();

    verify(providerStartupTask, never()).complete(any());
    verify(providerStartupTask).completeExceptionally(exception);
  }

  @Test
  public void reportsStartupTaskToResourceManagerForColocatedRegionsAndColocationCompleted() {
    @SuppressWarnings("unchecked")
    CompletableFuture<Void> providerStartupTask = mock(CompletableFuture.class);
    when(partitionedRegion.getColocatedWith()).thenReturn("region2");
    InternalCache cache = mock(InternalCache.class);
    PartitionRegionConfig partitionRegionConfig = mock(PartitionRegionConfig.class);
    DistributedRegion prRoot = mock(DistributedRegion.class);
    when(cache.getRegion(anyString(), anyBoolean())).thenReturn(prRoot);
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(prRoot.get(any())).thenReturn(partitionRegionConfig);
    when(partitionRegionConfig.isColocationComplete()).thenReturn(true);
    when(partitionedRegion.getRegionAdvisor()).thenReturn(mock(RegionAdvisor.class));
    when(partitionedRegion.getPartitionAttributes()).thenReturn(mock(PartitionAttributes.class));
    when(partitionedRegion.isDataStore()).thenReturn(true);
    when(resourceManager.getExecutor()).thenReturn(mock(ScheduledExecutorService.class));

    prHaRedundancyProvider = new PRHARedundancyProvider(partitionedRegion, resourceManager,
        (a, b) -> mock(PersistentBucketRecoverer.class),
        PRHARedundancyProviderTest::createRebalanceOp, providerStartupTask);

    prHaRedundancyProvider.startRedundancyRecovery();

    verify(resourceManager).addStartupTask(same(providerStartupTask));
  }

  @Test
  public void doNotNotReportStartupTaskForColocatedRegionsAndColocationNotCompleted() {
    CompletableFuture<Void> providerStartupTask = mock(CompletableFuture.class);
    when(partitionedRegion.getColocatedWith()).thenReturn("region2");
    InternalCache cache = mock(InternalCache.class);
    PartitionRegionConfig partitionRegionConfig = mock(PartitionRegionConfig.class);
    DistributedRegion prRoot = mock(DistributedRegion.class);
    when(cache.getRegion(anyString(), anyBoolean())).thenReturn(prRoot);
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(prRoot.get(any())).thenReturn(partitionRegionConfig);
    when(partitionRegionConfig.isColocationComplete()).thenReturn(false);
    when(resourceManager.getExecutor()).thenReturn(mock(ScheduledExecutorService.class));

    prHaRedundancyProvider = new PRHARedundancyProvider(partitionedRegion, resourceManager,
        (a, b) -> mock(PersistentBucketRecoverer.class),
        PRHARedundancyProviderTest::createRebalanceOp, providerStartupTask);

    prHaRedundancyProvider.startRedundancyRecovery();

    verify(resourceManager, never()).addStartupTask(any());
  }

  private static PartitionedRegionRebalanceOp createRebalanceOp(PartitionedRegion region,
      boolean simulate, RebalanceDirector director, boolean replaceOfflineData,
      boolean isRebalance) {
    return mock(PartitionedRegionRebalanceOp.class);
  }

  private static PartitionedRegionRebalanceOp createThrowingRebalanceOp(Exception exception) {
    PartitionedRegionRebalanceOp rebalanceOp = mock(PartitionedRegionRebalanceOp.class);
    when(rebalanceOp.execute()).thenThrow(exception);
    return rebalanceOp;
  }

  private enum ExceptionToThrow {
    RUNTIME(new RuntimeException("Runtime error")),
    CANCEL(new CacheClosedException("Cache closed")),
    REGION_DESTROYED(new RegionDestroyedException("Region destroyed", SEPARATOR + "Region"));

    private final Exception exception;

    ExceptionToThrow(Exception e) {
      exception = e;
    }

    Exception getException() {
      return exception;
    }
  }

  private static class ThreadlessPersistentBucketRecoverer extends PersistentBucketRecoverer {

    ThreadlessPersistentBucketRecoverer(
        PRHARedundancyProvider prhaRedundancyProvider, int proxyBuckets) {
      super(prhaRedundancyProvider, proxyBuckets);
    }

    @Override
    public void startLoggingThread() {
      // do nothing
    }

  }

  private static Answer<?> runTheRunnable() {
    return (Answer<Object>) invocation -> {
      ((Runnable) invocation.getArgument(0)).run();
      return null;
    };
  }
}
