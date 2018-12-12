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
package org.apache.geode.cache.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.partition.PartitionMemberInfo;
import org.apache.geode.cache.util.AutoBalancer.AuditScheduler;
import org.apache.geode.cache.util.AutoBalancer.CacheOperationFacade;
import org.apache.geode.cache.util.AutoBalancer.GeodeCacheFacade;
import org.apache.geode.cache.util.AutoBalancer.OOBAuditor;
import org.apache.geode.cache.util.AutoBalancer.SizeBasedOOBAuditor;
import org.apache.geode.cache.util.AutoBalancer.TimeProvider;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PRHARedundancyProvider;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.partitioned.InternalPRInfo;
import org.apache.geode.internal.cache.partitioned.LoadProbe;

/**
 * UnitTests for AutoBalancer. All collaborators should be mocked.
 */
public class AutoBalancerJUnitTest {
  private TimeProvider mockClock;
  private OOBAuditor mockAuditor;
  private AuditScheduler mockScheduler;
  private CacheOperationFacade mockCacheFacade;

  @Before
  public void setupMock() {
    mockClock = mock(TimeProvider.class);
    mockAuditor = mock(OOBAuditor.class);
    mockScheduler = mock(AuditScheduler.class);
    mockCacheFacade = mock(CacheOperationFacade.class);
  }

  private static Properties getBasicConfig() {
    Properties props = new Properties();
    // every second schedule
    props.put(AutoBalancer.SCHEDULE, "* 0/30 * * * ?");
    return props;
  }

  private GeodeCacheFacade getFacadeForResourceManagerOps(final boolean simulate) throws Exception {
    final GemFireCacheImpl mockCache = mock(GemFireCacheImpl.class);
    final InternalResourceManager mockRM = mock(InternalResourceManager.class);
    final RebalanceFactory mockRebalanceFactory = mock(RebalanceFactory.class);
    final RebalanceOperation mockRebalanceOperation = mock(RebalanceOperation.class);
    final RebalanceResults mockRebalanceResults = mock(RebalanceResults.class);

    when(mockCache.isClosed()).thenReturn(false);
    when(mockCache.getResourceManager()).thenReturn(mockRM);
    when(mockRM.createRebalanceFactory()).thenReturn(mockRebalanceFactory);
    when(mockRebalanceFactory.start()).thenReturn(mockRebalanceOperation);
    when(mockRebalanceFactory.simulate()).thenReturn(mockRebalanceOperation);
    when(mockRebalanceOperation.getResults()).thenReturn(mockRebalanceResults);
    if (simulate)
      when(mockRebalanceResults.getTotalBucketTransferBytes()).thenReturn(12345L);

    return new GeodeCacheFacade(mockCache);
  }

  @Test
  public void testLockStatExecuteInSequence() {
    when(mockCacheFacade.acquireAutoBalanceLock()).thenReturn(true);
    when(mockCacheFacade.getTotalTransferSize()).thenReturn(0L);

    AutoBalancer balancer = new AutoBalancer(null, null, null, mockCacheFacade);
    balancer.getOOBAuditor().execute();
    InOrder inOrder = Mockito.inOrder(mockCacheFacade);
    inOrder.verify(mockCacheFacade, times(1)).acquireAutoBalanceLock();
    inOrder.verify(mockCacheFacade, times(1)).incrementAttemptCounter();
    inOrder.verify(mockCacheFacade, times(1)).getTotalTransferSize();
  }

  @Test
  public void testAcquireLockAfterReleasedRemotely() {
    when(mockCacheFacade.getTotalTransferSize()).thenReturn(0L);
    when(mockCacheFacade.acquireAutoBalanceLock()).thenReturn(false, true);

    AutoBalancer balancer = new AutoBalancer(null, null, null, mockCacheFacade);
    balancer.getOOBAuditor().execute();
    balancer.getOOBAuditor().execute();
    InOrder inOrder = Mockito.inOrder(mockCacheFacade);
    inOrder.verify(mockCacheFacade, times(2)).acquireAutoBalanceLock();
    inOrder.verify(mockCacheFacade, times(1)).incrementAttemptCounter();
    inOrder.verify(mockCacheFacade, times(1)).getTotalTransferSize();
  }

  @Test
  public void testFailExecuteIfLockedElsewhere() {
    when(mockCacheFacade.acquireAutoBalanceLock()).thenReturn(false);

    AutoBalancer balancer = new AutoBalancer(null, null, null, mockCacheFacade);
    balancer.getOOBAuditor().execute();
    verify(mockCacheFacade, times(1)).acquireAutoBalanceLock();
  }

  @Test
  public void testNoCacheError() {
    AutoBalancer balancer = new AutoBalancer();
    OOBAuditor auditor = balancer.getOOBAuditor();
    assertThatThrownBy(auditor::execute).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void testOOBWhenBelowSizeThreshold() {
    final long totalSize = 1000L;
    final Map<PartitionedRegion, InternalPRInfo> details = new HashMap<>();
    when(mockCacheFacade.getRegionMemberDetails()).thenReturn(details);
    when(mockCacheFacade.getTotalDataSize(details)).thenReturn(totalSize);
    // First Run: half of threshold limit. Second Run: nothing to transfer.
    when(mockCacheFacade.getTotalTransferSize())
        .thenReturn((AutoBalancer.DEFAULT_SIZE_THRESHOLD_PERCENT * totalSize / 100) / 2, 0L);

    AutoBalancer balancer = new AutoBalancer(null, null, null, mockCacheFacade);
    Properties config = getBasicConfig();
    config.put(AutoBalancer.MINIMUM_SIZE, "10");
    balancer.initialize(null, config);
    SizeBasedOOBAuditor auditor = (SizeBasedOOBAuditor) balancer.getOOBAuditor();

    // First run
    assertThat(auditor.needsRebalancing()).isFalse();

    // Second run
    assertThat(auditor.needsRebalancing()).isFalse();
  }

  @Test
  public void testOOBWhenAboveThresholdButBelowMin() {
    final long totalSize = 1000L;
    // First Run: twice threshold. Second Run: more than total size.
    when(mockCacheFacade.getTotalTransferSize()).thenReturn(
        (AutoBalancer.DEFAULT_SIZE_THRESHOLD_PERCENT * totalSize / 100) / 2, 2 * totalSize);

    AutoBalancer balancer = new AutoBalancer(null, null, null, mockCacheFacade);
    Properties config = getBasicConfig();
    config.put(AutoBalancer.MINIMUM_SIZE, "" + (totalSize * 5));
    balancer.initialize(null, config);
    SizeBasedOOBAuditor auditor = (SizeBasedOOBAuditor) balancer.getOOBAuditor();

    // First run
    assertThat(auditor.needsRebalancing()).isFalse();

    // Second run
    assertThat(auditor.needsRebalancing()).isFalse();
  }

  @Test
  public void testOOBWhenAboveThresholdAndMin() {
    final long totalSize = 1000L;
    final Map<PartitionedRegion, InternalPRInfo> details = new HashMap<>();
    when(mockCacheFacade.getRegionMemberDetails()).thenReturn(details);
    when(mockCacheFacade.getTotalDataSize(details)).thenReturn(totalSize);
    // First Run: twice threshold. Second Run: more than total size.
    when(mockCacheFacade.getTotalTransferSize()).thenReturn(
        (AutoBalancer.DEFAULT_SIZE_THRESHOLD_PERCENT * totalSize / 100) * 2, 2 * totalSize);

    AutoBalancer balancer = new AutoBalancer(null, null, null, mockCacheFacade);
    Properties config = getBasicConfig();
    config.put(AutoBalancer.MINIMUM_SIZE, "10");
    balancer.initialize(null, config);
    SizeBasedOOBAuditor auditor = (SizeBasedOOBAuditor) balancer.getOOBAuditor();

    // First run
    assertThat(auditor.needsRebalancing()).isTrue();

    // Second run
    assertThat(auditor.needsRebalancing()).isTrue();
  }

  @Test
  public void testInvalidSchedule() {
    String someSchedule = "X Y * * * *";
    Properties props = new Properties();
    props.put(AutoBalancer.SCHEDULE, someSchedule);

    AutoBalancer autoR = new AutoBalancer();
    assertThatThrownBy(() -> autoR.initialize(null, props))
        .isInstanceOf(GemFireConfigException.class);
  }

  @Test
  public void testOOBAuditorInit() {
    AutoBalancer balancer = new AutoBalancer();
    balancer.initialize(null, getBasicConfig());
    SizeBasedOOBAuditor auditor = (SizeBasedOOBAuditor) balancer.getOOBAuditor();
    assertThat(auditor.getSizeThreshold()).isEqualTo(AutoBalancer.DEFAULT_SIZE_THRESHOLD_PERCENT);
    assertThat(auditor.getSizeMinimum()).isEqualTo(AutoBalancer.DEFAULT_MINIMUM_SIZE);

    Properties props = getBasicConfig();
    props.put(AutoBalancer.SIZE_THRESHOLD_PERCENT, "17");
    props.put(AutoBalancer.MINIMUM_SIZE, "10");
    balancer = new AutoBalancer();
    balancer.initialize(null, props);
    auditor = (SizeBasedOOBAuditor) balancer.getOOBAuditor();
    assertThat(auditor.getSizeThreshold()).isEqualTo(17);
    assertThat(auditor.getSizeMinimum()).isEqualTo(10);
  }

  @Test
  public void testConfigTransferThresholdNegative() {
    AutoBalancer balancer = new AutoBalancer();
    Properties props = getBasicConfig();
    props.put(AutoBalancer.SIZE_THRESHOLD_PERCENT, "-1");
    assertThatThrownBy(() -> balancer.initialize(null, props))
        .isInstanceOf(GemFireConfigException.class);
  }

  @Test
  public void testConfigSizeMinNegative() {
    AutoBalancer balancer = new AutoBalancer();
    Properties props = getBasicConfig();
    props.put(AutoBalancer.MINIMUM_SIZE, "-1");
    assertThatThrownBy(() -> balancer.initialize(null, props))
        .isInstanceOf(GemFireConfigException.class);
  }

  @Test
  public void testConfigTransferThresholdZero() {
    AutoBalancer balancer = new AutoBalancer();
    Properties props = getBasicConfig();
    props.put(AutoBalancer.SIZE_THRESHOLD_PERCENT, "0");
    assertThatThrownBy(() -> balancer.initialize(null, props))
        .isInstanceOf(GemFireConfigException.class);
  }

  @Test
  public void testConfigTransferThresholdTooHigh() {
    AutoBalancer balancer = new AutoBalancer();
    Properties props = getBasicConfig();
    props.put(AutoBalancer.SIZE_THRESHOLD_PERCENT, "100");
    assertThatThrownBy(() -> balancer.initialize(null, props))
        .isInstanceOf(GemFireConfigException.class);
  }

  @Test
  public void testAutoBalancerInit() {
    final String someSchedule = "1 * * * 1 *";
    final Properties props = new Properties();
    props.put(AutoBalancer.SCHEDULE, someSchedule);
    props.put(AutoBalancer.SIZE_THRESHOLD_PERCENT, 17);

    AutoBalancer autoR = new AutoBalancer(mockScheduler, mockAuditor, null, null);
    autoR.initialize(null, props);
    verify(mockAuditor, times(1)).init(props);
    verify(mockScheduler, times(1)).init(someSchedule);
  }

  @Test
  public void testMinimalConfiguration() {
    AutoBalancer autoR = new AutoBalancer();
    assertThatThrownBy(() -> autoR.initialize(null, null))
        .isInstanceOf(GemFireConfigException.class);

    Properties props = getBasicConfig();
    assertThatCode(() -> autoR.initialize(null, props)).doesNotThrowAnyException();
  }

  @Test
  public void testFacadeTotalTransferSize() throws Exception {
    assertThat(getFacadeForResourceManagerOps(true).getTotalTransferSize()).isEqualTo(12345);
  }

  @Test
  public void testFacadeRebalance() {
    assertThatCode(() -> getFacadeForResourceManagerOps(false).rebalance())
        .doesNotThrowAnyException();
  }

  @Test
  public void testFacadeTotalBytesNoRegion() {
    CacheOperationFacade facade = new AutoBalancer().getCacheOperationFacade();

    assertThat(facade.getTotalDataSize(new HashMap<>())).isEqualTo(0);
  }

  @Test
  public void testFacadeCollectMemberDetailsNoRegion() {
    final GemFireCacheImpl mockCache = mock(GemFireCacheImpl.class);
    when(mockCache.isClosed()).thenReturn(false);
    when(mockCache.getPartitionedRegions()).thenReturn(Collections.emptySet());
    GeodeCacheFacade facade = new GeodeCacheFacade(mockCache);

    assertThat(facade.getRegionMemberDetails().size()).isEqualTo(0);
  }

  @Test
  public void testFacadeCollectMemberDetails2Regions() {
    final LoadProbe mockProbe = mock(LoadProbe.class);
    final GemFireCacheImpl mockCache = mock(GemFireCacheImpl.class);
    final InternalResourceManager mockRM = mock(InternalResourceManager.class);
    final PartitionedRegion mockR1 = mock(PartitionedRegion.class, "r1");
    final PartitionedRegion mockR2 = mock(PartitionedRegion.class, "r2");
    final PRHARedundancyProvider mockRedundancyProviderR1 =
        mock(PRHARedundancyProvider.class, "prhaR1");
    final InternalPRInfo mockR1PRInfo = mock(InternalPRInfo.class, "prInforR1");
    final PRHARedundancyProvider mockRedundancyProviderR2 =
        mock(PRHARedundancyProvider.class, "prhaR2");
    final InternalPRInfo mockR2PRInfo = mock(InternalPRInfo.class, "prInforR2");
    final HashSet<PartitionedRegion> regions = new HashSet<>();
    regions.add(mockR1);
    regions.add(mockR2);

    when(mockCache.isClosed()).thenReturn(false);
    when(mockCache.getPartitionedRegions()).thenReturn(regions);
    when(mockCache.getResourceManager()).thenReturn(mockRM);
    when(mockCache.getInternalResourceManager()).thenReturn(mockRM);
    when(mockRM.getLoadProbe()).thenReturn(mockProbe);
    when(mockR1.getRedundancyProvider()).thenReturn(mockRedundancyProviderR1);
    when(mockR2.getRedundancyProvider()).thenReturn(mockRedundancyProviderR2);
    when(mockRedundancyProviderR1.buildPartitionedRegionInfo(eq(true), any(LoadProbe.class)))
        .thenReturn(mockR1PRInfo);
    when(mockRedundancyProviderR2.buildPartitionedRegionInfo(eq(true), any(LoadProbe.class)))
        .thenReturn(mockR2PRInfo);

    GeodeCacheFacade facade = new GeodeCacheFacade(mockCache);
    Map<PartitionedRegion, InternalPRInfo> map = facade.getRegionMemberDetails();
    assertThat(map).isNotNull();
    assertThat(map.size()).isEqualTo(2);
    assertThat(map.get(mockR1)).isEqualTo(mockR1PRInfo);
    assertThat(map.get(mockR2)).isEqualTo(mockR2PRInfo);
  }

  @Test
  public void testFacadeTotalBytes2Regions() {
    final PartitionedRegion mockR1 = mock(PartitionedRegion.class, "r1");
    final InternalPRInfo mockR1PRInfo = mock(InternalPRInfo.class, "prInforR1");
    final PartitionMemberInfo mockR1M1Info = mock(PartitionMemberInfo.class, "r1M1");
    final PartitionMemberInfo mockR1M2Info = mock(PartitionMemberInfo.class, "r1M2");
    final HashSet<PartitionMemberInfo> r1Members = new HashSet<>();
    r1Members.add(mockR1M1Info);
    r1Members.add(mockR1M2Info);
    when(mockR1PRInfo.getPartitionMemberInfo()).thenReturn(r1Members);
    when(mockR1M1Info.getSize()).thenReturn(123L);
    when(mockR1M2Info.getSize()).thenReturn(74L);

    final PartitionedRegion mockR2 = mock(PartitionedRegion.class, "r2");
    final InternalPRInfo mockR2PRInfo = mock(InternalPRInfo.class, "prInforR2");
    final PartitionMemberInfo mockR2M1Info = mock(PartitionMemberInfo.class, "r2M1");
    final HashSet<PartitionMemberInfo> r2Members = new HashSet<>();
    r2Members.add(mockR2M1Info);
    when(mockR2PRInfo.getPartitionMemberInfo()).thenReturn(r2Members);
    when(mockR2M1Info.getSize()).thenReturn(3475L);

    final Map<PartitionedRegion, InternalPRInfo> details = new HashMap<>();
    details.put(mockR1, mockR1PRInfo);
    details.put(mockR2, mockR2PRInfo);

    GeodeCacheFacade facade = new GeodeCacheFacade() {
      @Override
      public Map<PartitionedRegion, InternalPRInfo> getRegionMemberDetails() {
        return details;
      }
    };

    assertThat(facade.getTotalDataSize(details)).isEqualTo(123 + 74 + 3475);
    verify(mockR1M1Info, atLeastOnce()).getSize();
    verify(mockR1M2Info, atLeastOnce()).getSize();
    verify(mockR2M1Info, atLeastOnce()).getSize();
  }

  @Test
  public void testAuditorInvocation() throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(3);

    when(mockClock.currentTimeMillis()).then((invocation) -> {
      latch.countDown();
      return 990L;
    });

    Properties props = AutoBalancerJUnitTest.getBasicConfig();
    assertThat(latch.getCount()).isEqualTo(3);
    AutoBalancer autoR = new AutoBalancer(null, mockAuditor, mockClock, null);
    autoR.initialize(null, props);

    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    verify(mockAuditor, atLeast(2)).execute();
    verify(mockAuditor, times(1)).init(any(Properties.class));
  }

  @Test
  public void destroyAutoBalancer() throws InterruptedException {
    final int timer = 20; // simulate 20 milliseconds
    final CountDownLatch latch = new CountDownLatch(2);
    final CountDownLatch timerLatch = new CountDownLatch(1);
    when(mockClock.currentTimeMillis()).then((invocation) -> {
      latch.countDown();
      if (latch.getCount() == 0) {
        assertThat(timerLatch.await(1, TimeUnit.SECONDS)).isTrue();
        // scheduler is destroyed before wait is over
        // fail();
        throw new AssertionError();
      }
      return 1000L - timer;
    });

    Properties props = AutoBalancerJUnitTest.getBasicConfig();
    assertThat(latch.getCount()).isEqualTo(2);
    AutoBalancer autoR = new AutoBalancer(null, mockAuditor, mockClock, null);
    autoR.initialize(null, props);
    assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
    verify(mockAuditor, times(1)).init(any(Properties.class));

    // after destroy no more execute will be called.
    autoR.destroy();
    timerLatch.countDown();
    TimeUnit.MILLISECONDS.sleep(2 * timer);
  }
}
