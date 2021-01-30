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
package org.apache.geode.management.internal;

import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import javax.management.Notification;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ErrorCollector;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.HasCachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.management.ManagementException;
import org.apache.geode.test.junit.categories.JMXTest;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

@Category(JMXTest.class)
public class FederatingManagerTest {

  private InternalCache cache;
  private InternalDistributedMember member;
  private DistributionManager distributionManager;
  private ExecutorService executorService;
  private MemberMessenger messenger;
  private Region<String, Object> monitoringRegion;
  private Region<NotificationKey, Notification> notificationRegion;
  private MBeanProxyFactory proxyFactory;
  private HasCachePerfStats regionManagementStats;
  private ManagementResourceRepo repo;
  private SystemManagementService service;
  private InternalRegionFactory regionFactory1;
  private InternalRegionFactory regionFactory2;
  private StatisticsFactory statisticsFactory;
  private StatisticsClock statisticsClock;

  private ArgumentCaptor<HasCachePerfStats> managementRegionStatsCaptor;

  private InternalCacheForClientAccess cacheForClientAccess;

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();
  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();
  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() {
    cache = mock(InternalCache.class);
    cacheForClientAccess = mock(InternalCacheForClientAccess.class);
    distributionManager = mock(DistributionManager.class);
    executorService = mock(ExecutorService.class);
    member = mock(InternalDistributedMember.class);
    messenger = mock(MemberMessenger.class);
    monitoringRegion = uncheckedCast(mock(Region.class));
    notificationRegion = uncheckedCast(mock(Region.class));
    proxyFactory = mock(MBeanProxyFactory.class);
    regionFactory1 = mock(InternalRegionFactory.class);
    regionFactory2 = mock(InternalRegionFactory.class);
    regionManagementStats = mock(HasCachePerfStats.class);
    repo = mock(ManagementResourceRepo.class);
    service = mock(SystemManagementService.class);
    statisticsFactory = mock(StatisticsFactory.class);
    statisticsClock = mock(StatisticsClock.class);

    managementRegionStatsCaptor = forClass(HasCachePerfStats.class);

    when(cache.getCacheForProcessingClientRequests())
        .thenReturn(cacheForClientAccess);
  }

  @Test
  public void addMemberArtifactsCreatesMonitoringRegion() {
    when(cacheForClientAccess.createInternalRegionFactory())
        .thenReturn(uncheckedCast(regionFactory1))
        .thenReturn(uncheckedCast(regionFactory2));
    when(member.getInetAddress())
        .thenReturn(mock(InetAddress.class));
    when(member.getVmViewId())
        .thenReturn(1);
    when(member.getMembershipPort())
        .thenReturn(20);
    when(regionFactory1.create(any()))
        .thenReturn(mock(Region.class));
    when(regionFactory2.create(any()))
        .thenReturn(mock(Region.class));
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);
    federatingManager.startManager();

    federatingManager.addMemberArtifacts(member);

    verify(regionFactory1)
        .create(eq("_monitoringRegion_null<v1>20"));
  }

  @Test
  public void addMemberArtifactsCreatesMonitoringRegionWithHasOwnStats() {
    when(cacheForClientAccess.createInternalRegionFactory())
        .thenReturn(uncheckedCast(regionFactory1))
        .thenReturn(uncheckedCast(regionFactory2));
    when(member.getInetAddress())
        .thenReturn(mock(InetAddress.class));
    when(member.getVmViewId())
        .thenReturn(2);
    when(member.getMembershipPort())
        .thenReturn(40);
    when(regionFactory1.create(any()))
        .thenReturn(mock(Region.class));
    when(regionFactory2.create(any()))
        .thenReturn(mock(Region.class));
    when(regionManagementStats.hasOwnStats())
        .thenReturn(true);
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);
    federatingManager.startManager();

    federatingManager.addMemberArtifacts(member);

    verify(regionFactory1)
        .setCachePerfStatsHolder(managementRegionStatsCaptor.capture());
    assertThat(managementRegionStatsCaptor.getValue().hasOwnStats())
        .isTrue();
  }

  @Test
  public void addMemberArtifactsCreatesNotificationRegion() {
    when(cacheForClientAccess.createInternalRegionFactory())
        .thenReturn(uncheckedCast(regionFactory1))
        .thenReturn(uncheckedCast(regionFactory2));
    when(member.getInetAddress())
        .thenReturn(mock(InetAddress.class));
    when(member.getVmViewId())
        .thenReturn(3);
    when(member.getMembershipPort())
        .thenReturn(60);
    when(regionFactory1.create(any()))
        .thenReturn(mock(Region.class));
    when(regionFactory2.create(any()))
        .thenReturn(mock(Region.class));
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);
    federatingManager.startManager();

    federatingManager.addMemberArtifacts(member);

    verify(regionFactory2)
        .create(eq("_notificationRegion_null<v3>60"));
  }

  @Test
  public void addMemberArtifactsCreatesNotificationRegionWithHasOwnStats() {
    when(cacheForClientAccess.createInternalRegionFactory())
        .thenReturn(uncheckedCast(regionFactory1))
        .thenReturn(uncheckedCast(regionFactory2));
    when(member.getInetAddress())
        .thenReturn(mock(InetAddress.class));
    when(member.getVmViewId())
        .thenReturn(4);
    when(member.getMembershipPort())
        .thenReturn(80);
    when(regionFactory1.create(any()))
        .thenReturn(mock(Region.class));
    when(regionFactory2.create(any()))
        .thenReturn(mock(Region.class));
    when(regionManagementStats.hasOwnStats())
        .thenReturn(true);
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);
    federatingManager.startManager();

    federatingManager.addMemberArtifacts(member);

    verify(regionFactory2)
        .setCachePerfStatsHolder(managementRegionStatsCaptor.capture());
    assertThat(managementRegionStatsCaptor.getValue().hasOwnStats())
        .isTrue();
  }

  @Test
  public void removeMemberArtifactsLocallyDestroysMonitoringRegion() {
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(monitoringRegion);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(uncheckedCast(mock(Region.class)));
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(monitoringRegion)
        .localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsLocallyDestroysNotificationRegion() {
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(uncheckedCast(mock(Region.class)));
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(notificationRegion);
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(notificationRegion)
        .localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsDoesNotThrowIfMonitoringRegionIsAlreadyDestroyed() {
    doThrow(new RegionDestroyedException("test", "monitoringRegion"))
        .when(monitoringRegion)
        .localDestroyRegion();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(monitoringRegion);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(uncheckedCast(mock(Region.class)));
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(monitoringRegion)
        .localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsDoesNotThrowIfNotificationRegionIsAlreadyDestroyed() {
    doThrow(new RegionDestroyedException("test", "notificationRegion"))
        .when(notificationRegion)
        .localDestroyRegion();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(uncheckedCast(mock(Region.class)));
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(notificationRegion);
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(notificationRegion)
        .localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsDoesNotThrowIfMBeanProxyFactoryThrowsRegionDestroyedException() {
    doThrow(new RegionDestroyedException("test", "monitoringRegion"))
        .when(proxyFactory)
        .removeAllProxies(member, monitoringRegion);
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(monitoringRegion);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(uncheckedCast(mock(Region.class)));
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(proxyFactory)
        .removeAllProxies(member, monitoringRegion);
  }

  @Test
  public void removeMemberArtifactsProxyFactoryDoesNotThrowIfCacheClosed() {
    doThrow(new CacheClosedException("test"))
        .when(proxyFactory)
        .removeAllProxies(member, monitoringRegion);
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(monitoringRegion);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(uncheckedCast(mock(Region.class)));
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(proxyFactory)
        .removeAllProxies(member, monitoringRegion);
  }

  @Test
  public void removeMemberArtifactsNotificationRegionDoesNotThrowIfCacheClosed() {
    doThrow(new CacheClosedException("test"))
        .when(notificationRegion)
        .localDestroyRegion();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(uncheckedCast(mock(Region.class)));
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(notificationRegion);
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(notificationRegion)
        .localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsMonitoringRegionDoesNotThrowIfCacheClosed() {
    doThrow(new CacheClosedException("test"))
        .when(monitoringRegion)
        .localDestroyRegion();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(monitoringRegion);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(uncheckedCast(mock(Region.class)));
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(monitoringRegion)
        .localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsProxyFactoryDoesNotThrowIfSystemDisconnected() {
    doThrow(new DistributedSystemDisconnectedException("test"))
        .when(proxyFactory)
        .removeAllProxies(member, monitoringRegion);
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(monitoringRegion);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(uncheckedCast(mock(Region.class)));
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(proxyFactory)
        .removeAllProxies(member, monitoringRegion);
  }

  @Test
  public void removeMemberArtifactsNotificationRegionDoesNotThrowIfSystemDisconnected() {
    doThrow(new DistributedSystemDisconnectedException("test"))
        .when(notificationRegion)
        .localDestroyRegion();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(uncheckedCast(mock(Region.class)));
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(notificationRegion);
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(notificationRegion)
        .localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsMonitoringRegionDoesNotThrowIfSystemDisconnected() {
    doThrow(new DistributedSystemDisconnectedException("test"))
        .when(monitoringRegion)
        .localDestroyRegion();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(monitoringRegion);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(uncheckedCast(mock(Region.class)));
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(monitoringRegion)
        .localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsDoesNotThrowIfNotificationRegionIsNull() {
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(uncheckedCast(mock(Region.class)));
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(null);
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    Throwable thrown = catchThrowable(() -> federatingManager.removeMemberArtifacts(member, false));

    assertThat(thrown)
        .isNull();
  }

  @Test
  public void removeMemberArtifactsDoesNotThrowIfMonitoringRegionIsNull() {
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(null);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(uncheckedCast(mock(Region.class)));
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    Throwable thrown = catchThrowable(() -> federatingManager.removeMemberArtifacts(member, false));

    assertThat(thrown)
        .isNull();
  }

  @Test
  public void startManagerGetsNewExecutorServiceFromSupplier() {
    Supplier<ExecutorService> executorServiceSupplier = uncheckedCast(mock(Supplier.class));
    when(executorServiceSupplier.get())
        .thenReturn(mock(ExecutorService.class));
    FederatingManager federatingManager =
        new FederatingManager(repo, member, distributionManager, service, cache, proxyFactory,
            messenger, statisticsFactory, statisticsClock, executorServiceSupplier);

    federatingManager.startManager();

    verify(executorServiceSupplier)
        .get();
  }

  @Test
  public void removeMemberArtifactsDoesNotRemoveAllProxiesIfMonitoringRegionIsNull() {
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(null);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(uncheckedCast(mock(Region.class)));
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verifyNoMoreInteractions(proxyFactory);
  }

  @Test
  public void removeMemberWaitsForStartManager() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    CyclicBarrier barrier = new CyclicBarrier(2);

    when(executorService.invokeAll(any())).thenAnswer(invocation -> {
      awaitCyclicBarrier(barrier);
      awaitCountDownLatch(latch);
      return Collections.<Future<InternalDistributedMember>>emptyList();
    });

    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    executorServiceRule.submit(() -> {
      federatingManager.startManager();
    });

    executorServiceRule.submit(() -> {
      awaitCyclicBarrier(barrier);
      federatingManager.removeMember(member, true);
    });

    await().untilAsserted(() -> {
      assertThat(federatingManager.pendingTasks())
          .hasSize(1);
    });

    latch.countDown();

    await().untilAsserted(() -> {
      assertThat(federatingManager.pendingTasks())
          .isEmpty();
    });
  }

  @Test
  public void pendingTasksIsEmptyByDefault() {
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    assertThat(federatingManager.pendingTasks())
        .isEmpty();
  }

  @Test
  public void restartDoesNotThrowIfOtherMembersExist() {
    when(distributionManager.getOtherDistributionManagerIds())
        .thenReturn(singleton(mock(InternalDistributedMember.class)));
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    federatingManager.startManager();
    federatingManager.stopManager();

    assertThatCode(() -> federatingManager.startManager())
        .doesNotThrowAnyException();
  }

  @Test
  public void startManagerThrowsManagementExceptionWithNestedCauseOfFailure() {
    RuntimeException exception = new RuntimeException("startManager failed");
    doThrow(exception)
        .when(messenger)
        .broadcastManagerInfo();
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    Throwable thrown = catchThrowable(() -> federatingManager.startManager());

    assertThat(thrown)
        .isInstanceOf(ManagementException.class)
        .hasCause(exception);
  }

  @Test
  public void pendingTasksIsClearIfStartManagerFails() {
    RuntimeException exception = new RuntimeException("startManager failed");
    doThrow(exception)
        .when(messenger)
        .broadcastManagerInfo();
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    Throwable thrown = catchThrowable(() -> federatingManager.startManager());

    assertThat(thrown)
        .isNotNull();
    assertThat(federatingManager.pendingTasks())
        .isEmpty();
  }

  @Test
  public void startingIsFalseIfStartManagerFails() {
    RuntimeException exception = new RuntimeException("startManager failed");
    doThrow(exception)
        .when(messenger)
        .broadcastManagerInfo();
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    Throwable thrown = catchThrowable(() -> federatingManager.startManager());

    assertThat(thrown)
        .isNotNull();
    assertThat(federatingManager.isStarting())
        .isFalse();
  }

  @Test
  public void runningIsFalseIfStartManagerFails() {
    RuntimeException exception = new RuntimeException("startManager failed");
    doThrow(exception)
        .when(messenger)
        .broadcastManagerInfo();
    FederatingManager federatingManager = new FederatingManager(repo, member, distributionManager,
        service, cache, proxyFactory, messenger, regionManagementStats, executorService);

    Throwable thrown = catchThrowable(() -> federatingManager.startManager());

    assertThat(thrown)
        .isNotNull();
    assertThat(federatingManager.isRunning())
        .isFalse();
  }

  private void awaitCyclicBarrier(CyclicBarrier barrier) {
    try {
      barrier.await(getTimeout().toMillis(), MILLISECONDS);
    } catch (Exception e) {
      errorCollector.addError(e);
      throw new RuntimeException(e);
    }
  }

  private void awaitCountDownLatch(CountDownLatch latch) {
    try {
      latch.await(getTimeout().toMillis(), MILLISECONDS);
    } catch (Exception e) {
      errorCollector.addError(e);
      throw new RuntimeException(e);
    }
  }
}
