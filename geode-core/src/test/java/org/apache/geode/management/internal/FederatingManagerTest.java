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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ErrorCollector;
import org.mockito.ArgumentCaptor;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.alerting.internal.spi.AlertLevel;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.HasCachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementException;
import org.apache.geode.test.junit.categories.JMXTest;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

@Category(JMXTest.class)
public class FederatingManagerTest {

  private InternalCache cache;
  private ExecutorService executorService;
  private MemberMessenger messenger;
  private MBeanProxyFactory proxyFactory;
  private ManagementResourceRepo repo;
  private SystemManagementService service;
  private StatisticsFactory statisticsFactory;
  private StatisticsClock statisticsClock;
  private InternalDistributedSystem system;
  private InternalRegionFactory regionFactory1;
  private InternalRegionFactory regionFactory2;

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();
  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Before
  public void setUp() {
    cache = mock(InternalCache.class);
    executorService = mock(ExecutorService.class);
    messenger = mock(MemberMessenger.class);
    proxyFactory = mock(MBeanProxyFactory.class);
    repo = mock(ManagementResourceRepo.class);
    service = mock(SystemManagementService.class);
    statisticsClock = mock(StatisticsClock.class);
    statisticsFactory = mock(StatisticsFactory.class);
    system = mock(InternalDistributedSystem.class);
    regionFactory1 = mock(InternalRegionFactory.class);
    regionFactory2 = mock(InternalRegionFactory.class);

    InternalCacheForClientAccess cacheForClientAccess = mock(InternalCacheForClientAccess.class);
    DistributedSystemMXBean distributedSystemMXBean = mock(DistributedSystemMXBean.class);
    MBeanJMXAdapter jmxAdapter = mock(MBeanJMXAdapter.class);

    when(cache.getCacheForProcessingClientRequests())
        .thenReturn(cacheForClientAccess);
    when(cacheForClientAccess.createInternalRegionFactory())
        .thenReturn(regionFactory1)
        .thenReturn(regionFactory2);
    when(regionFactory1.create(any()))
        .thenReturn(mock(Region.class));
    when(regionFactory2.create(any()))
        .thenReturn(mock(Region.class));
    when(distributedSystemMXBean.getAlertLevel())
        .thenReturn(AlertLevel.WARNING.name());
    when(jmxAdapter.getDistributedSystemMXBean())
        .thenReturn(distributedSystemMXBean);
    when(system.getDistributionManager())
        .thenReturn(mock(DistributionManager.class));
  }

  @Test
  public void addMemberArtifactsCreatesMonitoringRegion() {
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);
    federatingManager.startManager();

    federatingManager.addMemberArtifacts(member(1, 20));

    verify(regionFactory1).create(eq("_monitoringRegion_null<v1>20"));
  }

  @Test
  public void addMemberArtifactsCreatesMonitoringRegionWithHasOwnStats() {
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);
    federatingManager.startManager();

    federatingManager.addMemberArtifacts(member(2, 40));

    ArgumentCaptor<HasCachePerfStats> captor = forClass(HasCachePerfStats.class);
    verify(regionFactory1).setCachePerfStatsHolder(captor.capture());
    assertThat(captor.getValue().hasOwnStats()).isTrue();
  }

  @Test
  public void addMemberArtifactsCreatesNotificationRegion() {
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);
    federatingManager.startManager();

    federatingManager.addMemberArtifacts(member(3, 60));

    verify(regionFactory2).create(eq("_notificationRegion_null<v3>60"));
  }

  @Test
  public void addMemberArtifactsCreatesNotificationRegionWithHasOwnStats() {
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);
    federatingManager.startManager();

    federatingManager.addMemberArtifacts(member(4, 80));

    ArgumentCaptor<HasCachePerfStats> captor = forClass(HasCachePerfStats.class);
    verify(regionFactory2).setCachePerfStatsHolder(captor.capture());
    assertThat(captor.getValue().hasOwnStats()).isTrue();
  }

  @Test
  public void removeMemberArtifactsLocallyDestroysMonitoringRegion() {
    InternalDistributedMember member = member();
    Region monitoringRegion = mock(Region.class);
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(monitoringRegion);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(monitoringRegion).localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsLocallyDestroysNotificationRegion() {
    InternalDistributedMember member = member();
    Region notificationRegion = mock(Region.class);
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(notificationRegion);
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(notificationRegion).localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsDoesNotThrowIfMonitoringRegionIsAlreadyDestroyed() {
    InternalDistributedMember member = member();
    Region monitoringRegion = mock(Region.class);
    doThrow(new RegionDestroyedException("test", "monitoringRegion"))
        .when(monitoringRegion).localDestroyRegion();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(monitoringRegion);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(monitoringRegion).localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsDoesNotThrowIfNotificationRegionIsAlreadyDestroyed() {
    InternalDistributedMember member = member();
    Region notificationRegion = mock(Region.class);
    doThrow(new RegionDestroyedException("test", "notificationRegion"))
        .when(notificationRegion).localDestroyRegion();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(notificationRegion);
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(notificationRegion).localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsDoesNotThrowIfMBeanProxyFactoryThrowsRegionDestroyedException() {
    InternalDistributedMember member = member();
    Region monitoringRegion = mock(Region.class);
    MBeanProxyFactory proxyFactory = mock(MBeanProxyFactory.class);
    doThrow(new RegionDestroyedException("test", "monitoringRegion"))
        .when(proxyFactory).removeAllProxies(member, monitoringRegion);
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(monitoringRegion);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(proxyFactory).removeAllProxies(member, monitoringRegion);
  }

  @Test
  public void removeMemberArtifactsProxyFactoryDoesNotThrowIfCacheClosed() {
    InternalDistributedMember member = member();
    Region monitoringRegion = mock(Region.class);
    MBeanProxyFactory proxyFactory = mock(MBeanProxyFactory.class);
    doThrow(new CacheClosedException("test"))
        .when(proxyFactory).removeAllProxies(member, monitoringRegion);
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(monitoringRegion);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(proxyFactory).removeAllProxies(member, monitoringRegion);
  }

  @Test
  public void removeMemberArtifactsNotificationRegionDoesNotThrowIfCacheClosed() {
    InternalDistributedMember member = member();
    Region notificationRegion = mock(Region.class);
    doThrow(new CacheClosedException("test"))
        .when(notificationRegion).localDestroyRegion();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(notificationRegion);
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(notificationRegion).localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsMonitoringRegionDoesNotThrowIfCacheClosed() {
    InternalDistributedMember member = member();
    Region monitoringRegion = mock(Region.class);
    doThrow(new CacheClosedException("test"))
        .when(monitoringRegion).localDestroyRegion();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(monitoringRegion);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(monitoringRegion).localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsProxyFactoryDoesNotThrowIfSystemDisconnected() {
    InternalDistributedMember member = member();
    Region monitoringRegion = mock(Region.class);
    MBeanProxyFactory proxyFactory = mock(MBeanProxyFactory.class);
    doThrow(new DistributedSystemDisconnectedException("test"))
        .when(proxyFactory).removeAllProxies(member, monitoringRegion);
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(monitoringRegion);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(proxyFactory).removeAllProxies(member, monitoringRegion);
  }

  @Test
  public void removeMemberArtifactsNotificationRegionDoesNotThrowIfSystemDisconnected() {
    InternalDistributedMember member = member();
    Region notificationRegion = mock(Region.class);
    doThrow(new DistributedSystemDisconnectedException("test"))
        .when(notificationRegion).localDestroyRegion();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(notificationRegion);
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(notificationRegion).localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsMonitoringRegionDoesNotThrowIfSystemDisconnected() {
    InternalDistributedMember member = member();
    Region monitoringRegion = mock(Region.class);
    doThrow(new DistributedSystemDisconnectedException("test"))
        .when(monitoringRegion).localDestroyRegion();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(monitoringRegion);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verify(monitoringRegion).localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsDoesNotThrowIfNotificationRegionIsNull() {
    InternalDistributedMember member = member();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(null);
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);

    Throwable thrown = catchThrowable(() -> federatingManager.removeMemberArtifacts(member, false));

    assertThat(thrown).isNull();
  }

  @Test
  public void removeMemberArtifactsDoesNotThrowIfMonitoringRegionIsNull() {
    InternalDistributedMember member = member();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(null);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);

    Throwable thrown = catchThrowable(() -> federatingManager.removeMemberArtifacts(member, false));

    assertThat(thrown).isNull();
  }

  @Test
  public void startManagerGetsNewExecutorServiceFromSupplier() {
    Supplier<ExecutorService> executorServiceSupplier = mock(Supplier.class);
    when(executorServiceSupplier.get())
        .thenReturn(mock(ExecutorService.class));
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorServiceSupplier);

    federatingManager.startManager();

    verify(executorServiceSupplier).get();
  }

  @Test
  public void removeMemberArtifactsDoesNotRemoveAllProxiesIfMonitoringRegionIsNull() {
    InternalDistributedMember member = member();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(null);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory,
            statisticsClock, proxyFactory, messenger, executorService);

    federatingManager.removeMemberArtifacts(member, false);

    verifyNoMoreInteractions(proxyFactory);
  }

  @Test
  public void removeMemberWaitsForStartManager() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    CyclicBarrier barrier = new CyclicBarrier(2);
    ExecutorService executorService = mock(ExecutorService.class);
    List<Future<InternalDistributedMember>> futureTaskList = Collections.emptyList();

    when(executorService.invokeAll(any())).thenAnswer(invocation -> {
      awaitCyclicBarrier(barrier);
      awaitCountDownLatch(latch);
      return futureTaskList;
    });

    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);

    executorServiceRule.submit(federatingManager::startManager);

    executorServiceRule.submit(() -> {
      awaitCyclicBarrier(barrier);
      federatingManager.removeMember(member(), true);
    });

    await().untilAsserted(() -> {
      assertThat(federatingManager.pendingTasks()).hasSize(1);
    });

    latch.countDown();

    await().untilAsserted(() -> {
      assertThat(federatingManager.pendingTasks()).isEmpty();
    });
  }

  @Test
  public void pendingTasksIsEmptyByDefault() {
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);

    assertThat(federatingManager.pendingTasks()).isEmpty();
  }

  @Test
  public void restartDoesNotThrowIfOtherMembersExist() {
    DistributionManager distributionManager = mock(DistributionManager.class);
    when(distributionManager.getOtherDistributionManagerIds())
        .thenReturn(singleton(mock(InternalDistributedMember.class)));
    when(system.getDistributionManager())
        .thenReturn(distributionManager);

    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, Executors::newSingleThreadExecutor);

    federatingManager.startManager();
    federatingManager.stopManager();

    assertThatCode(federatingManager::startManager)
        .doesNotThrowAnyException();
  }

  @Test
  public void startManagerThrowsManagementExceptionWithNestedCauseOfFailure() {
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);
    RuntimeException exception = new RuntimeException("startManager failed");
    doThrow(exception)
        .when(messenger).broadcastManagerInfo();

    Throwable thrown = catchThrowable(federatingManager::startManager);

    assertThat(thrown)
        .isInstanceOf(ManagementException.class)
        .hasCause(exception);
  }

  @Test
  public void pendingTasksIsClearIfStartManagerFails() {
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);
    RuntimeException exception = new RuntimeException("startManager failed");
    doThrow(exception)
        .when(messenger).broadcastManagerInfo();

    Throwable thrown = catchThrowable(federatingManager::startManager);
    assertThat(thrown).isNotNull();

    assertThat(federatingManager.pendingTasks()).isEmpty();
  }

  @Test
  public void startingIsFalseIfStartManagerFails() {
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);
    RuntimeException exception = new RuntimeException("startManager failed");
    doThrow(exception)
        .when(messenger).broadcastManagerInfo();

    Throwable thrown = catchThrowable(federatingManager::startManager);
    assertThat(thrown).isNotNull();

    assertThat(federatingManager.isStarting()).isFalse();
  }

  @Test
  public void runningIsFalseIfStartManagerFails() {
    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory, statisticsClock,
            proxyFactory, messenger, executorService);
    RuntimeException exception = new RuntimeException("startManager failed");
    doThrow(exception)
        .when(messenger).broadcastManagerInfo();

    Throwable thrown = catchThrowable(federatingManager::startManager);
    assertThat(thrown).isNotNull();

    assertThat(federatingManager.isRunning()).isFalse();
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

  private InternalDistributedMember member() {
    return member(1, 1);
  }

  private InternalDistributedMember member(int viewId, int port) {
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(member.getInetAddress())
        .thenReturn(mock(InetAddress.class));
    when(member.getVmViewId())
        .thenReturn(viewId);
    when(member.getMembershipPort())
        .thenReturn(port);
    return member;
  }
}
