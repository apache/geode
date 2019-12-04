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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.GemFireCacheImpl.ReplyProcessor21Factory;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.ResourceAdvisor;
import org.apache.geode.internal.cache.eviction.HeapEvictor;
import org.apache.geode.internal.cache.eviction.OffHeapEvictor;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.util.concurrent.MeteredCountDownLatch;
import org.apache.geode.management.internal.JmxManagerAdvisor;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

/**
 * Unit tests for closing {@link GemFireCacheImpl}.
 */
public class GemFireCacheImplCloseTest {

  private CacheConfig cacheConfig;
  private CompositeMeterRegistry meterRegistry;
  private InternalDistributedSystem internalDistributedSystem;
  private PoolFactory poolFactory;
  private ReplyProcessor21Factory replyProcessor21Factory;
  private TypeRegistry typeRegistry;

  private GemFireCacheImpl gemFireCacheImpl;

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Before
  public void setUp() {
    cacheConfig = mock(CacheConfig.class);
    internalDistributedSystem = mock(InternalDistributedSystem.class);
    poolFactory = mock(PoolFactory.class);
    replyProcessor21Factory = mock(ReplyProcessor21Factory.class);
    typeRegistry = mock(TypeRegistry.class);

    DistributionConfig distributionConfig = mock(DistributionConfig.class);
    DistributionManager distributionManager = mock(DistributionManager.class);
    ReplyProcessor21 replyProcessor21 = mock(ReplyProcessor21.class);

    when(distributionConfig.getSecurityProps())
        .thenReturn(new Properties());
    when(internalDistributedSystem.getConfig())
        .thenReturn(distributionConfig);
    when(internalDistributedSystem.getDistributionManager())
        .thenReturn(distributionManager);
    when(internalDistributedSystem.getCancelCriterion())
        .thenReturn(mock(CancelCriterion.class));
    when(replyProcessor21.getProcessorId())
        .thenReturn(21);
    when(replyProcessor21Factory.create(any(), any()))
        .thenReturn(replyProcessor21);

    gemFireCacheImpl = gemFireCacheImpl(false);
  }

  @After
  public void tearDown() {
    if (gemFireCacheImpl != null) {
      gemFireCacheImpl.close();
    }
    if (meterRegistry != null) {
      meterRegistry.close();
    }
  }

  @Test
  public void isClosed_returnsFalse_ifCacheExists() {
    assertThat(gemFireCacheImpl.isClosed())
        .isFalse();
  }

  @Test
  public void isClosed_returnsTrue_ifCacheIsClosed() {
    gemFireCacheImpl.close();

    assertThat(gemFireCacheImpl.isClosed())
        .isTrue();
  }

  @Test
  public void close_closesHeapEvictor() {
    HeapEvictor heapEvictor = mock(HeapEvictor.class);
    gemFireCacheImpl.setHeapEvictor(heapEvictor);

    gemFireCacheImpl.close();

    verify(heapEvictor)
        .close();
  }

  @Test
  public void close_closesOffHeapEvictor() {
    OffHeapEvictor offHeapEvictor = mock(OffHeapEvictor.class);
    gemFireCacheImpl.setOffHeapEvictor(offHeapEvictor);

    gemFireCacheImpl.close();

    verify(offHeapEvictor)
        .close();
  }

  @Test
  public void close_doesNotCloseUserMeterRegistries() {
    meterRegistry = new CompositeMeterRegistry();
    MeterRegistry userRegistry = spy(new SimpleMeterRegistry());
    meterRegistry.add(userRegistry);
    when(internalDistributedSystem.getMeterRegistry())
        .thenReturn(meterRegistry);

    gemFireCacheImpl.close();

    assertThat(userRegistry.isClosed())
        .isFalse();
  }

  /**
   * InternalDistributed.disconnect is invoked only once despite invoking GemFireCacheImpl.close
   * more than once.
   */
  @Test
  public void close_doesNothingIfAlreadyClosed() {
    gemFireCacheImpl.close();

    verify(internalDistributedSystem).disconnect();

    assertThatCode(() -> gemFireCacheImpl.close())
        .doesNotThrowAnyException();

    verify(internalDistributedSystem).disconnect();
  }

  @Test
  public void close_blocksUntilFirstCallToCloseCompletes() throws Exception {
    MeteredCountDownLatch go = new MeteredCountDownLatch(1);
    AtomicLong winner = new AtomicLong();

    Future<Long> close1 = executorServiceRule.submit(() -> {
      synchronized (GemFireCacheImpl.class) {
        long threadId = Thread.currentThread().getId();
        go.await(getTimeout().toMillis(), MILLISECONDS);
        gemFireCacheImpl.close();
        winner.compareAndSet(0, threadId);
        return threadId;
      }
    });

    await().until(() -> go.getWaitCount() == 1);

    Future<Long> close2 = executorServiceRule.submit(() -> {
      long threadId = Thread.currentThread().getId();
      go.await(getTimeout().toMillis(), MILLISECONDS);
      gemFireCacheImpl.close();
      winner.compareAndSet(0, threadId);
      return threadId;
    });

    await().until(() -> go.getWaitCount() == 2);

    go.countDown();

    long threadId1 = close1.get();
    long threadId2 = close2.get();

    assertThat(winner.get())
        .as("ThreadId1=" + threadId1 + " and threadId2=" + threadId2)
        .isEqualTo(threadId1);
  }

  @SuppressWarnings({"SameParameterValue", "LambdaParameterHidesMemberVariable",
      "OverlyCoupledMethod", "unchecked"})
  private GemFireCacheImpl gemFireCacheImpl(boolean useAsyncEventListeners) {
    return new GemFireCacheImpl(
        false,
        poolFactory,
        internalDistributedSystem,
        cacheConfig,
        useAsyncEventListeners,
        typeRegistry,
        mock(Consumer.class),
        (properties, cacheConfigArg) -> mock(SecurityService.class),
        () -> true,
        mock(Function.class),
        mock(Function.class),
        (factory, clock) -> mock(CachePerfStats.class),
        mock(GemFireCacheImpl.TXManagerImplFactory.class),
        mock(Supplier.class),
        distributionAdvisee -> mock(ResourceAdvisor.class),
        mock(Function.class),
        jmxManagerAdvisee -> mock(JmxManagerAdvisor.class),
        internalCache -> mock(InternalResourceManager.class),
        () -> 1,
        (cache, statisticsClock) -> mock(HeapEvictor.class),
        mock(Runnable.class),
        mock(Runnable.class),
        mock(Runnable.class),
        mock(Function.class),
        mock(Consumer.class),
        mock(GemFireCacheImpl.TypeRegistryFactory.class),
        mock(Consumer.class),
        mock(Consumer.class),
        o -> mock(SystemTimer.class),
        internalCache -> mock(TombstoneService.class),
        internalDistributedSystem -> mock(ExpirationScheduler.class),
        file -> mock(DiskStoreMonitor.class),
        () -> mock(RegionEntrySynchronizationListener.class),
        mock(Function.class),
        mock(Function.class),
        mock(TXEntryStateFactory.class),
        replyProcessor21Factory);
  }
}
