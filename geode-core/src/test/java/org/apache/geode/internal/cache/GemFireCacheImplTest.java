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

import static org.apache.geode.distributed.internal.InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS_PROPERTY;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.NotSerializableException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.CancelCriterion;
import org.apache.geode.SerializationException;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.GemFireCacheImpl.ReplyProcessor21Factory;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.ResourceAdvisor;
import org.apache.geode.internal.cache.eviction.HeapEvictor;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.JmxManagerAdvisor;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

/**
 * Unit tests for {@link GemFireCacheImpl}.
 */
public class GemFireCacheImplTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();
  private CacheConfig cacheConfig;
  private InternalDistributedSystem internalDistributedSystem;
  private PoolFactory poolFactory;
  private ReplyProcessor21Factory replyProcessor21Factory;
  private TypeRegistry typeRegistry;
  private GemFireCacheImpl gemFireCacheImpl;

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
  }

  @Test
  public void canBeMocked() {
    GemFireCacheImpl gemFireCacheImpl = mock(GemFireCacheImpl.class);
    InternalResourceManager internalResourceManager = mock(InternalResourceManager.class);

    when(gemFireCacheImpl.getInternalResourceManager())
        .thenReturn(internalResourceManager);

    assertThat(gemFireCacheImpl.getInternalResourceManager())
        .isSameAs(internalResourceManager);
  }

  @Test
  public void checkPurgeCCPTimer() {
    SystemTimer cacheClientProxyTimer = mock(SystemTimer.class);
    gemFireCacheImpl.setCCPTimer(cacheClientProxyTimer);

    for (int i = 1; i < GemFireCacheImpl.PURGE_INTERVAL; i++) {
      gemFireCacheImpl.purgeCCPTimer();
      verify(cacheClientProxyTimer,
          times(0))
              .timerPurge();
    }

    gemFireCacheImpl.purgeCCPTimer();
    verify(cacheClientProxyTimer,
        times(1))
            .timerPurge();

    for (int i = 1; i < GemFireCacheImpl.PURGE_INTERVAL; i++) {
      gemFireCacheImpl.purgeCCPTimer();
      verify(cacheClientProxyTimer,
          times(1))
              .timerPurge();
    }

    gemFireCacheImpl.purgeCCPTimer();
    verify(cacheClientProxyTimer,
        times(2))
            .timerPurge();
  }

  @Test
  public void registerPdxMetaDataThrowsIfInstanceNotSerializable() {
    Throwable thrown = catchThrowable(() -> gemFireCacheImpl.registerPdxMetaData(new Object()));

    assertThat(thrown)
        .isInstanceOf(SerializationException.class)
        .hasMessage("Serialization failed")
        .hasCauseInstanceOf(NotSerializableException.class);
  }

  @Test
  public void registerPdxMetaDataThrowsIfInstanceIsNotPDX() {
    Throwable thrown = catchThrowable(() -> {
      gemFireCacheImpl.registerPdxMetaData("string");
    });

    assertThat(thrown)
        .isInstanceOf(SerializationException.class)
        .hasMessage("The instance is not PDX serializable");
  }

  @Test
  public void checkThatAsyncEventListenersUseAllThreadsInPool() {
    gemFireCacheImpl = gemFireCacheImpl(true);
    ThreadPoolExecutor eventThreadPoolExecutor =
        (ThreadPoolExecutor) gemFireCacheImpl.getEventThreadPool();

    assertThat(eventThreadPoolExecutor.getCompletedTaskCount())
        .isZero();
    assertThat(eventThreadPoolExecutor.getActiveCount())
        .isZero();

    int eventThreadLimit = GemFireCacheImpl.EVENT_THREAD_LIMIT;
    CountDownLatch threadLatch = new CountDownLatch(eventThreadLimit);

    for (int i = 1; i <= eventThreadLimit; i++) {
      eventThreadPoolExecutor.execute(() -> {
        threadLatch.countDown();
        try {
          threadLatch.await();
        } catch (InterruptedException ignore) {
          // ignored
        }
      });
    }

    await().untilAsserted(() -> {
      assertThat(eventThreadPoolExecutor.getCompletedTaskCount())
          .isEqualTo(eventThreadLimit);
    });
  }

  @Test
  public void getCacheClosedException_withoutReasonOrCauseGivesExceptionWithoutEither() {
    CacheClosedException value = gemFireCacheImpl.getCacheClosedException(null, null);

    assertThat(value.getCause())
        .isNull();
    assertThat(value.getMessage())
        .isNull();
  }

  @Test
  public void getCacheClosedException_withoutCauseGivesExceptionWithReason() {
    CacheClosedException value = gemFireCacheImpl.getCacheClosedException("message");

    assertThat(value.getCause())
        .isNull();
    assertThat(value.getMessage())
        .isEqualTo("message");
  }

  @Test
  public void getCacheClosedException_returnsExceptionWithProvidedCauseAndReason() {
    Throwable cause = new Throwable();

    CacheClosedException value = gemFireCacheImpl.getCacheClosedException("message", cause);

    assertThat(value.getCause())
        .isEqualTo(cause);
    assertThat(value.getMessage())
        .isEqualTo("message");
  }

  @Test
  public void getCacheClosedException_prefersGivenCauseWhenDisconnectExceptionExists() {
    gemFireCacheImpl.setDisconnectCause(new Throwable("disconnectCause"));
    Throwable cause = new Throwable();

    CacheClosedException value = gemFireCacheImpl.getCacheClosedException("message", cause);

    assertThat(value.getCause())
        .isEqualTo(cause);
    assertThat(value.getMessage())
        .isEqualTo("message");
  }

  @Test
  public void getCacheClosedException_withoutCauseGiven_providesDisconnectExceptionIfExists() {
    Throwable disconnectCause = new Throwable("disconnectCause");
    gemFireCacheImpl.setDisconnectCause(disconnectCause);

    CacheClosedException value = gemFireCacheImpl.getCacheClosedException("message");

    assertThat(value.getCause())
        .isEqualTo(disconnectCause);
    assertThat(value.getMessage())
        .isEqualTo("message");
  }

  @Test
  public void getCacheClosedException_returnsExceptionWithProvidedReason() {
    CacheClosedException value = gemFireCacheImpl.getCacheClosedException("message");

    assertThat(value.getMessage())
        .isEqualTo("message");
    assertThat(value.getCause())
        .isNull();
  }

  @Test
  public void getCacheClosedException_returnsExceptionWithoutMessageWhenReasonNotGiven() {
    CacheClosedException value = gemFireCacheImpl.getCacheClosedException(null);

    assertThat(value.getMessage())
        .isEqualTo(null);
    assertThat(value.getCause())
        .isNull();
  }

  @Test
  public void getCacheClosedException_returnsExceptionWithDisconnectCause() {
    Throwable disconnectCause = new Throwable("disconnectCause");
    gemFireCacheImpl.setDisconnectCause(disconnectCause);

    CacheClosedException value = gemFireCacheImpl.getCacheClosedException("message");

    assertThat(value.getMessage())
        .isEqualTo("message");
    assertThat(value.getCause())
        .isEqualTo(disconnectCause);
  }

  @Test
  public void addGatewayReceiverDoesNotAllowMoreThanOneGatewayReceiver() {
    GatewayReceiver receiver = mock(GatewayReceiver.class);
    GatewayReceiver receiver2 = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(receiver);

    gemFireCacheImpl.addGatewayReceiver(receiver2);

    assertThat(gemFireCacheImpl.getGatewayReceivers())
        .containsOnly(receiver2);
  }

  @Test
  public void addGatewayReceiverRequiresSuppliedGatewayReceiver() {
    Throwable thrown = catchThrowable(() -> gemFireCacheImpl.addGatewayReceiver(null));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void addGatewayReceiverAddsGatewayReceiver() {
    GatewayReceiver receiver = mock(GatewayReceiver.class);

    gemFireCacheImpl.addGatewayReceiver(receiver);

    assertThat(gemFireCacheImpl.getGatewayReceivers())
        .hasSize(1);
  }

  @Test
  public void removeGatewayReceiverRemovesGatewayReceiver() {
    GatewayReceiver receiver = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(receiver);

    gemFireCacheImpl.removeGatewayReceiver(receiver);

    assertThat(gemFireCacheImpl.getGatewayReceivers())
        .isEmpty();
  }

  @Test
  public void addCacheServerAddsOneCacheServer() {
    gemFireCacheImpl.addCacheServer();

    assertThat(gemFireCacheImpl.getCacheServers())
        .hasSize(1);
  }

  @Test
  public void removeCacheServerRemovesSpecifiedCacheServer() {
    CacheServer cacheServer = gemFireCacheImpl.addCacheServer();

    gemFireCacheImpl.removeCacheServer(cacheServer);

    assertThat(gemFireCacheImpl.getCacheServers())
        .isEmpty();
  }

  @Test
  public void testIsMisConfigured() {
    Properties clusterProps = new Properties();
    Properties serverProps = new Properties();

    // both does not have the key
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isFalse();

    // cluster has the key, not the server
    clusterProps.setProperty("key", "value");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isFalse();

    clusterProps.setProperty("key", "");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isFalse();

    // server has the key, not the cluster
    clusterProps.clear();
    serverProps.clear();
    serverProps.setProperty("key", "value");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isTrue();

    serverProps.setProperty("key", "");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isFalse();

    // server has the key, not the cluster
    clusterProps.clear();
    serverProps.clear();
    clusterProps.setProperty("key", "");
    serverProps.setProperty("key", "value");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isTrue();

    serverProps.setProperty("key", "");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isFalse();

    // server and cluster has the same value
    clusterProps.clear();
    serverProps.clear();
    clusterProps.setProperty("key", "value");
    serverProps.setProperty("key", "value");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isFalse();

    clusterProps.setProperty("key", "");
    serverProps.setProperty("key", "");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isFalse();

    // server and cluster has the different value
    clusterProps.clear();
    serverProps.clear();
    clusterProps.setProperty("key", "value1");
    serverProps.setProperty("key", "value2");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isTrue();

    clusterProps.setProperty("key", "value1");
    serverProps.setProperty("key", "");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isFalse();
  }

  @Test
  public void clientCacheDoesNotRequestClusterConfig() {
    System.setProperty(ALLOW_MULTIPLE_SYSTEMS_PROPERTY, "true");
    gemFireCacheImpl = mock(GemFireCacheImpl.class);
    when(internalDistributedSystem.getCache()).thenReturn(gemFireCacheImpl);

    new InternalCacheBuilder(new ServiceLoaderModuleService(LogService.getLogger()))
        .setIsClient(true)
        .create(internalDistributedSystem);

    verify(gemFireCacheImpl,
        times(0))
            .requestSharedConfiguration();
    verify(gemFireCacheImpl,
        times(0))
            .applyJarAndXmlFromClusterConfig();
  }

  @Test
  public void getMeterRegistry_returnsTheSystemMeterRegistry() {
    MeterRegistry systemMeterRegistry = mock(MeterRegistry.class);
    when(internalDistributedSystem.getMeterRegistry()).thenReturn(systemMeterRegistry);

    assertThat(gemFireCacheImpl.getMeterRegistry())
        .isSameAs(systemMeterRegistry);
  }

  @Test
  public void addGatewayReceiverServer_requiresPreviouslyAddedGatewayReceiver() {
    Throwable thrown = catchThrowable(() -> {
      gemFireCacheImpl.addGatewayReceiverServer(mock(GatewayReceiver.class));
    });

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void addGatewayReceiverServer_requiresSuppliedGatewayReceiver() {
    GatewayReceiver gatewayReceiver = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(gatewayReceiver);

    Throwable thrown = catchThrowable(() -> gemFireCacheImpl.addGatewayReceiverServer(null));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void addGatewayReceiverServer_addsCacheServer() {
    GatewayReceiver gatewayReceiver = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(gatewayReceiver);

    InternalCacheServer receiverServer = gemFireCacheImpl.addGatewayReceiverServer(gatewayReceiver);

    assertThat(gemFireCacheImpl.getCacheServersAndGatewayReceiver())
        .containsOnly(receiverServer);
  }

  @Test
  public void getCacheServers_isEmptyByDefault() {
    List<CacheServer> value = gemFireCacheImpl.getCacheServers();

    assertThat(value)
        .isEmpty();
  }

  @Test
  public void getCacheServers_returnsAddedCacheServer() {
    CacheServer cacheServer = gemFireCacheImpl.addCacheServer();

    List<CacheServer> value = gemFireCacheImpl.getCacheServers();

    assertThat(value)
        .containsExactly(cacheServer);
  }

  @Test
  public void getCacheServers_returnsMultipleAddedCacheServers() {
    CacheServer cacheServer1 = gemFireCacheImpl.addCacheServer();
    CacheServer cacheServer2 = gemFireCacheImpl.addCacheServer();
    CacheServer cacheServer3 = gemFireCacheImpl.addCacheServer();

    List<CacheServer> value = gemFireCacheImpl.getCacheServers();

    assertThat(value)
        .containsExactlyInAnyOrder(cacheServer1, cacheServer2, cacheServer3);
  }

  @Test
  public void getCacheServers_isStillEmptyAfterAddingGatewayReceiverServer() {
    GatewayReceiver gatewayReceiver = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(gatewayReceiver);
    gemFireCacheImpl.addGatewayReceiverServer(gatewayReceiver);

    List<CacheServer> value = gemFireCacheImpl.getCacheServers();

    assertThat(value)
        .isEmpty();
  }

  @Test
  public void getCacheServers_doesNotIncludeGatewayReceiverServer() {
    CacheServer cacheServer1 = gemFireCacheImpl.addCacheServer();
    CacheServer cacheServer2 = gemFireCacheImpl.addCacheServer();
    CacheServer cacheServer3 = gemFireCacheImpl.addCacheServer();
    GatewayReceiver gatewayReceiver = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(gatewayReceiver);
    gemFireCacheImpl.addGatewayReceiverServer(gatewayReceiver);

    List<CacheServer> value = gemFireCacheImpl.getCacheServers();

    assertThat(value)
        .containsExactlyInAnyOrder(cacheServer1, cacheServer2, cacheServer3);
  }

  @Test
  public void getCacheServersAndGatewayReceiver_isEmptyByDefault() {
    List<InternalCacheServer> value = gemFireCacheImpl.getCacheServersAndGatewayReceiver();

    assertThat(value)
        .isEmpty();
  }

  @Test
  public void getCacheServersAndGatewayReceiver_includesCacheServers() {
    InternalCacheServer cacheServer1 = (InternalCacheServer) gemFireCacheImpl.addCacheServer();
    InternalCacheServer cacheServer2 = (InternalCacheServer) gemFireCacheImpl.addCacheServer();
    InternalCacheServer cacheServer3 = (InternalCacheServer) gemFireCacheImpl.addCacheServer();

    List<InternalCacheServer> value = gemFireCacheImpl.getCacheServersAndGatewayReceiver();

    assertThat(value)
        .containsExactlyInAnyOrder(cacheServer1, cacheServer2, cacheServer3);
  }

  @Test
  public void getCacheServersAndGatewayReceiver_includesGatewayReceiver() {
    GatewayReceiver gatewayReceiver = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(gatewayReceiver);
    InternalCacheServer receiverServer = gemFireCacheImpl.addGatewayReceiverServer(gatewayReceiver);

    List<InternalCacheServer> value = gemFireCacheImpl.getCacheServersAndGatewayReceiver();

    assertThat(value)
        .containsExactly(receiverServer);
  }

  @Test
  public void getCacheServersAndGatewayReceiver_includesCacheServersAndGatewayReceiver() {
    InternalCacheServer cacheServer1 = (InternalCacheServer) gemFireCacheImpl.addCacheServer();
    InternalCacheServer cacheServer2 = (InternalCacheServer) gemFireCacheImpl.addCacheServer();
    InternalCacheServer cacheServer3 = (InternalCacheServer) gemFireCacheImpl.addCacheServer();
    GatewayReceiver gatewayReceiver = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(gatewayReceiver);
    InternalCacheServer receiverServer = gemFireCacheImpl.addGatewayReceiverServer(gatewayReceiver);

    List<InternalCacheServer> value = gemFireCacheImpl.getCacheServersAndGatewayReceiver();

    assertThat(value)
        .containsExactlyInAnyOrder(cacheServer1, cacheServer2, cacheServer3, receiverServer);
  }

  @Test
  public void isServer_isFalseByDefault() {
    boolean value = gemFireCacheImpl.isServer();

    assertThat(value)
        .isFalse();
  }

  @Test
  public void isServer_isTrueIfIsServerIsSet() {
    gemFireCacheImpl.setIsServer(true);

    boolean value = gemFireCacheImpl.isServer();

    assertThat(value)
        .isTrue();
  }

  @Test
  public void isServer_isTrueIfCacheServerExists() {
    gemFireCacheImpl.addCacheServer();

    boolean value = gemFireCacheImpl.isServer();

    assertThat(value)
        .isTrue();
  }

  @Test
  public void isServer_isFalseEvenIfGatewayReceiverServerExists() {
    GatewayReceiver gatewayReceiver = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(gatewayReceiver);
    gemFireCacheImpl.addGatewayReceiverServer(gatewayReceiver);

    boolean value = gemFireCacheImpl.isServer();

    assertThat(value)
        .isFalse();
  }

  @Test
  public void getCacheServers_isCanonical() {
    assertThat(gemFireCacheImpl.getCacheServers())
        .isSameAs(gemFireCacheImpl.getCacheServers());
  }

  @Test
  public void testMultiThreadLockUnlockDiskStore() throws InterruptedException {
    int nThread = 10;
    String diskStoreName = "MyDiskStore";
    AtomicInteger nTrue = new AtomicInteger();
    AtomicInteger nFalse = new AtomicInteger();
    IntStream.range(0, nThread).forEach(tid -> {
      executorServiceRule.submit(() -> {
        try {
          boolean lockResult = gemFireCacheImpl.doLockDiskStore(diskStoreName);
          if (lockResult) {
            nTrue.incrementAndGet();
          } else {
            nFalse.incrementAndGet();
          }
        } finally {
          boolean unlockResult = gemFireCacheImpl.doUnlockDiskStore(diskStoreName);
          if (unlockResult) {
            nTrue.incrementAndGet();
          } else {
            nFalse.incrementAndGet();
          }
        }
      });
    });
    executorServiceRule.getExecutorService().shutdown();
    executorServiceRule.getExecutorService()
        .awaitTermination(GeodeAwaitility.getTimeout().toNanos(), TimeUnit.NANOSECONDS);
    // 1 thread returns true for locking, all 10 threads return true for unlocking
    assertThat(nTrue.get()).isEqualTo(11);
    // 9 threads return false for locking
    assertThat(nFalse.get()).isEqualTo(9);
  }

  @SuppressWarnings({"LambdaParameterHidesMemberVariable", "OverlyCoupledMethod", "unchecked"})
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
        mock(GemFireCacheImpl.InternalCqServiceFactory.class),
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
        replyProcessor21Factory, new ServiceLoaderModuleService(LogService.getLogger()));
  }
}
