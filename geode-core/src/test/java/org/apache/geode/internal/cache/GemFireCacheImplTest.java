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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.NotSerializableException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.After;
import org.junit.Test;

import org.apache.geode.SerializationException;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.eviction.HeapEvictor;
import org.apache.geode.internal.cache.eviction.OffHeapEvictor;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.test.fake.Fakes;

/**
 * Unit tests for {@link GemFireCacheImpl}.
 */
public class GemFireCacheImplTest {

  private GemFireCacheImpl gemFireCacheImpl;

  @After
  public void tearDown() {
    InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS = false;

    if (gemFireCacheImpl != null) {
      gemFireCacheImpl.close();
    }
  }

  @Test
  public void canBeMocked() {
    GemFireCacheImpl mockGemFireCacheImpl = mock(GemFireCacheImpl.class);
    InternalResourceManager mockInternalResourceManager = mock(InternalResourceManager.class);

    when(mockGemFireCacheImpl.getInternalResourceManager()).thenReturn(mockInternalResourceManager);

    assertThat(mockGemFireCacheImpl.getInternalResourceManager())
        .isSameAs(mockInternalResourceManager);
  }

  @Test
  public void checkPurgeCCPTimer() {
    SystemTimer cacheClientProxyTimer = mock(SystemTimer.class);

    gemFireCacheImpl = createGemFireCacheWithTypeRegistry();

    gemFireCacheImpl.setCCPTimer(cacheClientProxyTimer);
    for (int i = 1; i < GemFireCacheImpl.PURGE_INTERVAL; i++) {
      gemFireCacheImpl.purgeCCPTimer();
      verify(cacheClientProxyTimer, times(0)).timerPurge();
    }
    gemFireCacheImpl.purgeCCPTimer();
    verify(cacheClientProxyTimer, times(1)).timerPurge();
    for (int i = 1; i < GemFireCacheImpl.PURGE_INTERVAL; i++) {
      gemFireCacheImpl.purgeCCPTimer();
      verify(cacheClientProxyTimer, times(1)).timerPurge();
    }
    gemFireCacheImpl.purgeCCPTimer();
    verify(cacheClientProxyTimer, times(2)).timerPurge();
  }

  @Test
  public void checkEvictorsClosed() {
    HeapEvictor heapEvictor = mock(HeapEvictor.class);
    OffHeapEvictor offHeapEvictor = mock(OffHeapEvictor.class);

    gemFireCacheImpl = createGemFireCacheWithTypeRegistry();

    gemFireCacheImpl.setHeapEvictor(heapEvictor);
    gemFireCacheImpl.setOffHeapEvictor(offHeapEvictor);
    gemFireCacheImpl.close();

    verify(heapEvictor).close();
    verify(offHeapEvictor).close();
  }

  @Test
  public void registerPdxMetaDataThrowsIfInstanceNotSerializable() {
    gemFireCacheImpl = createGemFireCacheWithTypeRegistry();

    assertThatThrownBy(() -> gemFireCacheImpl.registerPdxMetaData(new Object()))
        .isInstanceOf(SerializationException.class).hasMessage("Serialization failed")
        .hasCauseInstanceOf(NotSerializableException.class);
  }

  @Test
  public void registerPdxMetaDataThrowsIfInstanceIsNotPDX() {
    gemFireCacheImpl = createGemFireCacheWithTypeRegistry();

    assertThatThrownBy(() -> gemFireCacheImpl.registerPdxMetaData("string"))
        .isInstanceOf(SerializationException.class)
        .hasMessage("The instance is not PDX serializable");
  }

  @Test
  public void checkThatAsyncEventListenersUseAllThreadsInPool() {
    gemFireCacheImpl = createGemFireCacheWithTypeRegistry();

    ThreadPoolExecutor eventThreadPoolExecutor =
        (ThreadPoolExecutor) gemFireCacheImpl.getEventThreadPool();
    assertEquals(0, eventThreadPoolExecutor.getCompletedTaskCount());
    assertEquals(0, eventThreadPoolExecutor.getActiveCount());

    int MAX_THREADS = GemFireCacheImpl.EVENT_THREAD_LIMIT;
    final CountDownLatch threadLatch = new CountDownLatch(MAX_THREADS);
    for (int i = 1; i <= MAX_THREADS; i++) {
      eventThreadPoolExecutor.execute(() -> {
        threadLatch.countDown();
        try {
          threadLatch.await();
        } catch (InterruptedException e) {
        }
      });
    }

    await().untilAsserted(
        () -> assertThat(eventThreadPoolExecutor.getCompletedTaskCount()).isEqualTo(MAX_THREADS));
  }

  @Test
  public void getCacheClosedExceptionWithNoReasonOrCauseGivesExceptionWithoutEither() {
    gemFireCacheImpl = createGemFireCacheImpl();

    CacheClosedException cacheClosedException =
        gemFireCacheImpl.getCacheClosedException(null, null);

    assertThat(cacheClosedException.getCause()).isNull();
    assertThat(cacheClosedException.getMessage()).isNull();
  }

  @Test
  public void getCacheClosedExceptionWithNoCauseGivesExceptionWithReason() {
    gemFireCacheImpl = createGemFireCacheImpl();

    CacheClosedException cacheClosedException = gemFireCacheImpl
        .getCacheClosedException("message", null);

    assertThat(cacheClosedException.getCause()).isNull();
    assertThat(cacheClosedException.getMessage()).isEqualTo("message");
  }

  @Test
  public void getCacheClosedExceptionReturnsExceptionWithProvidedCauseAndReason() {
    gemFireCacheImpl = createGemFireCacheImpl();
    Throwable cause = new Throwable();

    CacheClosedException cacheClosedException = gemFireCacheImpl
        .getCacheClosedException("message", cause);

    assertThat(cacheClosedException.getCause()).isEqualTo(cause);
    assertThat(cacheClosedException.getMessage()).isEqualTo("message");
  }

  @Test
  public void getCacheClosedExceptionWhenCauseGivenButDisconnectExceptionExistsPrefersCause() {
    gemFireCacheImpl = createGemFireCacheImpl();
    gemFireCacheImpl.disconnectCause = new Throwable("disconnectCause");
    Throwable cause = new Throwable();

    CacheClosedException cacheClosedException = gemFireCacheImpl
        .getCacheClosedException("message", cause);

    assertThat(cacheClosedException.getCause()).isEqualTo(cause);
    assertThat(cacheClosedException.getMessage()).isEqualTo("message");
  }

  @Test
  public void getCacheClosedExceptionWhenNoCauseGivenProvidesDisconnectExceptionIfExists() {
    gemFireCacheImpl = createGemFireCacheImpl();
    Throwable disconnectCause = new Throwable("disconnectCause");
    gemFireCacheImpl.disconnectCause = disconnectCause;

    CacheClosedException cacheClosedException = gemFireCacheImpl
        .getCacheClosedException("message", null);

    assertThat(cacheClosedException.getCause()).isEqualTo(disconnectCause);
    assertThat(cacheClosedException.getMessage()).isEqualTo("message");
  }

  @Test
  public void getCacheClosedExceptionReturnsExceptionWithProvidedReason() {
    gemFireCacheImpl = createGemFireCacheImpl();

    CacheClosedException cacheClosedException = gemFireCacheImpl.getCacheClosedException("message");

    assertThat(cacheClosedException.getMessage()).isEqualTo("message");
    assertThat(cacheClosedException.getCause()).isNull();
  }

  @Test
  public void getCacheClosedExceptionReturnsExceptionWithNoMessageWhenReasonNotGiven() {
    gemFireCacheImpl = createGemFireCacheImpl();

    CacheClosedException cacheClosedException = gemFireCacheImpl.getCacheClosedException(null);

    assertThat(cacheClosedException.getMessage()).isEqualTo(null);
    assertThat(cacheClosedException.getCause()).isNull();
  }

  @Test
  public void getCacheClosedExceptionReturnsExceptionWithDisconnectCause() {
    gemFireCacheImpl = createGemFireCacheImpl();
    Throwable disconnectCause = new Throwable("disconnectCause");
    gemFireCacheImpl.disconnectCause = disconnectCause;

    CacheClosedException cacheClosedException = gemFireCacheImpl.getCacheClosedException("message");

    assertThat(cacheClosedException.getMessage()).isEqualTo("message");
    assertThat(cacheClosedException.getCause()).isEqualTo(disconnectCause);
  }

  @Test
  public void removeGatewayReceiverShouldRemoveFromReceiversList() {
    GatewayReceiver receiver = mock(GatewayReceiver.class);
    gemFireCacheImpl = createGemFireCacheImpl();
    gemFireCacheImpl.addGatewayReceiver(receiver);
    assertEquals(1, gemFireCacheImpl.getGatewayReceivers().size());

    gemFireCacheImpl.removeGatewayReceiver(receiver);

    assertEquals(0, gemFireCacheImpl.getGatewayReceivers().size());
  }


  @Test
  public void removeFromCacheServerShouldRemoveFromCacheServersList() {
    gemFireCacheImpl = createGemFireCacheImpl();
    CacheServer cacheServer = gemFireCacheImpl.addCacheServer(false);
    assertEquals(1, gemFireCacheImpl.getCacheServers().size());

    gemFireCacheImpl.removeCacheServer(cacheServer);

    assertEquals(0, gemFireCacheImpl.getCacheServers().size());
  }

  @Test
  public void testIsMisConfigured() {
    Properties clusterProps = new Properties();
    Properties serverProps = new Properties();

    // both does not have the key
    assertFalse(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key"));

    // cluster has the key, not the server
    clusterProps.setProperty("key", "value");
    assertFalse(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key"));
    clusterProps.setProperty("key", "");
    assertFalse(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key"));

    // server has the key, not the cluster
    clusterProps.clear();
    serverProps.clear();
    serverProps.setProperty("key", "value");
    assertTrue(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key"));
    serverProps.setProperty("key", "");
    assertFalse(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key"));

    // server has the key, not the cluster
    clusterProps.clear();
    serverProps.clear();
    clusterProps.setProperty("key", "");
    serverProps.setProperty("key", "value");
    assertTrue(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key"));
    serverProps.setProperty("key", "");
    assertFalse(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key"));

    // server and cluster has the same value
    clusterProps.clear();
    serverProps.clear();
    clusterProps.setProperty("key", "value");
    serverProps.setProperty("key", "value");
    assertFalse(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key"));
    clusterProps.setProperty("key", "");
    serverProps.setProperty("key", "");
    assertFalse(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key"));

    // server and cluster has the different value
    clusterProps.clear();
    serverProps.clear();
    clusterProps.setProperty("key", "value1");
    serverProps.setProperty("key", "value2");
    assertTrue(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key"));
    clusterProps.setProperty("key", "value1");
    serverProps.setProperty("key", "");
    assertFalse(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key"));
  }

  @Test
  public void clientCacheWouldNotRequestClusterConfig() {
    // we will need to set the value to true so that we can use a mock gemFireCacheImpl
    boolean oldValue = InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS;
    InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS = true;

    InternalDistributedSystem internalDistributedSystem = Fakes.distributedSystem();
    gemFireCacheImpl = mock(GemFireCacheImpl.class);
    when(internalDistributedSystem.getCache()).thenReturn(gemFireCacheImpl);

    new InternalCacheBuilder()
        .setIsClient(true)
        .create(internalDistributedSystem);

    verify(gemFireCacheImpl, times(0)).requestSharedConfiguration();
    verify(gemFireCacheImpl, times(0)).applyJarAndXmlFromClusterConfig();

    // reset it back to the old value
    InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS = oldValue;
  }

  @Test
  public void getMeterRegistryReturnsTheMeterRegistry() {
    gemFireCacheImpl = createGemFireCacheImpl();

    assertThat(gemFireCacheImpl.getMeterRegistry()).isInstanceOf(MeterRegistry.class);
  }

  private static GemFireCacheImpl createGemFireCacheImpl() {
    return (GemFireCacheImpl) new InternalCacheBuilder().create(Fakes.distributedSystem());
  }

  private static GemFireCacheImpl createGemFireCacheWithTypeRegistry() {
    InternalDistributedSystem internalDistributedSystem = Fakes.distributedSystem();
    TypeRegistry typeRegistry = mock(TypeRegistry.class);
    return (GemFireCacheImpl) new InternalCacheBuilder()
        .setUseAsyncEventListeners(true)
        .setTypeRegistry(typeRegistry)
        .create(internalDistributedSystem);
  }
}
