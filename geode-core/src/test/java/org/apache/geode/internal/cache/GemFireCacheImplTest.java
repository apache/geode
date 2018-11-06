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
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
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

public class GemFireCacheImplTest {

  private InternalDistributedSystem distributedSystem;
  private GemFireCacheImpl cache;
  private CacheConfig cacheConfig;

  @Before
  public void setup() {
    distributedSystem = Fakes.distributedSystem();
    cacheConfig = new CacheConfig();
  }

  @After
  public void tearDown() {
    if (cache != null) {
      cache.close();
    }
  }

  @Test
  public void shouldBeMockable() throws Exception {
    GemFireCacheImpl mockGemFireCacheImpl = mock(GemFireCacheImpl.class);
    InternalResourceManager mockInternalResourceManager = mock(InternalResourceManager.class);

    when(mockGemFireCacheImpl.getInternalResourceManager()).thenReturn(mockInternalResourceManager);

    assertThat(mockGemFireCacheImpl.getInternalResourceManager())
        .isSameAs(mockInternalResourceManager);
  }

  @Test
  public void checkPurgeCCPTimer() {
    InternalDistributedSystem ds = Fakes.distributedSystem();
    CacheConfig cc = new CacheConfig();
    TypeRegistry typeRegistry = mock(TypeRegistry.class);
    SystemTimer ccpTimer = mock(SystemTimer.class);
    GemFireCacheImpl gfc = GemFireCacheImpl.createWithAsyncEventListeners(ds, cc, typeRegistry);
    try {
      gfc.setCCPTimer(ccpTimer);
      for (int i = 1; i < GemFireCacheImpl.PURGE_INTERVAL; i++) {
        gfc.purgeCCPTimer();
        verify(ccpTimer, times(0)).timerPurge();
      }
      gfc.purgeCCPTimer();
      verify(ccpTimer, times(1)).timerPurge();
      for (int i = 1; i < GemFireCacheImpl.PURGE_INTERVAL; i++) {
        gfc.purgeCCPTimer();
        verify(ccpTimer, times(1)).timerPurge();
      }
      gfc.purgeCCPTimer();
      verify(ccpTimer, times(2)).timerPurge();
    } finally {
      gfc.close();
    }
  }

  @Test
  public void checkEvictorsClosed() {
    InternalDistributedSystem ds = Fakes.distributedSystem();
    CacheConfig cc = new CacheConfig();
    TypeRegistry typeRegistry = mock(TypeRegistry.class);
    SystemTimer ccpTimer = mock(SystemTimer.class);
    HeapEvictor he = mock(HeapEvictor.class);
    OffHeapEvictor ohe = mock(OffHeapEvictor.class);
    GemFireCacheImpl gfc = GemFireCacheImpl.createWithAsyncEventListeners(ds, cc, typeRegistry);
    try {
      gfc.setHeapEvictor(he);
      gfc.setOffHeapEvictor(ohe);
    } finally {
      gfc.close();
    }
    verify(he, times(1)).close();
    verify(ohe, times(1)).close();
  }

  @Test
  public void registerPdxMetaDataThrowsIfInstanceNotSerializable() {
    InternalDistributedSystem ds = Fakes.distributedSystem();
    CacheConfig cc = new CacheConfig();
    TypeRegistry typeRegistry = mock(TypeRegistry.class);
    GemFireCacheImpl gfc = GemFireCacheImpl.createWithAsyncEventListeners(ds, cc, typeRegistry);
    try {
      assertThatThrownBy(() -> gfc.registerPdxMetaData(new Object()))
          .isInstanceOf(SerializationException.class).hasMessage("Serialization failed")
          .hasCauseInstanceOf(NotSerializableException.class);
    } finally {
      gfc.close();
    }
  }

  @Test
  public void registerPdxMetaDataThrowsIfInstanceIsNotPDX() {
    InternalDistributedSystem ds = Fakes.distributedSystem();
    CacheConfig cc = new CacheConfig();
    TypeRegistry typeRegistry = mock(TypeRegistry.class);
    GemFireCacheImpl gfc = GemFireCacheImpl.createWithAsyncEventListeners(ds, cc, typeRegistry);
    try {
      assertThatThrownBy(() -> gfc.registerPdxMetaData("string"))
          .isInstanceOf(SerializationException.class)
          .hasMessage("The instance is not PDX serializable");
    } finally {
      gfc.close();
    }
  }

  @Test
  public void checkThatAsyncEventListenersUseAllThreadsInPool() {
    InternalDistributedSystem ds = Fakes.distributedSystem();
    CacheConfig cc = new CacheConfig();
    TypeRegistry typeRegistry = mock(TypeRegistry.class);
    GemFireCacheImpl gfc = GemFireCacheImpl.createWithAsyncEventListeners(ds, cc, typeRegistry);
    try {
      ThreadPoolExecutor executor = (ThreadPoolExecutor) gfc.getEventThreadPool();
      assertEquals(0, executor.getCompletedTaskCount());
      assertEquals(0, executor.getActiveCount());
      int MAX_THREADS = GemFireCacheImpl.EVENT_THREAD_LIMIT;
      final CountDownLatch cdl = new CountDownLatch(MAX_THREADS);
      for (int i = 1; i <= MAX_THREADS; i++) {
        executor.execute(() -> {
          cdl.countDown();
          try {
            cdl.await();
          } catch (InterruptedException e) {
          }
        });
      }
      await().timeout(90, TimeUnit.SECONDS)
          .untilAsserted(() -> assertEquals(MAX_THREADS, executor.getCompletedTaskCount()));
    } finally {
      gfc.close();
    }
  }

  @Test
  public void getCacheClosedExceptionWithNoReasonOrCauseGivesExceptionWithoutEither() {
    cache = GemFireCacheImpl.create(distributedSystem, cacheConfig);
    CacheClosedException e = cache.getCacheClosedException(null, null);
    assertThat(e.getCause()).isNull();
    assertThat(e.getMessage()).isNull();
  }

  @Test
  public void getCacheClosedExceptionWithNoCauseGivesExceptionWithReason() {
    cache = GemFireCacheImpl.create(distributedSystem, cacheConfig);
    CacheClosedException e = cache.getCacheClosedException("message", null);
    assertThat(e.getCause()).isNull();
    assertThat(e.getMessage()).isEqualTo("message");
  }

  @Test
  public void getCacheClosedExceptionReturnsExceptionWithProvidedCauseAndReason() {
    cache = GemFireCacheImpl.create(distributedSystem, cacheConfig);
    Throwable cause = new Throwable();
    CacheClosedException e = cache.getCacheClosedException("message", cause);
    assertThat(e.getCause()).isEqualTo(cause);
    assertThat(e.getMessage()).isEqualTo("message");
  }

  @Test
  public void getCacheClosedExceptionWhenCauseGivenButDisconnectExceptionExistsPrefersCause() {
    cache = GemFireCacheImpl.create(distributedSystem, cacheConfig);
    cache.disconnectCause = new Throwable("disconnectCause");
    Throwable cause = new Throwable();
    CacheClosedException e = cache.getCacheClosedException("message", cause);
    assertThat(e.getCause()).isEqualTo(cause);
    assertThat(e.getMessage()).isEqualTo("message");
  }

  @Test
  public void getCacheClosedExceptionWhenNoCauseGivenProvidesDisconnectExceptionIfExists() {
    cache = GemFireCacheImpl.create(distributedSystem, cacheConfig);
    Throwable disconnectCause = new Throwable("disconnectCause");
    cache.disconnectCause = disconnectCause;
    CacheClosedException e = cache.getCacheClosedException("message", null);
    assertThat(e.getCause()).isEqualTo(disconnectCause);
    assertThat(e.getMessage()).isEqualTo("message");
  }

  @Test
  public void getCacheClosedExceptionReturnsExceptionWithProvidedReason() {
    cache = GemFireCacheImpl.create(distributedSystem, cacheConfig);
    CacheClosedException e = cache.getCacheClosedException("message");
    assertThat(e.getMessage()).isEqualTo("message");
    assertThat(e.getCause()).isNull();
  }

  @Test
  public void getCacheClosedExceptionReturnsExceptionWithNoMessageWhenReasonNotGiven() {
    cache = GemFireCacheImpl.create(distributedSystem, cacheConfig);
    CacheClosedException e = cache.getCacheClosedException(null);
    assertThat(e.getMessage()).isEqualTo(null);
    assertThat(e.getCause()).isNull();
  }

  @Test
  public void getCacheClosedExceptionReturnsExceptionWithDisconnectCause() {
    cache = GemFireCacheImpl.create(distributedSystem, cacheConfig);
    Throwable disconnectCause = new Throwable("disconnectCause");
    cache.disconnectCause = disconnectCause;
    CacheClosedException e = cache.getCacheClosedException("message");
    assertThat(e.getMessage()).isEqualTo("message");
    assertThat(e.getCause()).isEqualTo(disconnectCause);
  }

  @Test
  public void removeGatewayReceiverShouldRemoveFromReceiversList() {
    GatewayReceiver receiver = mock(GatewayReceiver.class);
    cache = GemFireCacheImpl.create(distributedSystem, cacheConfig);
    cache.addGatewayReceiver(receiver);
    assertEquals(1, cache.getGatewayReceivers().size());
    cache.removeGatewayReceiver(receiver);
    assertEquals(0, cache.getGatewayReceivers().size());
  }


  @Test
  public void removeFromCacheServerShouldRemoveFromCacheServersList() {
    cache = GemFireCacheImpl.create(distributedSystem, cacheConfig);
    CacheServer cacheServer = cache.addCacheServer(false);
    assertEquals(1, cache.getCacheServers().size());
    cache.removeCacheServer(cacheServer);
    assertEquals(0, cache.getCacheServers().size());
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
    // we will need to set the value to true so that we can use a mock cache
    boolean oldValue = InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS;
    InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS = true;

    cache = mock(GemFireCacheImpl.class);
    when(distributedSystem.getCache()).thenReturn(cache);
    GemFireCacheImpl.createClient(distributedSystem, null, cacheConfig);

    verify(cache, times(0)).requestSharedConfiguration();
    verify(cache, times(0)).applyJarAndXmlFromClusterConfig();

    // reset it back to the old value
    InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS = oldValue;
  }
}
