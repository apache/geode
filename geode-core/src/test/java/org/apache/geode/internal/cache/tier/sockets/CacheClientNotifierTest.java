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
package org.apache.geode.internal.cache.tier.sockets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.query.internal.cq.ServerCQ;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.FilterRoutingInfo;
import org.apache.geode.internal.cache.FilterRoutingInfo.FilterInfo;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheEvent;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionQueueException;
import org.apache.geode.internal.cache.tier.sockets.ClientRegistrationEventQueueManager.ClientRegistrationEventQueue;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class CacheClientNotifierTest {

  private static final String CQ_NAME = "testCQ";
  private static final long CQ_ID = 0;

  private final AtomicReference<CountDownLatch> afterLatch =
      new AtomicReference<>(new CountDownLatch(0));
  private final AtomicReference<CountDownLatch> beforeLatch =
      new AtomicReference<>(new CountDownLatch(0));

  private CacheClientProxy cacheClientProxy;
  private ClientProxyMembershipID clientProxyMembershipId;
  private ClientRegistrationEventQueueManager clientRegistrationEventQueueManager;
  private ClientRegistrationMetadata clientRegistrationMetadata;
  private InternalCache internalCache;
  private InternalDistributedSystem internalDistributedSystem;
  private Socket socket;
  private Statistics statistics;
  private StatisticsManager statisticsManager;
  private InternalRegion region;

  private CacheClientNotifier cacheClientNotifier;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);
  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @BeforeClass
  public static void clearStatics() {
    // Perform cleanup on any singletons received from previous test runs, since the
    // CacheClientNotifier is a static and previous tests may not have cleaned up properly.
    CacheClientNotifier cacheClientNotifier = CacheClientNotifier.getInstance();
    if (cacheClientNotifier != null) {
      cacheClientNotifier.shutdown(0);
    }
  }

  @Before
  public void setUp() {
    cacheClientProxy = mock(CacheClientProxy.class);
    clientProxyMembershipId = mock(ClientProxyMembershipID.class);
    clientRegistrationEventQueueManager = mock(ClientRegistrationEventQueueManager.class);
    clientRegistrationMetadata = mock(ClientRegistrationMetadata.class);
    internalCache = mock(InternalCache.class);
    internalDistributedSystem = mock(InternalDistributedSystem.class);
    region = mock(InternalRegion.class);
    socket = mock(Socket.class);
    statistics = mock(Statistics.class);
    statisticsManager = mock(StatisticsManager.class);
  }

  @After
  public void tearDown() {
    beforeLatch.get().countDown();
    afterLatch.get().countDown();

    clearStatics();
  }

  @Test
  public void eventsInClientRegistrationQueueAreSentToClientAfterRegistrationIsComplete()
      throws Exception {
    // this test requires real impl instance of ClientRegistrationEventQueueManager
    clientRegistrationEventQueueManager = new ClientRegistrationEventQueueManager();

    when(cacheClientProxy.getProxyID())
        .thenReturn(clientProxyMembershipId);
    when(clientRegistrationMetadata.getClientProxyMembershipID())
        .thenReturn(clientProxyMembershipId);
    when(internalCache.getCancelCriterion())
        .thenReturn(mock(CancelCriterion.class));
    when(internalCache.getCCPTimer())
        .thenReturn(mock(SystemTimer.class));
    when(internalCache.getInternalDistributedSystem())
        .thenReturn(internalDistributedSystem);
    when(internalDistributedSystem.getStatisticsManager())
        .thenReturn(statisticsManager);
    when(statisticsManager.createAtomicStatistics(any(), any()))
        .thenReturn(statistics);

    cacheClientNotifier = spy(CacheClientNotifier.getInstance(internalCache,
        clientRegistrationEventQueueManager, mock(StatisticsClock.class),
        mock(CacheServerStats.class), 0, 0, mock(ConnectionListener.class), null, false));

    beforeLatch.set(new CountDownLatch(1));
    afterLatch.set(new CountDownLatch(1));

    // We stub out the CacheClientNotifier.registerClientInternal() to do some "work" until
    // a new event is received and queued, as triggered by the afterLatch
    doAnswer(invocation -> {
      cacheClientNotifier.addClientProxy(cacheClientProxy);
      beforeLatch.get().countDown();
      afterLatch.get().await();
      return null;
    })
        .when(cacheClientNotifier)
        .registerClientInternal(clientRegistrationMetadata, socket, false, 0, true);

    Collection<Callable<Void>> tasks = new ArrayList<>();

    // In one thread, we register the new client which should create the temporary client
    // registration event queue. Events will be passed to that queue while registration is
    // underway. Once registration is complete, the queue is drained and the event is processed
    // as normal.
    tasks.add(() -> {
      cacheClientNotifier.registerClient(clientRegistrationMetadata, socket, false, 0, true);
      return null;
    });

    // In a second thread, we mock the arrival of a new event. We want to ensure this event
    // goes into the temporary client registration event queue. To do that, we wait on the
    // beforeLatch until registration is underway and the temp queue is
    // created. Once it is, we process the event and notify clients, which should add the event
    // to the temp queue. Finally, we resume registration and after it is complete, we verify
    // that the event was drained, processed, and "delivered" (note message delivery is mocked
    // and results in a no-op).
    tasks.add(() -> {
      beforeLatch.get().await();

      InternalCacheEvent internalCacheEvent = internalCacheEvent(clientProxyMembershipId);
      ClientUpdateMessageImpl clientUpdateMessageImpl = mock(ClientUpdateMessageImpl.class);
      CacheClientNotifier.notifyClients(internalCacheEvent, clientUpdateMessageImpl);

      afterLatch.get().countDown();
      return null;
    });

    for (Future future : executorServiceRule.getExecutorService().invokeAll(tasks)) {
      future.get();
    }

    verify(cacheClientProxy).deliverMessage(any());
  }

  @Test
  public void initializingMessageDoesNotSerializeValuePrematurely() {
    // this test requires mock of EntryEventImpl instead of InternalCacheEvent
    EntryEventImpl entryEventImpl = mock(EntryEventImpl.class);

    when(entryEventImpl.getEventType())
        .thenReturn(EnumListenerEvent.AFTER_CREATE);
    when(entryEventImpl.getOperation())
        .thenReturn(Operation.CREATE);
    when(entryEventImpl.getRegion())
        .thenReturn(region);
    when(internalCache.getCCPTimer())
        .thenReturn(mock(SystemTimer.class));
    when(internalCache.getInternalDistributedSystem())
        .thenReturn(internalDistributedSystem);
    when(internalDistributedSystem.getStatisticsManager())
        .thenReturn(statisticsManager);
    when(statisticsManager.createAtomicStatistics(any(), any()))
        .thenReturn(statistics);

    cacheClientNotifier = CacheClientNotifier.getInstance(internalCache,
        mock(ClientRegistrationEventQueueManager.class), mock(StatisticsClock.class),
        mock(CacheServerStats.class), 0, 0, mock(ConnectionListener.class), null, false);

    cacheClientNotifier.constructClientMessage(entryEventImpl);

    verify(entryEventImpl, never()).exportNewValue(any());
  }

  @Test
  public void clientRegistrationFailsQueueStillDrained() throws Exception {
    ClientRegistrationEventQueue clientRegistrationEventQueue =
        mock(ClientRegistrationEventQueue.class);

    when(clientRegistrationEventQueueManager.create(eq(clientProxyMembershipId), any(), any()))
        .thenReturn(clientRegistrationEventQueue);
    when(clientRegistrationMetadata.getClientProxyMembershipID())
        .thenReturn(clientProxyMembershipId);
    when(internalCache.getCCPTimer())
        .thenReturn(mock(SystemTimer.class));
    when(internalCache.getInternalDistributedSystem())
        .thenReturn(internalDistributedSystem);
    when(internalDistributedSystem.getStatisticsManager())
        .thenReturn(statisticsManager);
    when(statisticsManager.createAtomicStatistics(any(), any()))
        .thenReturn(statistics);

    cacheClientNotifier = spy(CacheClientNotifier.getInstance(internalCache,
        clientRegistrationEventQueueManager, mock(StatisticsClock.class),
        mock(CacheServerStats.class), 0, 0, mock(ConnectionListener.class), null, false));

    doThrow(new RegionQueueException("thrown during client registration"))
        .when(cacheClientNotifier)
        .registerClientInternal(clientRegistrationMetadata, socket, false, 0, true);

    Throwable thrown = catchThrowable(() -> {
      cacheClientNotifier.registerClient(clientRegistrationMetadata, socket, false, 0, true);
    });
    assertThat(thrown).isInstanceOf(IOException.class);

    verify(clientRegistrationEventQueueManager)
        .create(eq(clientProxyMembershipId), any(), any());

    verify(clientRegistrationEventQueueManager)
        .drain(eq(clientRegistrationEventQueue), eq(cacheClientNotifier));
  }

  @Test
  public void testSingletonHasClientProxiesFalseNoCCN() {
    assertThat(CacheClientNotifier.singletonHasClientProxies()).isFalse();
  }

  @Test
  public void testSingletonHasClientProxiesFalseNoProxy() {
    when(internalCache.getCCPTimer())
        .thenReturn(mock(SystemTimer.class));

    cacheClientNotifier = CacheClientNotifier.getInstance(internalCache,
        mock(ClientRegistrationEventQueueManager.class), mock(StatisticsClock.class),
        mock(CacheServerStats.class), 10, 10, mock(ConnectionListener.class), null, true);

    assertThat(CacheClientNotifier.singletonHasClientProxies()).isFalse();
  }

  @Test
  public void testSingletonHasClientProxiesTrue() {
    when(cacheClientProxy.getAcceptorId())
        .thenReturn(111L);
    when(cacheClientProxy.getProxyID())
        .thenReturn(mock(ClientProxyMembershipID.class));
    when(internalCache.getCCPTimer())
        .thenReturn(mock(SystemTimer.class));

    cacheClientNotifier = CacheClientNotifier.getInstance(internalCache,
        mock(ClientRegistrationEventQueueManager.class), mock(StatisticsClock.class),
        mock(CacheServerStats.class), 10, 10, mock(ConnectionListener.class), null, true);

    cacheClientNotifier.addClientProxy(cacheClientProxy);

    // check ClientProxy Map is not empty
    assertThat(CacheClientNotifier.singletonHasClientProxies()).isTrue();
  }

  @Test
  public void testSingletonHasInitClientProxiesTrue() {
    when(cacheClientProxy.getAcceptorId())
        .thenReturn(111L);
    when(cacheClientProxy.getProxyID())
        .thenReturn(mock(ClientProxyMembershipID.class));
    when(internalCache.getCCPTimer())
        .thenReturn(mock(SystemTimer.class));

    cacheClientNotifier = CacheClientNotifier.getInstance(internalCache,
        mock(ClientRegistrationEventQueueManager.class), mock(StatisticsClock.class),
        mock(CacheServerStats.class), 10, 10, mock(ConnectionListener.class), null, true);

    cacheClientNotifier.addClientInitProxy(cacheClientProxy);

    // check InitClientProxy Map is not empty
    assertThat(CacheClientNotifier.singletonHasClientProxies()).isTrue();

    cacheClientNotifier.addClientProxy(cacheClientProxy);

    // check ClientProxy Map is not empty
    assertThat(CacheClientNotifier.singletonHasClientProxies()).isTrue();
  }

  private InternalCacheEvent internalCacheEvent(ClientProxyMembershipID clientProxyMembershipID) {
    FilterInfo filterInfo = mock(FilterInfo.class);
    FilterProfile filterProfile = mock(FilterProfile.class);
    FilterRoutingInfo filterRoutingInfo = mock(FilterRoutingInfo.class);
    InternalCacheEvent internalCacheEvent = mock(InternalCacheEvent.class);
    ServerCQ serverCQ = mock(ServerCQ.class);

    HashMap<Long, Integer> cqs = new HashMap<>();
    cqs.put(CQ_ID, 123);

    when(filterInfo.getCQs())
        .thenReturn(cqs);
    when(filterProfile.getCq(CQ_NAME))
        .thenReturn(serverCQ);
    when(filterProfile.getRealCqID(CQ_ID))
        .thenReturn(CQ_NAME);
    when(filterProfile.getFilterRoutingInfoPart2(null, internalCacheEvent))
        .thenReturn(filterRoutingInfo);
    when(filterRoutingInfo.getLocalFilterInfo())
        .thenReturn(filterInfo);
    when(internalCacheEvent.getRegion())
        .thenReturn(region);
    when(internalCacheEvent.getLocalFilterInfo())
        .thenReturn(filterInfo);
    when(internalCacheEvent.getOperation())
        .thenReturn(mock(Operation.class));
    when(region.getFilterProfile())
        .thenReturn(filterProfile);
    when(serverCQ.getClientProxyId())
        .thenReturn(clientProxyMembershipID);

    return internalCacheEvent;
  }

}
