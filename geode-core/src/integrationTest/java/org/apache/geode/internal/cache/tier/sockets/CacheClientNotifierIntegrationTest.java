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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.shiro.subject.Subject;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DSClock;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.FilterRoutingInfo;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category(ClientSubscriptionTest.class)
public class CacheClientNotifierIntegrationTest {

  private final CountDownLatch messageDispatcherInitLatch = new CountDownLatch(1);
  private final CountDownLatch notifyClientLatch = new CountDownLatch(1);

  private final ExecutorService executorService = Executors.newFixedThreadPool(2);

  @Rule
  public Timeout timeout = Timeout.millis(getTimeout().getValueInMS());

  @After
  public void tearDown() {
    messageDispatcherInitLatch.countDown();
    notifyClientLatch.countDown();
    executorService.shutdownNow();
  }

  @Test
  public void testCacheClientNotifier_NotifyClients_QRMCausesPrematureRemovalFromHAContainer()
      throws Exception {
    InternalCache mockInternalCache = createMockInternalCache();
    assertThat(mockInternalCache.getMeterRegistry()).isNotNull();

    CacheClientNotifier cacheClientNotifier =
        CacheClientNotifier.getInstance(mockInternalCache, mock(CacheServerStats.class),
            100000, 100000, mock(ConnectionListener.class), null, false);

    String mockRegionNameProxyOne = "mockHARegionProxyOne";
    String mockRegionNameProxyTwo = "mockHARegionProxyTwo";

    CacheClientProxy cacheClientProxyOne =
        createMockCacheClientProxy(cacheClientNotifier, mockRegionNameProxyOne);

    CacheClientProxy cacheClientProxyTwo =
        createMockCacheClientProxy(cacheClientNotifier, mockRegionNameProxyTwo);

    doAnswer(invocation -> {
      messageDispatcherInitLatch.countDown();
      notifyClientLatch.await();
      return invocation.callRealMethod();
    }).when(cacheClientProxyTwo).createMessageDispatcher(anyString());

    createMockHARegion(mockInternalCache, cacheClientProxyOne, mockRegionNameProxyOne, true);
    createMockHARegion(mockInternalCache, cacheClientProxyTwo, mockRegionNameProxyTwo, false);

    List<Callable<Void>> initAndNotifyTasks = new ArrayList<>();

    // On one thread, we are initializing the message dispatchers for the two CacheClientProxy
    // objects. For the second client's initialization,
    // we block until we can simulate a QRM message while the event sits in the CacheClientProxy's
    // queuedEvents collection.
    // This blocking logic can be found in setupMockMessageDispatcher().
    initAndNotifyTasks.add(() -> {
      cacheClientProxyOne.initializeMessageDispatcher();
      cacheClientProxyTwo.initializeMessageDispatcher();
      return null;
    });

    EntryEventImpl mockEntryEventImpl = createMockEntryEvent(new ArrayList<CacheClientProxy>() {
      {
        add(cacheClientProxyOne);
        add(cacheClientProxyTwo);
      }
    });

    // On a second thread, we are notifying clients of our mocked events. For the first client, we
    // wait for the message dispatcher to be
    // initialized so the event is put/processed. For the second client, we ensure that the
    // dispatcher is still initializing so we put the event
    // in the CacheClientProxy's queuedEvents collection. When the event is handled by the first
    // CacheClientProxy, we mocked a QRM in the HARegion
    // put to simulate a QRM message being received at that time. The simulated QRM logic can be
    // found in the createMockHARegion() method.
    initAndNotifyTasks.add(() -> {
      messageDispatcherInitLatch.await();
      CacheClientNotifier.notifyClients(mockEntryEventImpl);
      notifyClientLatch.countDown();
      return null;
    });

    List<Future<Void>> futures = executorService.invokeAll(initAndNotifyTasks);

    for (Future<Void> future : futures) {
      future.get();
    }

    // Verify that we do not hang in peek() for the second proxy due to the wrapper
    await().until(() -> {
      if (cacheClientProxyTwo.getHARegionQueue().peek() != null) {
        cacheClientProxyTwo.getHARegionQueue().remove();
        return true;
      }
      return false;
    });

    assertThat(cacheClientNotifier.getHaContainer()).as("Expected the HAContainer to be empty")
        .isEmpty();
  }

  private HARegion createMockHARegion(InternalCache internalCache,
      CacheClientProxy cacheClientProxy, String haRegionName, boolean simulateQrm)
      throws IOException, ClassNotFoundException {
    HARegion mockHARegion = mock(HARegion.class);

    when(mockHARegion.getAttributesMutator()).thenReturn(mock(AttributesMutator.class));
    when(mockHARegion.getCache()).thenReturn(internalCache);
    when(mockHARegion.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(mockHARegion.getGemFireCache()).thenReturn(internalCache);
    when(internalCache.createVMRegion(eq(haRegionName), any(RegionAttributes.class),
        any(InternalRegionArguments.class))).thenReturn(mockHARegion);

    // We use a mock of the HARegion.put() method to simulate an queue removal message
    // immediately after the event was successfully put. In production when a queue removal takes
    // place at this time,
    // it will decrement the ref count on the HAEventWrapper and potentially make it eligible for
    // removal later on
    // when CacheClientNotifier.checkAndRemoveFromClientMsgsRegion() is called.

    Map<Object, Object> events = new HashMap<>();

    when(mockHARegion.put(any(long.class), any(HAEventWrapper.class)))
        .thenAnswer(invocation -> {
          long position = invocation.getArgument(0);
          HAEventWrapper haEventWrapper = invocation.getArgument(1);

          if (simulateQrm) {
            // This call is ultimately what a QRM message will do when it is processed, so we
            // simulate
            // that here.
            cacheClientProxy.getHARegionQueue().destroyFromAvailableIDs(position);
            events.remove(position);
            cacheClientProxy.getHARegionQueue()
                .decAndRemoveFromHAContainer(haEventWrapper);
          } else {
            events.put(position, haEventWrapper);
          }

          return null;
        });

    // This is so that when peek() is called, the object is returned. Later we want to verify that
    // it was successfully "delivered" to the client and subsequently removed from the HARegion.
    when(mockHARegion.get(any(long.class)))
        .thenAnswer(invocation -> events.get(invocation.getArgument(0)));

    return mockHARegion;
  }

  private InternalCache createMockInternalCache() {
    InternalCache mockInternalCache = mock(InternalCache.class);
    InternalDistributedSystem mockInternalDistributedSystem = createMockInternalDistributedSystem();

    when(mockInternalCache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(mockInternalCache.getCCPTimer()).thenReturn(mock(SystemTimer.class));
    when(mockInternalCache.getDistributedSystem()).thenReturn(mockInternalDistributedSystem);
    when(mockInternalCache.getInternalDistributedSystem())
        .thenReturn(mockInternalDistributedSystem);
    when(mockInternalCache.getMeterRegistry()).thenReturn(new SimpleMeterRegistry());

    return mockInternalCache;
  }

  private InternalDistributedSystem createMockInternalDistributedSystem() {
    DistributionManager mockDistributionManager = mock(DistributionManager.class);
    InternalDistributedSystem mockInternalDistributedSystem = mock(InternalDistributedSystem.class);

    when(mockDistributionManager.getConfig()).thenReturn(mock(DistributionConfig.class));
    when(mockInternalDistributedSystem.getDistributedMember())
        .thenReturn(mock(InternalDistributedMember.class));
    when(mockInternalDistributedSystem.createAtomicStatistics(any(StatisticsType.class),
        any(String.class))).thenReturn(mock(Statistics.class));
    when(mockInternalDistributedSystem.getDistributionManager())
        .thenReturn(mockDistributionManager);
    when(mockInternalDistributedSystem.getClock()).thenReturn(mock(DSClock.class));

    return mockInternalDistributedSystem;
  }

  private CacheClientProxy createMockCacheClientProxy(CacheClientNotifier cacheClientNotifier,
      String haRegionName) throws IOException {
    ClientProxyMembershipID mockClientProxyMembershipID = mock(ClientProxyMembershipID.class);
    Socket mockSocket = mock(Socket.class);

    when(mockClientProxyMembershipID.getDistributedMember())
        .thenReturn(mock(DistributedMember.class));
    when(mockClientProxyMembershipID.getHARegionName()).thenReturn(haRegionName);
    when(mockSocket.getInetAddress()).thenReturn(mock(InetAddress.class));

    CacheClientProxy cacheClientProxy =
        spy(new CacheClientProxy(cacheClientNotifier, mockSocket, mockClientProxyMembershipID, true,
            (byte) 0, Version.GFE_58, 0, true, mock(SecurityService.class), mock(Subject.class)));

    cacheClientNotifier.addClientInitProxy(cacheClientProxy);

    return cacheClientProxy;
  }

  private EntryEventImpl createMockEntryEvent(List<CacheClientProxy> proxies) {
    EntryEventImpl mockEntryEventImpl = mock(EntryEventImpl.class);
    EventID mockEventId = mock(EventID.class);
    FilterProfile mockFilterProfile = mock(FilterProfile.class);
    FilterRoutingInfo.FilterInfo mockFilterInfo = mock(FilterRoutingInfo.FilterInfo.class);
    LocalRegion mockLocalRegion = mock(LocalRegion.class);
    Set mockInterestedClients = mock(Set.class);

    when(mockEntryEventImpl.getEventId()).thenReturn(mockEventId);
    when(mockEntryEventImpl.getEventType()).thenReturn(EnumListenerEvent.AFTER_CREATE);
    when(mockEntryEventImpl.getLocalFilterInfo()).thenReturn(mockFilterInfo);
    when(mockEntryEventImpl.getOperation()).thenReturn(Operation.CREATE);
    when(mockEntryEventImpl.getRegion()).thenReturn(mockLocalRegion);
    when(mockEventId.getMembershipID()).thenReturn(new byte[] {1});
    when(mockFilterInfo.getInterestedClients()).thenReturn(mockInterestedClients);
    when(mockFilterInfo.getInterestedClientsInv()).thenReturn(null);

    Set<ClientProxyMembershipID> mockRealClientIDs = new HashSet<>();
    for (CacheClientProxy proxy : proxies) {
      mockRealClientIDs.add(proxy.getProxyID());
    }

    when(mockFilterProfile.getRealClientIDs(any(Collection.class))).thenReturn(mockRealClientIDs);
    when(mockLocalRegion.getFilterProfile()).thenReturn(mockFilterProfile);

    return mockEntryEventImpl;
  }
}
