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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

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

import org.apache.shiro.subject.Subject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.geode.CancelCriterion;
import org.apache.geode.DataSerializer;
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
import org.apache.geode.internal.InternalDataSerializer;
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

@Category({ClientSubscriptionTest.class})
@RunWith(PowerMockRunner.class)
@PrepareForTest({CacheClientProxy.class, DataSerializer.class})
public class CacheClientNotifierIntegrationTest {
  @Test
  public void testCacheClientNotifier_NotifyClients_QRMCausesPrematureRemovalFromHAContainer()
      throws Exception {
    final CountDownLatch messageDispatcherInitLatch = new CountDownLatch(1);
    final CountDownLatch notifyClientLatch = new CountDownLatch(1);

    setupMockMessageDispatcher(messageDispatcherInitLatch, notifyClientLatch);

    InternalCache mockInternalCache = createMockInternalCache();

    CacheClientNotifier cacheClientNotifier =
        CacheClientNotifier.getInstance(mockInternalCache, mock(CacheServerStats.class),
            100000, 100000, mock(ConnectionListener.class), null, false);

    final String mockRegionNameProxyOne = "mockHARegionProxyOne";
    final String mockRegionNameProxyTwo = "mockHARegionProxyTwo";

    CacheClientProxy cacheClientProxyOne =
        createMockCacheClientProxy(cacheClientNotifier, mockRegionNameProxyOne);
    CacheClientProxy cacheClientProxyTwo =
        createMockCacheClientProxy(cacheClientNotifier, mockRegionNameProxyTwo);

    createMockHARegion(mockInternalCache, cacheClientProxyOne, mockRegionNameProxyOne, true);
    createMockHARegion(mockInternalCache, cacheClientProxyTwo, mockRegionNameProxyTwo, false);

    ExecutorService executorService = Executors.newFixedThreadPool(2);

    Collection<Callable<Void>> initAndNotifyTasks = new ArrayList<>();

    // On one thread, we are initializing the message dispatchers for the two CacheClientProxy
    // objects. For the second client's initialization,
    // we block until we can simulate a QRM message while the event sits in the CacheClientProxy's
    // queuedEvents collection.
    // This blocking logic can be found in setupMockMessageDispatcher().
    ((ArrayList<Callable<Void>>) initAndNotifyTasks).add((Callable<Void>) () -> {
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
    ((ArrayList<Callable<Void>>) initAndNotifyTasks).add((Callable<Void>) () -> {
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

    Assert.assertEquals("Expected the HAContainer to be empty", 0,
        cacheClientNotifier.getHaContainer().size());
  }

  private HARegion createMockHARegion(InternalCache internalCache,
      CacheClientProxy cacheClientProxy,
      String haRegionName,
      boolean simulateQrm)
      throws IOException, ClassNotFoundException {
    HARegion mockHARegion = mock(HARegion.class);

    doReturn(internalCache).when(mockHARegion).getCache();
    doReturn(internalCache).when(mockHARegion).getGemFireCache();
    doReturn(mock(CancelCriterion.class)).when(mockHARegion).getCancelCriterion();
    doReturn(mock(AttributesMutator.class)).when(mockHARegion).getAttributesMutator();
    doReturn(mockHARegion).when(internalCache).createVMRegion(eq(haRegionName),
        any(RegionAttributes.class), any(InternalRegionArguments.class));

    // We use a mock of the HARegion.put() method to simulate an queue removal message
    // immediately after the event was successfully put. In production when a queue removal takes
    // place at this time,
    // it will decrement the ref count on the HAEventWrapper and potentially make it eligible for
    // removal later on
    // when CacheClientNotifier.checkAndRemoveFromClientMsgsRegion() is called.

    Map<Object, Object> events = new HashMap<Object, Object>();

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        long position = invocation.getArgument(0);
        HAEventWrapper haEventWrapper = invocation.getArgument(1);

        if (simulateQrm) {
          // This call is ultimately what a QRM message will do when it is processed, so we simulate
          // that here.
          cacheClientProxy.getHARegionQueue().destroyFromAvailableIDs(position);
          events.remove(position);
          cacheClientProxy.getHARegionQueue()
              .decAndRemoveFromHAContainer((HAEventWrapper) haEventWrapper);
        } else {
          events.put(position, haEventWrapper);
        }

        return null;
      }
    }).when(mockHARegion).put(any(long.class), any(HAEventWrapper.class));

    // This is so that when peek() is called, the object is returned. Later we want to verify that
    // it was successfully "delivered" to the client and subsequently removed from the HARegion.
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return events.get(invocation.getArgument(0));
      }
    }).when(mockHARegion).get(any(long.class));

    return mockHARegion;
  }

  private InternalCache createMockInternalCache() {
    InternalCache mockInternalCache = mock(InternalCache.class);
    doReturn(mock(SystemTimer.class)).when(mockInternalCache).getCCPTimer();
    doReturn(mock(CancelCriterion.class)).when(mockInternalCache).getCancelCriterion();

    InternalDistributedSystem mockInteralDistributedSystem = createMockInternalDistributedSystem();
    doReturn(mockInteralDistributedSystem).when(mockInternalCache).getInternalDistributedSystem();
    doReturn(mockInteralDistributedSystem).when(mockInternalCache).getDistributedSystem();

    return mockInternalCache;
  }

  private void setupMockMessageDispatcher(CountDownLatch messageDispatcherInitLatch,
      CountDownLatch notifyClientLatch) throws Exception {
    PowerMockito.whenNew(CacheClientProxy.MessageDispatcher.class).withAnyArguments()
        .thenAnswer(new Answer<CacheClientProxy.MessageDispatcher>() {
          private boolean secondClient = false;

          @Override
          public CacheClientProxy.MessageDispatcher answer(
              org.mockito.invocation.InvocationOnMock invocation) throws Throwable {
            if (secondClient) {
              messageDispatcherInitLatch.countDown();
              notifyClientLatch.await();
            }

            secondClient = true;

            CacheClientProxy cacheClientProxy = invocation.getArgument(0);
            String name = invocation.getArgument(1);

            CacheClientProxy.MessageDispatcher messageDispatcher =
                new CacheClientProxy.MessageDispatcher(cacheClientProxy,
                    name);

            return messageDispatcher;
          }
        });

    PowerMockito.mockStatic(InternalDataSerializer.class);
  }

  private InternalDistributedSystem createMockInternalDistributedSystem() {
    InternalDistributedSystem mockInternalDistributedSystem = mock(InternalDistributedSystem.class);
    DistributionManager mockDistributionManager = mock(DistributionManager.class);

    doReturn(mock(InternalDistributedMember.class)).when(mockInternalDistributedSystem)
        .getDistributedMember();
    doReturn(mock(Statistics.class)).when(mockInternalDistributedSystem)
        .createAtomicStatistics(any(StatisticsType.class), any(String.class));
    doReturn(mock(DistributionConfig.class)).when(mockDistributionManager).getConfig();
    doReturn(mockDistributionManager).when(mockInternalDistributedSystem).getDistributionManager();
    doReturn(mock(DSClock.class)).when(mockInternalDistributedSystem).getClock();

    return mockInternalDistributedSystem;
  }

  private CacheClientProxy createMockCacheClientProxy(CacheClientNotifier cacheClientNotifier,
      String haRegionName)
      throws IOException {
    Socket mockSocket = mock(Socket.class);
    doReturn(mock(InetAddress.class)).when(mockSocket).getInetAddress();

    ClientProxyMembershipID mockClientProxyMembershipID = mock(ClientProxyMembershipID.class);
    doReturn(mock(DistributedMember.class)).when(mockClientProxyMembershipID)
        .getDistributedMember();
    doReturn(haRegionName).when(mockClientProxyMembershipID).getHARegionName();

    CacheClientProxy cacheClientProxy =
        new CacheClientProxy(cacheClientNotifier, mockSocket, mockClientProxyMembershipID, true,
            (byte) 0, Version.GFE_58, 0, true, mock(SecurityService.class), mock(Subject.class));

    cacheClientNotifier.addClientInitProxy(cacheClientProxy);

    return cacheClientProxy;
  }

  private EntryEventImpl createMockEntryEvent(List<CacheClientProxy> proxies) {
    FilterRoutingInfo.FilterInfo mockFilterInfo = mock(FilterRoutingInfo.FilterInfo.class);
    Set mockInterestedClients = mock(Set.class);
    doReturn(mockInterestedClients).when(mockFilterInfo).getInterestedClients();
    doReturn(null).when(mockFilterInfo).getInterestedClientsInv();

    EntryEventImpl mockEntryEventImpl = mock(EntryEventImpl.class);
    doReturn(mockFilterInfo).when(mockEntryEventImpl).getLocalFilterInfo();

    FilterProfile mockFilterProfile = mock(FilterProfile.class);
    Set mockRealClientIDs = new HashSet<ClientProxyMembershipID>();

    for (CacheClientProxy proxy : proxies) {
      mockRealClientIDs.add(proxy.getProxyID());
    }

    doReturn(mockRealClientIDs).when(mockFilterProfile).getRealClientIDs(any(Collection.class));

    LocalRegion mockLocalRegion = mock(LocalRegion.class);
    doReturn(mockFilterProfile).when(mockLocalRegion).getFilterProfile();

    doReturn(mockLocalRegion).when(mockEntryEventImpl).getRegion();
    doReturn(EnumListenerEvent.AFTER_CREATE).when(mockEntryEventImpl).getEventType();
    doReturn(Operation.CREATE).when(mockEntryEventImpl).getOperation();
    EventID mockEventId = mock(EventID.class);
    doReturn(new byte[] {1}).when(mockEventId).getMembershipID();
    doReturn(mockEventId).when(mockEntryEventImpl).getEventId();

    return mockEntryEventImpl;
  }
}
