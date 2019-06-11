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

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.query.internal.cq.ServerCQ;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.FilterRoutingInfo;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheEvent;
import org.apache.geode.test.fake.Fakes;

public class CacheClientNotifierTest {
  @Test
  public void eventsInClientRegistrationQueueAreSentToClientAfterRegistrationIsComplete()
      throws IOException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
      InvocationTargetException, InterruptedException, ExecutionException {
    InternalCache internalCache = Fakes.cache();
    CacheServerStats cacheServerStats = mock(CacheServerStats.class);
    Socket socket = mock(Socket.class);
    ConnectionListener connectionListener = mock(ConnectionListener.class);
    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);
    CacheClientProxy cacheClientProxy = mock(CacheClientProxy.class);
    ClientUpdateMessageImpl clientUpdateMessage = mock(ClientUpdateMessageImpl.class);
    ClientRegistrationMetadata clientRegistrationMetadata = mock(ClientRegistrationMetadata.class);
    when(clientRegistrationMetadata.getClientProxyMembershipID()).thenReturn(
        clientProxyMembershipID);

    CacheClientNotifier cacheClientNotifier = CacheClientNotifier.getInstance(internalCache,
        cacheServerStats, 0, 0, connectionListener, null, false);
    final CacheClientNotifier cacheClientNotifierSpy = spy(cacheClientNotifier);

    CountDownLatch waitForEventDispatchCountdownLatch = new CountDownLatch(1);
    CountDownLatch waitForRegistrationCountdownLatch = new CountDownLatch(1);
    ExecutorService registerAndNotifyExecutor = Executors.newFixedThreadPool(2);

    try {
      // We stub out the CacheClientNotifier.registerClientInternal() to do some "work" until
      // a new event is received and queued, as triggered by the waitForEventDispatchCountdownLatch
      doAnswer((i) -> {
        when(cacheClientProxy.getProxyID()).thenReturn(clientProxyMembershipID);
        cacheClientNotifierSpy.addClientProxy(cacheClientProxy);
        waitForRegistrationCountdownLatch.countDown();
        waitForEventDispatchCountdownLatch.await();
        return null;
      }).when(cacheClientNotifierSpy).registerClientInternal(clientRegistrationMetadata, socket,
          false, 0, true);
      List<Callable<Void>> registerAndNotifyTasks = new ArrayList<>();

      // In one thread, we register the new client which should create the temporary client
      // registration event queue. Events will be passed to that queue while registration is
      // underway. Once registration is complete, the queue is drained and the event is processed
      // as normal.
      registerAndNotifyTasks.add(() -> {
        cacheClientNotifierSpy.registerClient(clientRegistrationMetadata, socket, false, 0, true);
        return null;
      });

      // In a second thread, we mock the arrival of a new event. We want to ensure this event
      // goes into the temporary client registration event queue. To do that, we wait on the
      // waitForRegistrationCountdownLatch until registration is underway and the temp queue is
      // created. Once it is, we process the event and notify clients, which should add the event
      // to the temp queue. Finally, we resume registration and after it is complete, we verify
      // that the event was drained, processed, and "delivered" (note message delivery is mocked
      // and results in a no-op).
      registerAndNotifyTasks.add(() -> {
        try {
          waitForRegistrationCountdownLatch.await();

          InternalCacheEvent internalCacheEvent =
              createMockInternalCacheEvent(clientProxyMembershipID, clientUpdateMessage,
                  cacheClientNotifierSpy);

          CacheClientNotifier.notifyClients(internalCacheEvent, clientUpdateMessage);
        } finally {
          // Ensure we always countdown so if the test fails it won't hang due to
          // awaiting on this countdown latch.
          waitForEventDispatchCountdownLatch.countDown();
        }
        return null;
      });

      final List<Future<Void>> futures =
          registerAndNotifyExecutor.invokeAll(registerAndNotifyTasks);

      for (final Future future : futures) {
        future.get();
      }
    } finally {
      // To prevent not cleaning up test resources in case an unexpected exception occurs
      waitForEventDispatchCountdownLatch.countDown();
      waitForRegistrationCountdownLatch.countDown();
      registerAndNotifyExecutor.shutdownNow();
      cacheClientNotifier.shutdown(0);
    }

    verify(cacheClientProxy, times(1)).deliverMessage(isA(HAEventWrapper.class));
  }

  private InternalCacheEvent createMockInternalCacheEvent(
      final ClientProxyMembershipID clientProxyMembershipID,
      final ClientUpdateMessageImpl clientUpdateMessage,
      final CacheClientNotifier cacheClientNotifierSpy) {
    InternalCacheEvent internalCacheEvent = mock(InternalCacheEvent.class);

    DistributedRegion region = mock(DistributedRegion.class);
    when(internalCacheEvent.getRegion()).thenReturn(region);

    FilterRoutingInfo.FilterInfo filterInfo = mock(FilterRoutingInfo.FilterInfo.class);
    final Long cqId = 0L;
    final String cqName = "testCQ";

    HashMap cqs = new HashMap<Long, Integer>() {
      {
        put(cqId, 123);
      }
    };
    when(filterInfo.getCQs()).thenReturn(cqs);
    when(internalCacheEvent.getLocalFilterInfo()).thenReturn(
        filterInfo);
    when(internalCacheEvent.getOperation()).thenReturn(mock(Operation.class));

    FilterProfile filterProfile = mock(FilterProfile.class);
    when(filterProfile.getRealCqID(cqId)).thenReturn(cqName);

    ServerCQ serverCQ = mock(ServerCQ.class);
    when(serverCQ.getClientProxyId()).thenReturn(clientProxyMembershipID);
    when(filterProfile.getCq(cqName)).thenReturn(serverCQ);
    when(region.getFilterProfile()).thenReturn(filterProfile);

    FilterRoutingInfo filterRoutingInfo = mock(FilterRoutingInfo.class);
    when(filterRoutingInfo.getLocalFilterInfo()).thenReturn(filterInfo);
    when(filterProfile.getFilterRoutingInfoPart2(null, internalCacheEvent))
        .thenReturn(filterRoutingInfo);
    doReturn(clientUpdateMessage).when(cacheClientNotifierSpy)
        .constructClientMessage(internalCacheEvent);

    return internalCacheEvent;
  }
}
