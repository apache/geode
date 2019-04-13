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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Test;

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
    ConnectionListener connectionListener = mock(ConnectionListener.class);
    ClientRegistrationMetadata clientRegistrationMetadata = mock(ClientRegistrationMetadata.class);
    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);
    when(clientRegistrationMetadata.getClientProxyMembershipID()).thenReturn(
        clientProxyMembershipID);
    Socket socket = mock(Socket.class);

    CacheClientNotifier cacheClientNotifier = CacheClientNotifier.getInstance(internalCache,
        cacheServerStats, 0, 0, connectionListener, null, false);
    final CacheClientNotifier cacheClientNotifierSpy = spy(cacheClientNotifier);
    ClientUpdateMessageImpl clientUpdateMessage = mock(ClientUpdateMessageImpl.class);

    CountDownLatch waitForEventDispatchCountdownLatch = new CountDownLatch(1);
    CountDownLatch waitForRegistrationCoundownLatch = new CountDownLatch(1);
    CacheClientProxy cacheClientProxy = mock(CacheClientProxy.class);

    doAnswer((i) -> {
      when(cacheClientProxy.getProxyID()).thenReturn(clientProxyMembershipID);
      cacheClientNotifierSpy.addClientProxy(cacheClientProxy);
      waitForRegistrationCoundownLatch.countDown();
      waitForEventDispatchCountdownLatch.await();
      return null;
    }).when(cacheClientNotifierSpy).registerClientInternal(clientRegistrationMetadata, socket,
        false, 0, true);

    ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
      List<Callable<Void>> registerAndNotifyTasks = new ArrayList<>();

      registerAndNotifyTasks.add(() -> {
        cacheClientNotifierSpy.registerClient(clientRegistrationMetadata, socket, false, 0, true);
        return null;
      });

      registerAndNotifyTasks.add(() -> {
        try {
          waitForRegistrationCoundownLatch.await();
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
          final FilterProfile filterProfile = mock(FilterProfile.class);
          when(filterProfile.getRealCqID(cqId)).thenReturn(cqName);
          final ServerCQ serverCQ = mock(ServerCQ.class);
          when(serverCQ.getClientProxyId()).thenReturn(clientProxyMembershipID);
          when(filterProfile.getCq(cqName)).thenReturn(serverCQ);
          when(region.getFilterProfile()).thenReturn(filterProfile);
          FilterRoutingInfo filterRoutingInfo = mock(FilterRoutingInfo.class);
          when(filterRoutingInfo.getLocalFilterInfo()).thenReturn(filterInfo);
          when(filterProfile.getFilterRoutingInfoPart2(null, internalCacheEvent))
              .thenReturn(filterRoutingInfo);
          doReturn(clientUpdateMessage).when(cacheClientNotifierSpy)
              .constructClientMessage(internalCacheEvent);
          CacheClientNotifier.notifyClients(internalCacheEvent, clientUpdateMessage);
        } catch (Exception ex) {
          System.out.println(ExceptionUtils.getStackTrace(ex));
        } finally {
          waitForEventDispatchCountdownLatch.countDown();
        }
        return null;
      });

      final List<Future<Void>> futures = executor.invokeAll(registerAndNotifyTasks);

      for (Future future : futures) {
        future.get();
      }
    } finally {
      executor.shutdownNow();
    }

    verify(cacheClientProxy, times(1)).deliverMessage(clientUpdateMessage);
  }
}
