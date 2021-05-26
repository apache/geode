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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.ha.HAContainerMap;
import org.apache.geode.internal.cache.ha.HAContainerWrapper;
import org.apache.geode.internal.net.SocketCloser;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class CacheClientProxyTest {

  @Rule
  public ServerStarterRule serverRule = new ServerStarterRule().withAutoStart();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Test
  public void closeSocketShouldBeAtomic() {
    final CacheServerStats stats = mock(CacheServerStats.class);
    doNothing().when(stats).incCurrentQueueConnections();

    final InternalCache cache = serverRule.getCache();

    final CacheClientNotifier ccn = mock(CacheClientNotifier.class);
    final SocketCloser sc = mock(SocketCloser.class);
    when(ccn.getCache()).thenReturn(cache);
    when(ccn.getAcceptorStats()).thenReturn(stats);
    when(ccn.getSocketCloser()).thenReturn(sc);

    final Socket socket = mock(Socket.class);
    final InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    when(address.getHostAddress()).thenReturn("localhost");
    doNothing().when(sc).asyncClose(any(), eq("localhost"), any(Runnable.class));

    final ClientProxyMembershipID proxyID = mock(ClientProxyMembershipID.class);
    final DistributedMember member = cache.getDistributedSystem().getDistributedMember();
    when(proxyID.getDistributedMember()).thenReturn(member);

    CacheClientProxy proxy = new CacheClientProxy(ccn, socket, proxyID, true,
        Handshake.CONFLATION_DEFAULT, KnownVersion.CURRENT, 1L, true,
        null, null, mock(StatisticsClock.class));

    CompletableFuture<Void> result1 = executorServiceRule.runAsync(() -> proxy.close());
    CompletableFuture<Void> result2 = executorServiceRule.runAsync(() -> proxy.close());
    CompletableFuture<Void> result3 = executorServiceRule.runAsync(() -> proxy.close());
    CompletableFuture.allOf(result1, result2, result3).join();
    assertThatCode(() -> result1.get(60, SECONDS)).doesNotThrowAnyException();
    assertThatCode(() -> result2.get(60, SECONDS)).doesNotThrowAnyException();
    assertThatCode(() -> result3.get(60, SECONDS)).doesNotThrowAnyException();
    verify(ccn, times(2)).getSocketCloser();
    assertNull(proxy._remoteHostAddress);
  }

  @Test
  public void closeSocket1000Times() {
    // run it for 1000 times to introduce conflicts between threads
    for (int i = 0; i < 1000; i++) {
      closeSocketShouldBeAtomic();
    }
  }

  @Test
  public void checkQueueingStats() {
    final CacheServerStats stats = mock(CacheServerStats.class);
    doNothing().when(stats).incCurrentQueueConnections();

    final InternalCache cache = serverRule.getCache();

    final CacheClientNotifier ccn = mock(CacheClientNotifier.class);
    final SocketCloser sc = mock(SocketCloser.class);
    when(ccn.getCache()).thenReturn(cache);
    when(ccn.getAcceptorStats()).thenReturn(stats);
    when(ccn.getSocketCloser()).thenReturn(sc);
    final HAContainerWrapper haContainer = new HAContainerMap(new ConcurrentHashMap<>());
    when(ccn.getHaContainer()).thenReturn(haContainer);

    final Socket socket = mock(Socket.class);
    final InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    when(address.getHostAddress()).thenReturn("localhost");
    doNothing().when(sc).asyncClose(any(), eq("localhost"), any(Runnable.class));

    final ClientProxyMembershipID proxyID = mock(ClientProxyMembershipID.class);
    final DistributedMember member = cache.getDistributedSystem().getDistributedMember();
    when(proxyID.getDistributedMember()).thenReturn(member);
    final String regionName = "region/test";
    when(proxyID.getHARegionName()).thenReturn(regionName);

    CacheClientProxy proxy = new CacheClientProxy(ccn, socket, proxyID, true,
        Handshake.CONFLATION_DEFAULT, KnownVersion.CURRENT, 1L, true,
        null, null, mock(StatisticsClock.class));

    Region dataRegion = createDataRegion();
    proxy.initializeMessageDispatcher();
    ClientUpdateMessage clientUpdateMessageImpl1 = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
            (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
            new ClientProxyMembershipID(), new EventID(cache.getDistributedSystem()));
    ClientUpdateMessage clientUpdateMessageImpl2 = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
            (LocalRegion) dataRegion, "key1", "value1".getBytes(), (byte) 0x01, null,
            new ClientProxyMembershipID(), new EventID(cache.getDistributedSystem()));
    proxy._messageDispatcher.enqueueMessage(clientUpdateMessageImpl1);
    proxy._messageDispatcher.enqueueMessage(clientUpdateMessageImpl2);

    assertThat(proxy.getStatistics().getMessageQueueSize()).isEqualTo(2);

  }

  private Region createDataRegion() {
    return serverRule.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("data");
  }
}
