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

import static org.apache.geode.cache.server.CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT;
import static org.apache.geode.cache.server.CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS;
import static org.apache.geode.cache.server.CacheServer.DEFAULT_MAX_THREADS;
import static org.apache.geode.cache.server.CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE;
import static org.apache.geode.cache.server.CacheServer.DEFAULT_SOCKET_BUFFER_SIZE;
import static org.apache.geode.cache.server.CacheServer.DEFAULT_TCP_NO_DELAY;
import static org.apache.geode.internal.cache.tier.sockets.AcceptorImpl.MINIMUM_MAX_CONNECTIONS;
import static org.apache.geode.internal.net.SocketCreatorFactory.getSocketCreatorForComponent;
import static org.apache.geode.internal.security.SecurableCommunicationChannel.SERVER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.BindException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class AcceptorImplIntegrationTest {

  private InternalCache cache;
  private Acceptor acceptor1;
  private Acceptor acceptor2;

  @Before
  public void setUp() throws Exception {
    cache = (InternalCache) new CacheFactory().create();
  }

  @After
  public void tearDown() throws Exception {
    if (acceptor1 != null) {
      acceptor1.close();
    }
    if (acceptor2 != null) {
      acceptor2.close();
    }
    cache.close();
  }

  @Test
  public void constructorThrowsBindExceptionIfPortIsInUse() throws Exception {
    acceptor1 = createAcceptor();

    Throwable thrown = catchThrowable(() -> acceptor2 = createAcceptor(acceptor1.getPort()));

    assertThat(thrown).isInstanceOf(BindException.class);
  }

  @Test
  public void acceptorBindsToLocalAddress() throws Exception {
    acceptor1 = createAcceptor();

    assertThat(acceptor1.getServerInetAddress().isAnyLocalAddress()).isTrue();
  }

  /**
   * If a CacheServer is stopped but the cache is still open we need to inform other members
   * of the cluster that the server component no longer exists. Partitioned Region bucket
   * advisors need to know about this event.
   */
  @Test
  public void acceptorCloseInformsOtherServersIfCacheIsNotClosed() throws Exception {
    acceptor1 = spy(createAcceptor());

    acceptor1.close();

    verify(acceptor1).notifyCacheMembersOfClose();
  }

  /**
   * If a CacheServer is stopped as part of cache.close() we don't need to inform other
   * members of the cluster since all regions will be destroyed.
   */
  @Test
  public void acceptorCloseDoesNotInformOtherServersIfCacheIsClosed() throws Exception {
    acceptor1 = spy(createAcceptor());
    cache.close();

    acceptor1.close();

    verify(acceptor1, never()).notifyCacheMembersOfClose();
  }

  private Acceptor createAcceptor() throws IOException {
    return createAcceptor(0);
  }

  private Acceptor createAcceptor(int port) throws IOException {
    return new AcceptorBuilder().setPort(port).setBindAddress(null).setNotifyBySubscription(false)
        .setSocketBufferSize(DEFAULT_SOCKET_BUFFER_SIZE)
        .setMaximumTimeBetweenPings(DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS).setCache(cache)
        .setMaxConnections(MINIMUM_MAX_CONNECTIONS)
        .setMaxThreads(DEFAULT_MAX_THREADS).setMaximumMessageCount(DEFAULT_MAXIMUM_MESSAGE_COUNT)
        .setMessageTimeToLive(DEFAULT_MESSAGE_TIME_TO_LIVE)
        .setConnectionListener(null)
        .setTcpNoDelay(DEFAULT_TCP_NO_DELAY)
        .setTimeLimitMillis(1000)
        .setSecurityService(cache.getSecurityService())
        .setSocketCreatorSupplier(() -> getSocketCreatorForComponent(SERVER))
        .setCacheClientNotifierProvider(CacheClientNotifier.singletonProvider())
        .setClientHealthMonitorProvider(ClientHealthMonitor.singletonProvider())
        .create(null);
  }
}
