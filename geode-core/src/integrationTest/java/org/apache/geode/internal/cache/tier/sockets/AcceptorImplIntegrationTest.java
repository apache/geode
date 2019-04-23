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
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class AcceptorImplIntegrationTest {

  private InternalCache cache;
  private AcceptorImpl acceptor1;
  private AcceptorImpl acceptor2;
  private ServerConnectionFactory serverConnectionFactory;

  @Before
  public void setUp() throws Exception {

    cache = (InternalCache) new CacheFactory().create();
    serverConnectionFactory = new ServerConnectionFactory();
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
    acceptor1 = createAcceptorImpl();

    Throwable thrown = catchThrowable(() -> acceptor2 = createAcceptorImpl(acceptor1.getPort()));

    assertThat(thrown).isInstanceOf(BindException.class);
  }

  @Test
  public void acceptorBindsToLocalAddress() throws Exception {
    acceptor1 = createAcceptorImpl();

    assertThat(acceptor1.getServerInetAddr().isAnyLocalAddress()).isTrue();
  }

  /**
   * If a CacheServer is stopped but the cache is still open we need to inform other members
   * of the cluster that the server component no longer exists. Partitioned Region bucket
   * advisors need to know about this event.
   */
  @Test
  public void acceptorCloseInformsOtherServersIfCacheIsNotClosed() throws Exception {
    acceptor1 = spy(createAcceptorImpl());

    acceptor1.close();

    verify(acceptor1).notifyCacheMembersOfClose();
  }

  /**
   * If a CacheServer is stopped as part of cache.close() we don't need to inform other
   * members of the cluster since all regions will be destroyed.
   */
  @Test
  public void acceptorCloseDoesNotInformOtherServersIfCacheIsClosed() throws Exception {
    acceptor1 = spy(createAcceptorImpl());
    cache.close();

    acceptor1.close();

    verify(acceptor1, never()).notifyCacheMembersOfClose();
  }

  private AcceptorImpl createAcceptorImpl() throws IOException {
    return createAcceptorImpl(0);
  }

  private AcceptorImpl createAcceptorImpl(int port) throws IOException {
    return new AcceptorImpl(port, null, false, CacheServer.DEFAULT_SOCKET_BUFFER_SIZE,
        CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS, cache, AcceptorImpl.MINIMUM_MAX_CONNECTIONS,
        CacheServer.DEFAULT_MAX_THREADS, CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT,
        CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE, null, null, CacheServer.DEFAULT_TCP_NO_DELAY,
        serverConnectionFactory, 1000, cache.getSecurityService(),
        () -> getSocketCreatorForComponent(SERVER), CacheClientNotifier.singletonProvider(),
        ClientHealthMonitor.singletonProvider());
  }
}
