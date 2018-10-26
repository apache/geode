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

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.BindException;
import java.util.Collections;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class AcceptorImplJUnitTest {

  DistributedSystem system;
  InternalCache cache;
  AcceptorImpl acceptor1 = null, acceptor2 = null;
  ServerConnectionFactory serverConnectionFactory = new ServerConnectionFactory();

  @Before
  public void setUp() throws Exception {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0");
    this.system = DistributedSystem.connect(p);
    this.cache = (InternalCache) CacheFactory.create(system);
  }

  @After
  public void tearDown() throws Exception {
    if (acceptor1 != null) {
      acceptor1.close();
    }
    if (acceptor2 != null) {
      acceptor2.close();
    }
    this.cache.close();
    this.system.disconnect();
  }

  @Test(expected = BindException.class)
  public void constructorThrowsBindException() throws CacheException, IOException {
    acceptor1 = new AcceptorImpl(0, null, false, CacheServer.DEFAULT_SOCKET_BUFFER_SIZE,
        CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS, this.cache,
        AcceptorImpl.MINIMUM_MAX_CONNECTIONS, CacheServer.DEFAULT_MAX_THREADS,
        CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT, CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE,
        null, null, false, Collections.EMPTY_LIST, CacheServer.DEFAULT_TCP_NO_DELAY,
        serverConnectionFactory, 1000);
    acceptor2 =
        new AcceptorImpl(acceptor1.getPort(), null, false, CacheServer.DEFAULT_SOCKET_BUFFER_SIZE,
            CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS, this.cache,
            AcceptorImpl.MINIMUM_MAX_CONNECTIONS, CacheServer.DEFAULT_MAX_THREADS,
            CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT, CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE,
            null, null, false, Collections.EMPTY_LIST, CacheServer.DEFAULT_TCP_NO_DELAY,
            serverConnectionFactory, 1000);
  }

  @Test
  public void acceptorBindsToLocalAddress() throws CacheException, IOException {
    acceptor1 = new AcceptorImpl(0, null, false, CacheServer.DEFAULT_SOCKET_BUFFER_SIZE,
        CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS, this.cache,
        AcceptorImpl.MINIMUM_MAX_CONNECTIONS, CacheServer.DEFAULT_MAX_THREADS,
        CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT, CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE, null,
        null, false, Collections.EMPTY_LIST, CacheServer.DEFAULT_TCP_NO_DELAY,
        serverConnectionFactory, 1000);
    InternalDistributedSystem isystem =
        (InternalDistributedSystem) this.cache.getDistributedSystem();
    DistributionConfig config = isystem.getConfig();
    String bindAddress = config.getBindAddress();
    if (bindAddress == null || bindAddress.length() <= 0) {
      assertTrue(acceptor1.getServerInetAddr().isAnyLocalAddress());
    }
  }

  /**
   * If a CacheServer is stopped but the cache is still open we need to inform other members
   * of the cluster that the server component no longer exists. Partitioned Region bucket
   * advisors need to know about this event.
   */
  @Test
  public void acceptorCloseInformsOtherServersIfCacheIsNotClosed() throws Exception {
    acceptor1 = new AcceptorImpl(0, null, false, CacheServer.DEFAULT_SOCKET_BUFFER_SIZE,
        CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS, this.cache,
        AcceptorImpl.MINIMUM_MAX_CONNECTIONS, CacheServer.DEFAULT_MAX_THREADS,
        CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT, CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE, null,
        null, false, Collections.EMPTY_LIST, CacheServer.DEFAULT_TCP_NO_DELAY,
        serverConnectionFactory, 1000);
    InternalDistributedSystem isystem =
        (InternalDistributedSystem) this.cache.getDistributedSystem();
    AcceptorImpl spy = Mockito.spy(acceptor1);
    spy.close();
    verify(spy, atLeastOnce()).notifyCacheMembersOfClose();
  }

  /**
   * If a CacheServer is stopped as part of cache.close() we don't need to inform other
   * members of the cluster since all regions will be destroyed.
   */
  @Test
  public void acceptorCloseDoesNotInformOtherServersIfCacheIsClosed() throws Exception {
    acceptor1 = new AcceptorImpl(0, null, false, CacheServer.DEFAULT_SOCKET_BUFFER_SIZE,
        CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS, this.cache,
        AcceptorImpl.MINIMUM_MAX_CONNECTIONS, CacheServer.DEFAULT_MAX_THREADS,
        CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT, CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE, null,
        null, false, Collections.EMPTY_LIST, CacheServer.DEFAULT_TCP_NO_DELAY,
        serverConnectionFactory, 1000);
    InternalDistributedSystem isystem =
        (InternalDistributedSystem) this.cache.getDistributedSystem();
    AcceptorImpl spy = Mockito.spy(acceptor1);
    cache.close();
    spy.close();
    verify(spy, never()).notifyCacheMembersOfClose();
  }

}
