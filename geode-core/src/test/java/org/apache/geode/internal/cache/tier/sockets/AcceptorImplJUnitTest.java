/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ServerRefusedConnectionException;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.BindException;
import java.net.Socket;
import java.util.Collections;
import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class AcceptorImplJUnitTest
{

  DistributedSystem system;
  InternalCache cache;
  
  @Before
  public void setUp() throws Exception
  {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0");
    this.system = DistributedSystem.connect(p);
    this.cache = (InternalCache) CacheFactory.create(system);
  }

  @After
  public void tearDown() throws Exception
  {
    this.cache.close();
    this.system.disconnect();
  }

  /*
   * Test method for 'org.apache.geode.internal.cache.tier.sockets.AcceptorImpl(int, int, boolean, int, Cache)'
   */
  @Ignore
  @Test
  public void testConstructor() throws CacheException, IOException
  {
    AcceptorImpl a1 = null, a2 = null, a3 = null;
    try {
      final int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
      int port1 = freeTCPPorts[0];
      int port2 = freeTCPPorts[1];


      try {
        new AcceptorImpl(
          port1,
          null,
          false,
          CacheServer.DEFAULT_SOCKET_BUFFER_SIZE,
          CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
          this.cache,
          AcceptorImpl.MINIMUM_MAX_CONNECTIONS - 1,
          CacheServer.DEFAULT_MAX_THREADS,
          CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT,
          CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE,null,null, false, Collections.EMPTY_LIST,
          CacheServer.DEFAULT_TCP_NO_DELAY);
        fail("Expected an IllegalArgumentExcption due to max conns < min pool size");
      } catch (IllegalArgumentException expected) {
      }

      try {
        new AcceptorImpl(
          port2,
          null,
          false,
          CacheServer.DEFAULT_SOCKET_BUFFER_SIZE,
          CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
          this.cache,
          0,
          CacheServer.DEFAULT_MAX_THREADS,
          CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT,
          CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE,null,null,false, Collections.EMPTY_LIST,
          CacheServer.DEFAULT_TCP_NO_DELAY);
        fail("Expected an IllegalArgumentExcption due to max conns of zero");
      } catch (IllegalArgumentException expected) {
      }

      try {
        a1 = new AcceptorImpl(
          port1,
          null,
          false,
          CacheServer.DEFAULT_SOCKET_BUFFER_SIZE,
          CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
          this.cache,
          AcceptorImpl.MINIMUM_MAX_CONNECTIONS,
          CacheServer.DEFAULT_MAX_THREADS,
          CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT,
          CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE,null,null,false, Collections.EMPTY_LIST,
          CacheServer.DEFAULT_TCP_NO_DELAY);
        a2 = new AcceptorImpl(
          port1,
          null,
          false,
          CacheServer.DEFAULT_SOCKET_BUFFER_SIZE,
          CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
          this.cache,
          AcceptorImpl.MINIMUM_MAX_CONNECTIONS,
          CacheServer.DEFAULT_MAX_THREADS,
          CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT,
          CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE,null,null,false, Collections.EMPTY_LIST,
          CacheServer.DEFAULT_TCP_NO_DELAY);
        fail("Expecetd a BindException while attaching to the same port");
      } catch (BindException expected) {
      }

      a3 = new AcceptorImpl(
        port2,
        null,
        false,
        CacheServer.DEFAULT_SOCKET_BUFFER_SIZE,
        CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        this.cache,
        AcceptorImpl.MINIMUM_MAX_CONNECTIONS,
        CacheServer.DEFAULT_MAX_THREADS,
        CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT,
        CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE,null,null, false, Collections.EMPTY_LIST,
        CacheServer.DEFAULT_TCP_NO_DELAY);
      assertEquals(port2, a3.getPort());
      InternalDistributedSystem isystem = (InternalDistributedSystem) this.cache.getDistributedSystem();
      DistributionConfig config = isystem.getConfig();
      String bindAddress = config.getBindAddress();
      if (bindAddress == null || bindAddress.length() <= 0) {
        assertTrue(a3.getServerInetAddr().isAnyLocalAddress());
      }
    } finally {
      if (a1!=null)
        a1.close();
      if (a2!=null)
        a2.close();
      if (a3!=null)
        a3.close();
    }
  }

}
