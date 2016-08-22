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
package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ServerRefusedConnectionException;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.internal.cache.tier.Acceptor;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
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

import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;
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
   * Test method for 'com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl(int, int, boolean, int, Cache)'
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
