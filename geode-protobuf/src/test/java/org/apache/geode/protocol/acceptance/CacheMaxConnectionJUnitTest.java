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

package org.apache.geode.protocol.acceptance;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.protocol.protobuf.ProtobufSerializationService;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Test that using the magic byte to indicate intend ot use ProtoBuf messages works
 */
@Category(IntegrationTest.class)
public class CacheMaxConnectionJUnitTest {
  private final String TEST_REGION = "testRegion";


  private Cache cache;
  private int cacheServerPort;
  private Socket socket;
  private OutputStream outputStream;

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setup() throws Exception {
    Properties properties = new Properties();
    CacheFactory cacheFactory = new CacheFactory(properties);
    cacheFactory.set(ConfigurationProperties.MCAST_PORT, "0");
    cacheFactory.set(ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION, "false");
    cacheFactory.set(ConfigurationProperties.USE_CLUSTER_CONFIGURATION, "false");
    cache = cacheFactory.create();

    CacheServer cacheServer = cache.addCacheServer();
    cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer.setPort(cacheServerPort);
    cacheServer.start();

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    regionFactory.create(TEST_REGION);

    System.setProperty("geode.feature-protobuf-protocol", "true");

    socket = new Socket("localhost", cacheServerPort);
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
    outputStream = socket.getOutputStream();
    outputStream.write(110);
  }

  @After
  public void cleanup() throws IOException {
    cache.close();
    socket.close();
    SocketCreatorFactory.close();
  }

  @Test
  public void testNewProtocolRespectsMaxConnectionLimit() throws IOException, InterruptedException {
    cache.getDistributedSystem().disconnect();

    CacheFactory cacheFactory = new CacheFactory();
    cacheFactory.set(ConfigurationProperties.LOCATORS, "");
    cacheFactory.set(ConfigurationProperties.MCAST_PORT, "0");
    cache = cacheFactory.create();

    CacheServer cacheServer = cache.addCacheServer();
    final int cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer.setPort(cacheServerPort);
    cacheServer.setMaxConnections(16);
    cacheServer.setMaxThreads(16);
    cacheServer.start();

    AcceptorImpl acceptor = ((CacheServerImpl) cacheServer).getAcceptor();

    // Start 16 sockets, which is exactly the maximum that the server will support.
    Socket[] sockets = new Socket[16];
    for (int i = 0; i < 16; i++) {
      Socket socket = new Socket("localhost", cacheServerPort);
      sockets[i] = socket;
      Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
      socket.getOutputStream()
          .write(CommunicationMode.ProtobufClientServerProtocol.getModeNumber());
    }

    // try to start a new socket, expecting it to be disconnected.
    try (Socket socket = new Socket("localhost", cacheServerPort)) {
      Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
      socket.getOutputStream()
          .write(CommunicationMode.ProtobufClientServerProtocol.getModeNumber());
      assertEquals(-1, socket.getInputStream().read()); // EOF implies disconnected.
    }

    for (Socket currentSocket : sockets) {
      currentSocket.close();
    }

    // Once all connections are closed, the acceptor should have a connection count of 0.
    Awaitility.await().atMost(5, TimeUnit.SECONDS)
        .until(() -> acceptor.getClientServerCnxCount() == 0);

    // Try to start 16 new connections, again at the limit.
    for (int i = 0; i < 16; i++) {
      Socket socket = new Socket("localhost", cacheServerPort);
      sockets[i] = socket;
      Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
      socket.getOutputStream()
          .write(CommunicationMode.ProtobufClientServerProtocol.getModeNumber());
    }

    for (Socket currentSocket : sockets) {
      currentSocket.close();
    }
  }
}
