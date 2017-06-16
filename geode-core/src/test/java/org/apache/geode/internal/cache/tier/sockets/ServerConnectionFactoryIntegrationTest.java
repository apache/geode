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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Test that switching on the header byte makes instances of {@link NewProtocolServerConnection}.
 */
@Category(IntegrationTest.class)
public class ServerConnectionFactoryIntegrationTest {
  /**
   *
   * @throws IOException
   */
  @Test
  public void testNewProtocolHeaderLeadsToNewProtocolServerConnection() throws IOException {
    System.setProperty("geode.feature-protobuf-protocol", "true");
    try {
      CacheFactory cacheFactory = new CacheFactory();
      Cache cache = cacheFactory.create();

      CacheServer cacheServer = cache.addCacheServer();
      final int cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
      cacheServer.setPort(cacheServerPort);
      cacheServer.start();

      Socket socket = new Socket("localhost", cacheServerPort);
      Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
      socket.getOutputStream().write(Acceptor.CLIENT_TO_SERVER_NEW_PROTOCOL);
      socket.getOutputStream().write(222);
      assertEquals(222, socket.getInputStream().read());

      cache.close();
    } finally {
      System.clearProperty("geode.feature-protobuf-protocol");
    }
  }
}
