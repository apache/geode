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
package org.apache.geode.internal.protocol.protobuf.v1;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.protocol.protobuf.ProtocolVersion;
import org.apache.geode.internal.protocol.protobuf.v1.serializer.ProtobufProtocolSerializer;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ProtocolVersionIntegrationTest {
  private Cache cache;

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private OutputStream outputStream;
  private InputStream inputStream;
  private ProtobufProtocolSerializer protobufProtocolSerializer;
  private Socket socket;
  private SocketChannel socketChannel;

  @Before
  public void setUp() throws Exception {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    // Create a cache with security disabled
    Properties properties = new Properties();
    CacheFactory cacheFactory = new CacheFactory(properties);
    cacheFactory.set(ConfigurationProperties.MCAST_PORT, "0");
    cacheFactory.set(ConfigurationProperties.USE_CLUSTER_CONFIGURATION, "false");
    cacheFactory.set(ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION, "false");
    cache = cacheFactory.create();

    CacheServer cacheServer = cache.addCacheServer();
    int cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer.setPort(cacheServerPort);
    cacheServer.start();

    InetSocketAddress localhost = new InetSocketAddress("localhost", cacheServerPort);
    socketChannel = SocketChannel.open(localhost);

    socket = socketChannel.socket();

    await().until(socket::isConnected);
    outputStream = socket.getOutputStream();
    inputStream = socket.getInputStream();

    protobufProtocolSerializer = new ProtobufProtocolSerializer();
  }

  @After
  public void tearDown() {
    if (cache != null) {
      cache.close();
    }
  }

  @Test
  public void testNormalHandshakeSucceeds() throws Exception {
    MessageUtil.performAndVerifyHandshake(socket);
  }

  @Test
  public void testInvalidMajorVersionBreaksConnection() throws Exception {
    ProtocolVersion.NewConnectionClientVersion.newBuilder().setMajorVersion(2000)
        .setMinorVersion(ProtocolVersion.MinorVersions.CURRENT_MINOR_VERSION_VALUE).build()
        .writeDelimitedTo(socket.getOutputStream());

    ProtocolVersion.VersionAcknowledgement handshakeResponse =
        ProtocolVersion.VersionAcknowledgement.parseDelimitedFrom(socket.getInputStream());
    assertFalse(handshakeResponse.getVersionAccepted());

    // Verify that connection is closed
    await().until(() -> {
      try {
        assertEquals(-1, socket.getInputStream().read()); // EOF implies disconnected.
        return true;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Protobuf seems to omit values that are set to their default (0). This ruins the serialization
   * trick we use because the message size changes.
   */
  @Test
  public void testMissingMajorVersionBreaksConnection() throws Exception {
    ProtocolVersion.NewConnectionClientVersion.newBuilder()
        .setMajorVersion(ProtocolVersion.MajorVersions.CURRENT_MAJOR_VERSION_VALUE)
        .setMinorVersion(0).build().writeDelimitedTo(socket.getOutputStream());

    // Verify that connection is closed
    await().until(() -> {
      try {
        assertEquals(-1, socket.getInputStream().read()); // EOF implies disconnected.
        return true;
      } catch (IOException e) {
        // Ignore IOExceptions (sometimes socket reset exception is thrown)
        return true;
      }
    });
  }
}
