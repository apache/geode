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

package org.apache.geode.internal.protocol.protobuf.v1.acceptance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ConnectionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.MessageUtil;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufRequestUtilities;
import org.apache.geode.internal.protocol.protobuf.v1.serializer.ProtobufProtocolSerializer;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ConnectionStateException;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.AcceptanceTest;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Test sending ProtoBuf messages to the locator, with a security manager configured on the locator
 */
@Category({AcceptanceTest.class, DistributedTest.class, ClientServerTest.class})
public class LocatorConnectionAuthenticationDUnitTest extends JUnit4CacheTestCase {
  @Rule
  public final DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();
  private int locatorPort;

  @Before
  public void setup() throws IOException {
    // Start a new locator with authorization
    locatorPort = Host.getHost(0).getVM(0).invoke(() -> {
      System.setProperty("geode.feature-protobuf-protocol", "true");
      Properties props = new Properties();
      props.setProperty(ConfigurationProperties.SECURITY_MANAGER,
          SimpleTestSecurityManager.class.getName());
      Locator locator = Locator.startLocatorAndDS(0, null, props);
      return locator.getPort();
    });

    startCacheWithCacheServer(locatorPort);
  }

  private Socket createSocket() throws IOException {
    Host host = Host.getHost(0);
    Socket socket = new Socket(host.getHostName(), locatorPort);
    MessageUtil.sendHandshake(socket);
    MessageUtil.verifyHandshakeSuccess(socket);
    return socket;
  }

  /**
   * Test that if the locator has a security manager, an authorized client is allowed to get an
   * available server
   */
  @Test
  public void authorizedClientCanGetServersIfSecurityIsEnabled() throws Throwable {
    ClientProtocol.Message authorization = ClientProtocol.Message.newBuilder()
        .setHandshakeRequest(ConnectionAPI.HandshakeRequest.newBuilder()
            .putCredentials("security-username", "cluster").putCredentials("security-password",
                "cluster"))
        .build();
    ClientProtocol.Message GetServerRequestMessage = ClientProtocol.Message.newBuilder()
        .setGetServerRequest(ProtobufRequestUtilities.createGetServerRequest()).build();

    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();

    try (Socket socket = createSocket()) {
      protobufProtocolSerializer.serialize(authorization, socket.getOutputStream());

      ClientProtocol.Message authorizationResponse =
          protobufProtocolSerializer.deserialize(socket.getInputStream());
      assertEquals(true, authorizationResponse.getHandshakeResponse().getAuthenticated());
      protobufProtocolSerializer.serialize(GetServerRequestMessage, socket.getOutputStream());

      ClientProtocol.Message GetServerResponseMessage =
          protobufProtocolSerializer.deserialize(socket.getInputStream());
      assertTrue("Got response: " + GetServerResponseMessage,
          GetServerResponseMessage.getGetServerResponse().hasServer());
    }
  }

  /**
   * Test that if the locator has a security manager, an unauthorized client is not allowed to do
   * anything.
   */
  @Test
  public void unauthorizedClientCannotGetServersIfSecurityIsEnabled() throws Throwable {
    IgnoredException.addIgnoredException(ConnectionStateException.class);
    ClientProtocol.Message getServerRequestMessage = ClientProtocol.Message.newBuilder()
        .setGetServerRequest(ProtobufRequestUtilities.createGetServerRequest()).build();

    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();

    try (Socket socket = createSocket()) {
      protobufProtocolSerializer.serialize(getServerRequestMessage, socket.getOutputStream());

      ClientProtocol.Message getServerResponseMessage =
          protobufProtocolSerializer.deserialize(socket.getInputStream());
      assertNotNull("Got response: " + getServerResponseMessage,
          getServerRequestMessage.getErrorResponse());
    }
  }


  private Integer startCacheWithCacheServer(int locatorPort) throws IOException {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    Properties props = new Properties();
    props.setProperty(ConfigurationProperties.LOCATORS, "localhost[" + locatorPort + "]");
    props.setProperty("security-username", "cluster");
    props.setProperty("security-password", "cluster");
    props.setProperty(ConfigurationProperties.USE_CLUSTER_CONFIGURATION, "true");
    InternalCache cache = getCache(props);
    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    return cacheServer.getPort();
  }
}
