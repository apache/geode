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

import static org.apache.geode.internal.Assert.assertTrue;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.Statistics;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.protocol.protobuf.statistics.ProtobufClientStatistics;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.LocatorAPI;
import org.apache.geode.internal.protocol.protobuf.v1.MessageUtil;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufRequestUtilities;
import org.apache.geode.internal.protocol.protobuf.v1.serializer.ProtobufProtocolSerializer;
import org.apache.geode.internal.protocol.protobuf.v1.serializer.exception.InvalidProtocolMessageException;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.ClientServerTest;

/*
 * Test sending ProtoBuf messages to the locator
 */
@Category(ClientServerTest.class)
public class LocatorConnectionDUnitTest extends JUnit4CacheTestCase {

  @Rule
  public final DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setup() throws IOException {
    Host.getLocator().invoke(() -> System.setProperty("geode.feature-protobuf-protocol", "true"));

    startCacheWithCacheServer();
  }

  private Socket createSocket() throws IOException {
    Host host = Host.getHost(0);
    int locatorPort = DistributedTestUtils.getDUnitLocatorPort();
    Socket socket = new Socket(host.getHostName(), locatorPort);
    MessageUtil.sendHandshake(socket);
    MessageUtil.verifyHandshakeSuccess(socket);
    return socket;
  }

  // Test getAvailableServers twice, validating stats before any messages, after 1, and after 2.
  @Test
  public void testGetAvailableServersWithStats() throws Throwable {
    ClientProtocol.Message getAvailableServersRequestMessage = ClientProtocol.Message.newBuilder()
        .setGetServerRequest(ProtobufRequestUtilities.createGetServerRequest()).build();

    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();

    testSocketWithStats(getAvailableServersRequestMessage, protobufProtocolSerializer);

    testSocketWithStats(getAvailableServersRequestMessage, protobufProtocolSerializer);
  }

  private void testSocketWithStats(ClientProtocol.Message getAvailableServersRequestMessage,
      ProtobufProtocolSerializer protobufProtocolSerializer)
      throws IOException, InvalidProtocolMessageException {
    try (Socket socket = createSocket()) {
      long messagesReceived = getMessagesReceived();
      long messagesSent = getMessagesSent();
      long bytesReceived = getBytesReceived();
      long bytesSent = getBytesSent();
      int clientConnectionStarts = getClientConnectionStarts();
      int clientConnectionTerminations = getClientConnectionTerminations();

      protobufProtocolSerializer.serialize(getAvailableServersRequestMessage,
          socket.getOutputStream());

      ClientProtocol.Message getAvailableServersResponseMessage =
          protobufProtocolSerializer.deserialize(socket.getInputStream());
      validateGetAvailableServersResponse(getAvailableServersResponseMessage);

      validateStats(messagesReceived + 1, messagesSent + 1,
          bytesReceived + getAvailableServersRequestMessage.getSerializedSize(),
          bytesSent + getAvailableServersResponseMessage.getSerializedSize(),
          clientConnectionStarts, clientConnectionTerminations + 1);
    }
  }

  @Test
  public void testInvalidOperationReturnsFailure()
      throws IOException, InvalidProtocolMessageException {
    IgnoredException ignoredInvalidExecutionContext =
        IgnoredException.addIgnoredException("Invalid execution context");
    IgnoredException ignoredOperationsOnTheLocator = IgnoredException
        .addIgnoredException("Operations on the locator should not to try to operate on a server");
    try (Socket socket = createSocket()) {
      ClientProtocol.Message getRegionNamesRequestMessage = ClientProtocol.Message.newBuilder()
          .setGetRegionNamesRequest(ProtobufRequestUtilities.createGetRegionNamesRequest()).build();

      long messagesReceived = getMessagesReceived();
      long messagesSent = getMessagesSent();
      long bytesReceived = getBytesReceived();
      long bytesSent = getBytesSent();
      int clientConnectionStarts = getClientConnectionStarts();
      int clientConnectionTerminations = getClientConnectionTerminations();

      ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
      protobufProtocolSerializer.serialize(getRegionNamesRequestMessage, socket.getOutputStream());

      ClientProtocol.Message getAvailableServersResponseMessage =
          protobufProtocolSerializer.deserialize(socket.getInputStream());
      assertEquals(ClientProtocol.Message.MessageTypeCase.ERRORRESPONSE,
          getAvailableServersResponseMessage.getMessageTypeCase());
      assertEquals(BasicTypes.ErrorCode.INVALID_REQUEST,
          getAvailableServersResponseMessage.getErrorResponse().getError().getErrorCode());

      validateStats(messagesReceived + 1, messagesSent + 1,
          bytesReceived + getRegionNamesRequestMessage.getSerializedSize(),
          bytesSent + getAvailableServersResponseMessage.getSerializedSize(),
          clientConnectionStarts, clientConnectionTerminations + 1);
    }
    ignoredOperationsOnTheLocator.remove();
    ignoredInvalidExecutionContext.remove();
  }

  private Statistics getStatistics() {
    InternalDistributedSystem distributedSystem =
        (InternalDistributedSystem) Locator.getLocator().getDistributedSystem();

    Statistics[] protobufServerStats = distributedSystem.findStatisticsByType(
        distributedSystem.findType(ProtobufClientStatistics.PROTOBUF_CLIENT_STATISTICS));
    assertEquals(1, protobufServerStats.length);
    return protobufServerStats[0];
  }

  private Long getBytesReceived() {
    return Host.getLocator().invoke(() -> {
      Statistics statistics = getStatistics();
      return statistics.get("bytesReceived").longValue();
    });
  }

  private Long getBytesSent() {
    return Host.getLocator().invoke(() -> {
      Statistics statistics = getStatistics();
      return statistics.get("bytesSent").longValue();
    });
  }

  private Long getMessagesReceived() {
    return Host.getLocator().invoke(() -> {
      Statistics statistics = getStatistics();
      return statistics.get("messagesReceived").longValue();
    });
  }

  private Long getMessagesSent() {
    return Host.getLocator().invoke(() -> {
      Statistics statistics = getStatistics();
      return statistics.get("messagesSent").longValue();
    });
  }

  private Integer getClientConnectionStarts() {
    return Host.getLocator().invoke(() -> {
      Statistics statistics = getStatistics();
      return statistics.get("clientConnectionStarts").intValue();
    });
  }

  private Integer getClientConnectionTerminations() {
    return Host.getLocator().invoke(() -> {
      Statistics statistics = getStatistics();
      return statistics.get("clientConnectionTerminations").intValue();
    });
  }

  private void validateGetAvailableServersResponse(
      ClientProtocol.Message getAvailableServersResponseMessage) {
    assertNotNull(getAvailableServersResponseMessage);
    assertEquals(ClientProtocol.Message.MessageTypeCase.GETSERVERRESPONSE,
        getAvailableServersResponseMessage.getMessageTypeCase());
    LocatorAPI.GetServerResponse getServerResponse =
        getAvailableServersResponseMessage.getGetServerResponse();
    assertTrue(getServerResponse.hasServer());
  }

  private void validateStats(long messagesReceived, long messagesSent, long bytesReceived,
      long bytesSent, int clientConnectionStarts, int clientConnectionTerminations) {
    Host.getLocator().invoke(() -> {
      await().untilAsserted(() -> {
        Statistics statistics = getStatistics();
        assertEquals(0, statistics.get("currentClientConnections"));
        assertEquals(messagesSent, statistics.get("messagesSent"));
        assertEquals(messagesReceived, statistics.get("messagesReceived"));
        assertEquals(bytesSent, statistics.get("bytesSent").longValue());
        assertEquals(bytesReceived, statistics.get("bytesReceived").longValue());
        assertEquals(clientConnectionStarts, statistics.get("clientConnectionStarts"));
        assertEquals(clientConnectionTerminations, statistics.get("clientConnectionTerminations"));
        assertEquals(0L, statistics.get("authorizationViolations"));
        assertEquals(0L, statistics.get("authenticationFailures"));
      });
    });
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.put(ConfigurationProperties.STATISTIC_SAMPLING_ENABLED, "true");
    properties.put(ConfigurationProperties.STATISTIC_SAMPLE_RATE, "100");
    return properties;
  }

  private Integer startCacheWithCacheServer() throws IOException {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    InternalCache cache = getCache();
    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    return cacheServer.getPort();
  }
}
