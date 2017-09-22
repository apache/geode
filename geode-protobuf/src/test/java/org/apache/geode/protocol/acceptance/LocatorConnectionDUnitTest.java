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

import static org.apache.geode.internal.cache.tier.CommunicationMode.ProtobufClientServerProtocol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.DataOutputStream;
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
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.ServerAPI;
import org.apache.geode.protocol.exception.InvalidProtocolMessageException;
import org.apache.geode.protocol.protobuf.ProtocolErrorCode;
import org.apache.geode.protocol.protobuf.serializer.ProtobufProtocolSerializer;
import org.apache.geode.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.DistributedTest;

/*
 * Test sending ProtoBuf messages to the locator
 */
@Category(DistributedTest.class)
public class LocatorConnectionDUnitTest extends JUnit4CacheTestCase {

  @Rule
  public final DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setup() throws IOException {
    startCacheWithCacheServer();

    Host.getLocator().invoke(() -> System.setProperty("geode.feature-protobuf-protocol", "true"));
  }

  private Socket createSocket() throws IOException {
    Host host = Host.getHost(0);
    int locatorPort = DistributedTestUtils.getDUnitLocatorPort();
    Socket socket = new Socket(host.getHostName(), locatorPort);
    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
    dataOutputStream.writeInt(0);
    // Using the constant from AcceptorImpl to ensure that magic byte is the same
    dataOutputStream.writeByte(ProtobufClientServerProtocol.getModeNumber());
    return socket;
  }

  // Test getAvailableServers twice, validating stats before any messages, after 1, and after 2.
  @Test
  public void testGetAvailableServersWithStats() throws Throwable {
    ClientProtocol.Request.Builder protobufRequestBuilder =
        ProtobufUtilities.createProtobufRequestBuilder();
    ClientProtocol.Message getAvailableServersRequestMessage =
        ProtobufUtilities.createProtobufMessage(ProtobufUtilities.createMessageHeader(1233445),
            protobufRequestBuilder.setGetAvailableServersRequest(
                ProtobufRequestUtilities.createGetAvailableServersRequest()).build());

    try {
      ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();

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
    } catch (RMIException e) {
      throw e.getCause(); // so that assertions propagate properly.
    }
  }

  @Test
  public void testInvalidOperationReturnsFailure()
      throws IOException, InvalidProtocolMessageException {
    IgnoredException ignoredInvalidExecutionContext =
        IgnoredException.addIgnoredException("Invalid execution context");
    try (Socket socket = createSocket()) {

      ClientProtocol.Request.Builder protobufRequestBuilder =
          ProtobufUtilities.createProtobufRequestBuilder();
      ClientProtocol.Message getRegionNamesRequestMessage =
          ProtobufUtilities.createProtobufMessage(ProtobufUtilities.createMessageHeader(1233445),
              protobufRequestBuilder
                  .setGetRegionNamesRequest(ProtobufRequestUtilities.createGetRegionNamesRequest())
                  .build());

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
      assertEquals(1233445,
          getAvailableServersResponseMessage.getMessageHeader().getCorrelationId());
      assertEquals(ClientProtocol.Message.MessageTypeCase.RESPONSE,
          getAvailableServersResponseMessage.getMessageTypeCase());
      ClientProtocol.Response messageResponse = getAvailableServersResponseMessage.getResponse();
      assertEquals(ClientProtocol.Response.ResponseAPICase.ERRORRESPONSE,
          messageResponse.getResponseAPICase());
      assertEquals(ProtocolErrorCode.UNSUPPORTED_OPERATION.codeValue,
          messageResponse.getErrorResponse().getError().getErrorCode());

      validateStats(messagesReceived + 1, messagesSent + 1,
          bytesReceived + getRegionNamesRequestMessage.getSerializedSize(),
          bytesSent + getAvailableServersResponseMessage.getSerializedSize(),
          clientConnectionStarts, clientConnectionTerminations + 1);
    }
    ignoredInvalidExecutionContext.remove();
  }

  private Long getBytesReceived() {
    return Host.getLocator().invoke(() -> {
      InternalDistributedSystem distributedSystem =
          (InternalDistributedSystem) Locator.getLocator().getDistributedSystem();

      Statistics[] protobufServerStats =
          distributedSystem.findStatisticsByType(distributedSystem.findType("ProtobufServerStats"));
      assertEquals(1, protobufServerStats.length);
      Statistics statistics = protobufServerStats[0];
      return statistics.get("bytesReceived").longValue();
    });
  }

  private Long getBytesSent() {
    return Host.getLocator().invoke(() -> {
      InternalDistributedSystem distributedSystem =
          (InternalDistributedSystem) Locator.getLocator().getDistributedSystem();

      Statistics[] protobufServerStats =
          distributedSystem.findStatisticsByType(distributedSystem.findType("ProtobufServerStats"));
      assertEquals(1, protobufServerStats.length);
      Statistics statistics = protobufServerStats[0];
      return statistics.get("bytesSent").longValue();
    });
  }

  private Long getMessagesReceived() {
    return Host.getLocator().invoke(() -> {
      InternalDistributedSystem distributedSystem =
          (InternalDistributedSystem) Locator.getLocator().getDistributedSystem();

      Statistics[] protobufServerStats =
          distributedSystem.findStatisticsByType(distributedSystem.findType("ProtobufServerStats"));
      assertEquals(1, protobufServerStats.length);
      Statistics statistics = protobufServerStats[0];
      return statistics.get("messagesReceived").longValue();
    });
  }

  private Long getMessagesSent() {
    return Host.getLocator().invoke(() -> {
      InternalDistributedSystem distributedSystem =
          (InternalDistributedSystem) Locator.getLocator().getDistributedSystem();

      Statistics[] protobufServerStats =
          distributedSystem.findStatisticsByType(distributedSystem.findType("ProtobufServerStats"));
      assertEquals(1, protobufServerStats.length);
      Statistics statistics = protobufServerStats[0];
      return statistics.get("messagesSent").longValue();
    });
  }

  private Integer getClientConnectionStarts() {
    return Host.getLocator().invoke(() -> {
      InternalDistributedSystem distributedSystem =
          (InternalDistributedSystem) Locator.getLocator().getDistributedSystem();

      Statistics[] protobufServerStats =
          distributedSystem.findStatisticsByType(distributedSystem.findType("ProtobufServerStats"));
      assertEquals(1, protobufServerStats.length);
      Statistics statistics = protobufServerStats[0];
      return statistics.get("clientConnectionStarts").intValue();
    });
  }

  private Integer getClientConnectionTerminations() {
    return Host.getLocator().invoke(() -> {
      InternalDistributedSystem distributedSystem =
          (InternalDistributedSystem) Locator.getLocator().getDistributedSystem();

      Statistics[] protobufServerStats =
          distributedSystem.findStatisticsByType(distributedSystem.findType("ProtobufServerStats"));
      assertEquals(1, protobufServerStats.length);
      Statistics statistics = protobufServerStats[0];
      return statistics.get("clientConnectionTerminations").intValue();
    });
  }

  private void validateGetAvailableServersResponse(
      ClientProtocol.Message getAvailableServersResponseMessage)
      throws InvalidProtocolMessageException, IOException {
    assertNotNull(getAvailableServersResponseMessage);
    assertEquals(1233445, getAvailableServersResponseMessage.getMessageHeader().getCorrelationId());
    assertEquals(ClientProtocol.Message.MessageTypeCase.RESPONSE,
        getAvailableServersResponseMessage.getMessageTypeCase());
    ClientProtocol.Response messageResponse = getAvailableServersResponseMessage.getResponse();
    assertEquals(ClientProtocol.Response.ResponseAPICase.GETAVAILABLESERVERSRESPONSE,
        messageResponse.getResponseAPICase());
    ServerAPI.GetAvailableServersResponse getAvailableServersResponse =
        messageResponse.getGetAvailableServersResponse();
    assertEquals(1, getAvailableServersResponse.getServersCount());
  }

  private void validateStats(long messagesReceived, long messagesSent, int clientConnectionStarts,
      int clientConnectionTerminations) {
    Host.getLocator().invoke(() -> {
      InternalDistributedSystem distributedSystem =
          (InternalDistributedSystem) Locator.getLocator().getDistributedSystem();

      Statistics[] protobufServerStats =
          distributedSystem.findStatisticsByType(distributedSystem.findType("ProtobufServerStats"));
      assertEquals(1, protobufServerStats.length);
      Statistics statistics = protobufServerStats[0];
      assertEquals(0, statistics.get("currentClientConnections"));
      assertEquals(messagesReceived, statistics.get("messagesReceived"));
      assertEquals(messagesSent, statistics.get("messagesSent"));
      assertTrue(statistics.get("bytesReceived").longValue() > 0);
      assertTrue(statistics.get("bytesSent").longValue() > 0);
      assertEquals(clientConnectionStarts, statistics.get("clientConnectionStarts"));
      assertEquals(clientConnectionTerminations, statistics.get("clientConnectionTerminations"));
      assertEquals(0L, statistics.get("authorizationViolations"));
      assertEquals(0L, statistics.get("authenticationFailures"));
    });
  }

  private void validateStats(long messagesReceived, long messagesSent, long bytesReceived,
      long bytesSent, int clientConnectionStarts, int clientConnectionTerminations) {
    Host.getLocator().invoke(() -> {
      InternalDistributedSystem distributedSystem =
          (InternalDistributedSystem) Locator.getLocator().getDistributedSystem();

      Statistics[] protobufServerStats =
          distributedSystem.findStatisticsByType(distributedSystem.findType("ProtobufServerStats"));
      assertEquals(1, protobufServerStats.length);
      Statistics statistics = protobufServerStats[0];
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
