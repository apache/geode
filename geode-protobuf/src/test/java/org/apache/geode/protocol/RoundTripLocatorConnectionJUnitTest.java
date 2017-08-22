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

package org.apache.geode.protocol;

import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.protocol.exception.InvalidProtocolMessageException;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.ProtocolErrorCode;
import org.apache.geode.protocol.protobuf.ServerAPI;
import org.apache.geode.protocol.protobuf.serializer.ProtobufProtocolSerializer;
import org.apache.geode.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class RoundTripLocatorConnectionJUnitTest extends JUnit4CacheTestCase {

  private Socket socket;
  private DataOutputStream dataOutputStream;
  private Locator locator;

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setup() throws IOException {
    Host host = Host.getHost(0);
    int locatorPort = DistributedTestUtils.getDUnitLocatorPort();
    int cacheServer1Port = startCacheWithCacheServer();

    Host.getLocator().invoke(() -> System.setProperty("geode.feature-protobuf-protocol", "true"));

    socket = new Socket(host.getHostName(), locatorPort);
    dataOutputStream = new DataOutputStream(socket.getOutputStream());
    dataOutputStream.writeInt(0);
    // Using the constant from AcceptorImpl to ensure that magic byte is the same
    dataOutputStream.writeByte(AcceptorImpl.PROTOBUF_CLIENT_SERVER_PROTOCOL);
  }

  @Test
  public void testEchoProtobufMessageFromLocator()
      throws IOException, InvalidProtocolMessageException {
    ClientProtocol.Request.Builder protobufRequestBuilder =
        ProtobufUtilities.createProtobufRequestBuilder();
    ClientProtocol.Message getAvailableServersRequestMessage =
        ProtobufUtilities.createProtobufMessage(ProtobufUtilities.createMessageHeader(1233445),
            protobufRequestBuilder.setGetAvailableServersRequest(
                ProtobufRequestUtilities.createGetAvailableServersRequest()).build());

    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    protobufProtocolSerializer.serialize(getAvailableServersRequestMessage,
        socket.getOutputStream());

    ClientProtocol.Message getAvailableServersResponseMessage =
        protobufProtocolSerializer.deserialize(socket.getInputStream());
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

  @Test
  public void testInvalidOperationReturnsFailure()
      throws IOException, InvalidProtocolMessageException {
    ClientProtocol.Request.Builder protobufRequestBuilder =
        ProtobufUtilities.createProtobufRequestBuilder();
    ClientProtocol.Message getAvailableServersRequestMessage =
        ProtobufUtilities.createProtobufMessage(ProtobufUtilities.createMessageHeader(1233445),
            protobufRequestBuilder
                .setGetRegionNamesRequest(ProtobufRequestUtilities.createGetRegionNamesRequest())
                .build());

    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    protobufProtocolSerializer.serialize(getAvailableServersRequestMessage,
        socket.getOutputStream());

    ClientProtocol.Message getAvailableServersResponseMessage =
        protobufProtocolSerializer.deserialize(socket.getInputStream());
    assertEquals(1233445, getAvailableServersResponseMessage.getMessageHeader().getCorrelationId());
    assertEquals(ClientProtocol.Message.MessageTypeCase.RESPONSE,
        getAvailableServersResponseMessage.getMessageTypeCase());
    ClientProtocol.Response messageResponse = getAvailableServersResponseMessage.getResponse();
    assertEquals(ClientProtocol.Response.ResponseAPICase.ERRORRESPONSE,
        messageResponse.getResponseAPICase());
    assertEquals(ProtocolErrorCode.UNSUPPORTED_OPERATION.codeValue,
        messageResponse.getErrorResponse().getError().getErrorCode());
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
