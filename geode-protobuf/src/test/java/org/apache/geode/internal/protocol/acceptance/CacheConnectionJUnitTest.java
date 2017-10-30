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

package org.apache.geode.internal.protocol.acceptance;

import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.Statistics;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.protocol.MessageUtil;
import org.apache.geode.internal.protocol.exception.InvalidProtocolMessageException;
import org.apache.geode.internal.protocol.protobuf.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.serializer.ProtobufProtocolSerializer;
import org.apache.geode.internal.protocol.protobuf.statistics.ProtobufClientStatisticsImpl;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.util.test.TestUtil;

/**
 * Test that using the magic byte to indicate intend ot use ProtoBuf messages works
 */
@Category(IntegrationTest.class)
@RunWith(value = Parameterized.class)
public class CacheConnectionJUnitTest {
  private final String TEST_KEY = "testKey";
  private final String TEST_VALUE = "testValue";
  private final String TEST_REGION = "testRegion";

  private final String DEFAULT_STORE = "default.keystore";
  private final String SSL_PROTOCOLS = "any";
  private final String SSL_CIPHERS = "any";


  private Cache cache;
  private int cacheServerPort;
  private SerializationService serializationService;
  private Socket socket;
  private OutputStream outputStream;

  @Parameterized.Parameter()
  public boolean useSSL;

  @Parameterized.Parameters(name = "use ssl {0}")
  public static Collection<Boolean> data() {
    return Arrays.asList(false, true);
  }

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setup() throws Exception {
    Properties properties = new Properties();
    if (useSSL) {
      updatePropertiesForSSLCache(properties);
    }

    CacheFactory cacheFactory = new CacheFactory(properties);
    cacheFactory.set(ConfigurationProperties.MCAST_PORT, "0");
    cacheFactory.set(ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION, "false");
    cacheFactory.set(ConfigurationProperties.USE_CLUSTER_CONFIGURATION, "false");
    cacheFactory.set(ConfigurationProperties.STATISTIC_SAMPLE_RATE, "100");
    cacheFactory.setSecurityManager(null);
    cache = cacheFactory.create();

    CacheServer cacheServer = cache.addCacheServer();
    cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer.setPort(cacheServerPort);
    cacheServer.start();

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    regionFactory.create(TEST_REGION);

    System.setProperty("geode.feature-protobuf-protocol", "true");

    if (useSSL) {
      socket = getSSLSocket();
    } else {
      socket = new Socket("localhost", cacheServerPort);
    }
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
    outputStream = socket.getOutputStream();
    outputStream.write(110);

    serializationService = new ProtobufSerializationService();
  }

  @After
  public void cleanup() throws IOException {
    cache.close();
    socket.close();
    SocketCreatorFactory.close();
  }

  @Test
  public void testBasicMessagesAndStats() throws Exception {
    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    ClientProtocol.Message putMessage =
        MessageUtil.makePutRequestMessage(serializationService, TEST_KEY, TEST_VALUE, TEST_REGION);
    protobufProtocolSerializer.serialize(putMessage, outputStream);
    validatePutResponse(socket, protobufProtocolSerializer);

    ClientProtocol.Message getMessage =
        MessageUtil.makeGetRequestMessage(serializationService, TEST_KEY, TEST_REGION);
    protobufProtocolSerializer.serialize(getMessage, outputStream);
    validateGetResponse(socket, protobufProtocolSerializer, TEST_VALUE);

    InternalDistributedSystem distributedSystem =
        (InternalDistributedSystem) cache.getDistributedSystem();
    Statistics[] protobufStats = distributedSystem.findStatisticsByType(
        distributedSystem.findType(ProtobufClientStatisticsImpl.PROTOBUF_CLIENT_STATISTICS));
    assertEquals(1, protobufStats.length);
    Statistics statistics = protobufStats[0];
    assertEquals(1, statistics.get("currentClientConnections"));
    assertEquals(2L, statistics.get("messagesReceived"));
    assertEquals(2L, statistics.get("messagesSent"));
    assertTrue(statistics.get("bytesReceived").longValue() > 0);
    assertTrue(statistics.get("bytesSent").longValue() > 0);
    assertEquals(1, statistics.get("clientConnectionStarts"));
    assertEquals(0, statistics.get("clientConnectionTerminations"));
    assertEquals(0L, statistics.get("authorizationViolations"));
    assertEquals(0L, statistics.get("authenticationFailures"));
  }

  @Test
  public void testConnectionCountIsProperlyDecremented() throws Exception {
    CacheServer cacheServer = this.cache.getCacheServers().stream().findFirst().get();
    AcceptorImpl acceptor = ((CacheServerImpl) cacheServer).getAcceptor();
    Awaitility.await().atMost(30, TimeUnit.SECONDS)
        .until(() -> acceptor.getClientServerCnxCount() == 1);

    // make a request to the server
    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    ClientProtocol.Message getMessage =
        MessageUtil.makeGetRequestMessage(serializationService, TEST_KEY, TEST_REGION);
    protobufProtocolSerializer.serialize(getMessage, outputStream);

    // make sure socket is still open
    assertFalse(socket.isClosed());
    socket.close();
    Awaitility.await().atMost(30, TimeUnit.SECONDS)
        .until(() -> acceptor.getClientServerCnxCount() == 0);
  }

  private void validatePutResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer) throws Exception {
    ClientProtocol.Response response = deserializeResponse(socket, protobufProtocolSerializer);
    assertEquals(ClientProtocol.Response.ResponseAPICase.PUTRESPONSE,
        response.getResponseAPICase());
  }

  private void validateGetResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer, Object expectedValue)
      throws InvalidProtocolMessageException, IOException {
    ClientProtocol.Response response = deserializeResponse(socket, protobufProtocolSerializer);

    assertEquals(ClientProtocol.Response.ResponseAPICase.GETRESPONSE,
        response.getResponseAPICase());
    RegionAPI.GetResponse getResponse = response.getGetResponse();
    BasicTypes.EncodedValue result = getResponse.getResult();
    assertEquals(BasicTypes.EncodedValue.ValueCase.STRINGRESULT, result.getValueCase());
    assertEquals(expectedValue, result.getStringResult());
  }

  private ClientProtocol.Response deserializeResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer)
      throws InvalidProtocolMessageException, IOException {
    ClientProtocol.Message message =
        protobufProtocolSerializer.deserialize(socket.getInputStream());
    assertEquals(ClientProtocol.Message.MessageTypeCase.RESPONSE, message.getMessageTypeCase());
    return message.getResponse();
  }

  private void updatePropertiesForSSLCache(Properties properties) {
    String keyStore = TestUtil.getResourcePath(CacheConnectionJUnitTest.class, DEFAULT_STORE);
    String trustStore = TestUtil.getResourcePath(CacheConnectionJUnitTest.class, DEFAULT_STORE);

    properties.put(SSL_ENABLED_COMPONENTS, "server");
    properties.put(ConfigurationProperties.SSL_PROTOCOLS, SSL_PROTOCOLS);
    properties.put(ConfigurationProperties.SSL_CIPHERS, SSL_CIPHERS);
    properties.put(SSL_REQUIRE_AUTHENTICATION, String.valueOf(true));

    properties.put(SSL_KEYSTORE_TYPE, "jks");
    properties.put(SSL_KEYSTORE, keyStore);
    properties.put(SSL_KEYSTORE_PASSWORD, "password");
    properties.put(SSL_TRUSTSTORE, trustStore);
    properties.put(SSL_TRUSTSTORE_PASSWORD, "password");
  }

  private Socket getSSLSocket() throws IOException {
    String keyStorePath = TestUtil.getResourcePath(CacheConnectionJUnitTest.class, DEFAULT_STORE);
    String trustStorePath = TestUtil.getResourcePath(CacheConnectionJUnitTest.class, DEFAULT_STORE);

    SSLConfig sslConfig = new SSLConfig();
    sslConfig.setEnabled(true);
    sslConfig.setCiphers(SSL_CIPHERS);
    sslConfig.setProtocols(SSL_PROTOCOLS);
    sslConfig.setRequireAuth(true);
    sslConfig.setKeystoreType("jks");
    sslConfig.setKeystore(keyStorePath);
    sslConfig.setKeystorePassword("password");
    sslConfig.setTruststore(trustStorePath);
    sslConfig.setKeystorePassword("password");

    SocketCreator socketCreator = new SocketCreator(sslConfig);
    return socketCreator.connectForClient("localhost", cacheServerPort, 5000);
  }
}
