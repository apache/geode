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

import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.internal.protocol.protobuf.v1.MessageUtil.validateGetResponse;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

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
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.Statistics;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.protocol.protobuf.statistics.ProtobufClientStatistics;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.MessageUtil;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.serializer.ProtobufProtocolSerializer;
import org.apache.geode.internal.protocol.protobuf.v1.serializer.exception.InvalidProtocolMessageException;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

/**
 * Test that using the magic byte to indicate intend ot use ProtoBuf messages works
 */
@Category(ClientServerTest.class)
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class CacheConnectionIntegrationTest {

  private static final String TEST_KEY = "testKey";
  private static final String TEST_VALUE = "testValue";
  private static final String TEST_REGION = "testRegion";

  /*
   * This file was generated with the following command:
   * keytool -genkey -dname "CN=localhost" -alias self -validity 3650 -keyalg EC \
   * -keystore default.keystore -keypass password -storepass password \
   * -ext san=ip:127.0.0.1 -storetype jks
   */
  private static final String DEFAULT_STORE = "default.keystore";
  private static final String SSL_PROTOCOLS_VALUE = "any";
  private static final String SSL_CIPHERS_VALUE = "any";


  private Cache cache;
  private int cacheServerPort;
  private ProtobufSerializationService serializationService;
  private Socket socket;
  private OutputStream outputStream;

  @Parameter()
  public boolean useSSL;

  @Parameters(name = "use ssl {0}")
  public static Collection<Boolean> data() {
    return Arrays.asList(false, true);
  }

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

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
    await().until(socket::isConnected);
    outputStream = socket.getOutputStream();

    MessageUtil.performAndVerifyHandshake(socket);

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
        distributedSystem.findType(ProtobufClientStatistics.PROTOBUF_CLIENT_STATISTICS));
    assertEquals(1, protobufStats.length);
    Statistics statistics = protobufStats[0];
    assertEquals(1L, statistics.get("currentClientConnections"));
    assertEquals(3L, statistics.get("messagesReceived"));
    assertEquals(3L, statistics.get("messagesSent"));
    assertTrue(statistics.get("bytesReceived").longValue() > 0);
    assertTrue(statistics.get("bytesSent").longValue() > 0);
    assertEquals(1L, statistics.get("clientConnectionStarts"));
    assertEquals(0L, statistics.get("clientConnectionTerminations"));
    assertEquals(0L, statistics.get("authorizationViolations"));
    assertEquals(0L, statistics.get("authenticationFailures"));
  }

  @Test
  public void testConnectionCountIsProperlyDecremented() throws Exception {
    List<CacheServer> cacheServers = cache.getCacheServers();
    assertEquals(1, cacheServers.size());
    CacheServer cacheServer = cacheServers.stream().findFirst().get();
    Acceptor acceptor = ((InternalCacheServer) cacheServer).getAcceptor();

    await().until(() -> acceptor.getClientServerConnectionCount() == 1);

    // make a request to the server
    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    ClientProtocol.Message getMessage =
        MessageUtil.makeGetRequestMessage(serializationService, TEST_KEY, TEST_REGION);
    protobufProtocolSerializer.serialize(getMessage, outputStream);
    // Make sure we're done processing the message server side.
    socket.getInputStream().read();

    // make sure socket is still open
    assertFalse(socket.isClosed());
    socket.close();
    await().until(() -> acceptor.getClientServerConnectionCount() == 0);
  }

  private void validatePutResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer) throws Exception {
    ClientProtocol.Message response = deserializeResponse(socket, protobufProtocolSerializer);
    assertEquals(ClientProtocol.Message.MessageTypeCase.PUTRESPONSE, response.getMessageTypeCase());
  }

  private ClientProtocol.Message deserializeResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer)
      throws InvalidProtocolMessageException, IOException {
    return protobufProtocolSerializer.deserialize(socket.getInputStream());
  }

  private void updatePropertiesForSSLCache(Properties properties) {
    String keyStore =
        createTempFileFromResource(CacheConnectionIntegrationTest.class, DEFAULT_STORE)
            .getAbsolutePath();
    String trustStore =
        createTempFileFromResource(CacheConnectionIntegrationTest.class, DEFAULT_STORE)
            .getAbsolutePath();

    properties.setProperty(SSL_ENABLED_COMPONENTS, "server");
    properties.setProperty(SSL_PROTOCOLS, SSL_PROTOCOLS_VALUE);
    properties.setProperty(SSL_CIPHERS, SSL_CIPHERS_VALUE);
    properties.setProperty(SSL_REQUIRE_AUTHENTICATION, String.valueOf(true));

    properties.setProperty(SSL_KEYSTORE_TYPE, "jks");
    properties.setProperty(SSL_KEYSTORE, keyStore);
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    properties.setProperty(SSL_TRUSTSTORE, trustStore);
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
  }

  private Socket getSSLSocket() throws IOException {
    String keyStorePath =
        createTempFileFromResource(CacheConnectionIntegrationTest.class, DEFAULT_STORE)
            .getAbsolutePath();
    String trustStorePath =
        createTempFileFromResource(CacheConnectionIntegrationTest.class, DEFAULT_STORE)
            .getAbsolutePath();

    SSLConfig sslConfig = new SSLConfig();
    sslConfig.setEnabled(true);
    sslConfig.setCiphers(SSL_CIPHERS_VALUE);
    sslConfig.setProtocols(SSL_PROTOCOLS_VALUE);
    sslConfig.setRequireAuth(true);
    sslConfig.setKeystoreType("jks");
    sslConfig.setKeystore(keyStorePath);
    sslConfig.setKeystorePassword("password");
    sslConfig.setTruststore(trustStorePath);
    sslConfig.setKeystorePassword("password");
    sslConfig.setEndpointIdentificationEnabled(false);

    SocketCreator socketCreator = new SocketCreator(sslConfig);
    return socketCreator.connectForClient("localhost", cacheServerPort, 5000);
  }
}
