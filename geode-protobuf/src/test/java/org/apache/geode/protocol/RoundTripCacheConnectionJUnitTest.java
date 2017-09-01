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
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.GenericProtocolServerConnection;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.protocol.protobuf.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.RegionAPI;
import org.apache.geode.protocol.exception.InvalidProtocolMessageException;
import org.apache.geode.protocol.protobuf.ProtobufSerializationService;
import org.apache.geode.protocol.protobuf.serializer.ProtobufProtocolSerializer;
import org.apache.geode.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.util.test.TestUtil;

/**
 * Test that switching on the header byte makes instances of
 * {@link GenericProtocolServerConnection}.
 */
@Category(IntegrationTest.class)
public class RoundTripCacheConnectionJUnitTest {
  private final String TEST_KEY = "testKey";
  private final String TEST_VALUE = "testValue";
  private final String TEST_REGION = "testRegion";
  private final int TEST_PUT_CORRELATION_ID = 574;
  private final int TEST_GET_CORRELATION_ID = 68451;
  private final int TEST_REMOVE_CORRELATION_ID = 51;

  private final String DEFAULT_STORE = "default.keystore";
  private final String SSL_PROTOCOLS = "any";
  private final String SSL_CIPHERS = "any";

  private final String TEST_MULTIOP_KEY1 = "multiopKey1";
  private final String TEST_MULTIOP_KEY2 = "multiopKey2";
  private final String TEST_MULTIOP_KEY3 = "multiopKey3";
  private final String TEST_MULTIOP_VALUE1 = "multiopValue1";
  private final String TEST_MULTIOP_VALUE2 = "multiopValue2";
  private final String TEST_MULTIOP_VALUE3 = "multiopValue3";

  private Cache cache;
  private int cacheServerPort;
  private SerializationService serializationService;
  private Socket socket;
  private OutputStream outputStream;

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setup() throws Exception {
    // Test names prefixed with useSSL_ will setup the cache and socket to use SSL transport
    boolean useSSL = testName.getMethodName().startsWith("useSSL_");

    Properties properties = new Properties();
    if (useSSL) {
      updatePropertiesForSSLCache(properties);
    }

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

    if (useSSL) {
      socket = getSSLSocket();
    } else {
      socket = new Socket("localhost", cacheServerPort);
    }
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
    outputStream = socket.getOutputStream();
    outputStream.write(CommunicationMode.ProtobufClientServerProtocol.getModeNumber());

    serializationService = new ProtobufSerializationService();
  }

  @After
  public void cleanup() throws IOException {
    cache.close();
    socket.close();
    SocketCreatorFactory.close();
  }

  @Test
  public void testNewProtocolHeaderLeadsToNewProtocolServerConnection() throws Exception {
    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    ClientProtocol.Message putMessage =
        MessageUtil.makePutRequestMessage(serializationService, TEST_KEY, TEST_VALUE, TEST_REGION,
            ProtobufUtilities.createMessageHeader(TEST_PUT_CORRELATION_ID));
    protobufProtocolSerializer.serialize(putMessage, outputStream);
    validatePutResponse(socket, protobufProtocolSerializer);

    ClientProtocol.Message getMessage = MessageUtil.makeGetRequestMessage(serializationService,
        TEST_KEY, TEST_REGION, ProtobufUtilities.createMessageHeader(TEST_GET_CORRELATION_ID));
    protobufProtocolSerializer.serialize(getMessage, outputStream);
    validateGetResponse(socket, protobufProtocolSerializer, TEST_VALUE);
  }

  @Test
  public void testNewProtocolWithMultikeyOperations() throws Exception {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    Socket socket = new Socket("localhost", cacheServerPort);
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
    OutputStream outputStream = socket.getOutputStream();
    outputStream.write(CommunicationMode.ProtobufClientServerProtocol.getModeNumber());

    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    Set<BasicTypes.Entry> putEntries = new HashSet<>();
    putEntries.add(ProtobufUtilities.createEntry(serializationService, TEST_MULTIOP_KEY1,
        TEST_MULTIOP_VALUE1));
    putEntries.add(ProtobufUtilities.createEntry(serializationService, TEST_MULTIOP_KEY2,
        TEST_MULTIOP_VALUE2));
    putEntries.add(ProtobufUtilities.createEntry(serializationService, TEST_MULTIOP_KEY3,
        TEST_MULTIOP_VALUE3));
    ClientProtocol.Message putAllMessage = ProtobufUtilities.createProtobufMessage(
        ProtobufUtilities.createMessageHeader(TEST_PUT_CORRELATION_ID),
        ProtobufRequestUtilities.createPutAllRequest(TEST_REGION, putEntries));
    protobufProtocolSerializer.serialize(putAllMessage, outputStream);
    validatePutAllResponse(socket, protobufProtocolSerializer, new HashSet<>());

    Set<BasicTypes.EncodedValue> getEntries = new HashSet<>();
    getEntries.add(ProtobufUtilities.createEncodedValue(serializationService, TEST_MULTIOP_KEY1));
    getEntries.add(ProtobufUtilities.createEncodedValue(serializationService, TEST_MULTIOP_KEY2));
    getEntries.add(ProtobufUtilities.createEncodedValue(serializationService, TEST_MULTIOP_KEY3));

    RegionAPI.GetAllRequest getAllRequest =
        ProtobufRequestUtilities.createGetAllRequest(TEST_REGION, getEntries);

    ClientProtocol.Message getAllMessage = ProtobufUtilities.createProtobufMessage(
        ProtobufUtilities.createMessageHeader(TEST_GET_CORRELATION_ID),
        ProtobufUtilities.createProtobufRequestWithGetAllRequest(getAllRequest));
    protobufProtocolSerializer.serialize(getAllMessage, outputStream);
    validateGetAllResponse(socket, protobufProtocolSerializer);
  }

  @Test
  public void multiKeyOperationErrorsWithClasscastException() throws Exception {
    RegionFactory<Float, Object> regionFactory = cache.createRegionFactory();
    regionFactory.setKeyConstraint(Float.class);
    String regionName = "constraintRegion";
    regionFactory.create(regionName);
    System.setProperty("geode.feature-protobuf-protocol", "true");

    Socket socket = new Socket("localhost", cacheServerPort);
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
    OutputStream outputStream = socket.getOutputStream();
    outputStream.write(CommunicationMode.ProtobufClientServerProtocol.getModeNumber());

    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    Set<BasicTypes.Entry> putEntries = new HashSet<>();
    Float validKey = new Float(2.2);
    putEntries
        .add(ProtobufUtilities.createEntry(serializationService, validKey, TEST_MULTIOP_VALUE1));
    putEntries.add(ProtobufUtilities.createEntry(serializationService, TEST_MULTIOP_KEY2,
        TEST_MULTIOP_VALUE2));
    putEntries.add(ProtobufUtilities.createEntry(serializationService, TEST_MULTIOP_KEY3,
        TEST_MULTIOP_VALUE3));
    ClientProtocol.Message putAllMessage = ProtobufUtilities.createProtobufMessage(
        ProtobufUtilities.createMessageHeader(TEST_PUT_CORRELATION_ID),
        ProtobufRequestUtilities.createPutAllRequest(regionName, putEntries));

    protobufProtocolSerializer.serialize(putAllMessage, outputStream);
    HashSet<BasicTypes.EncodedValue> expectedFailedKeys = new HashSet<BasicTypes.EncodedValue>();
    expectedFailedKeys
        .add(ProtobufUtilities.createEncodedValue(serializationService, TEST_MULTIOP_KEY2));
    expectedFailedKeys
        .add(ProtobufUtilities.createEncodedValue(serializationService, TEST_MULTIOP_KEY3));
    validatePutAllResponse(socket, protobufProtocolSerializer, expectedFailedKeys);

    ClientProtocol.Message getMessage = MessageUtil.makeGetRequestMessage(serializationService,
        validKey, regionName, ProtobufUtilities.createMessageHeader(TEST_GET_CORRELATION_ID));
    protobufProtocolSerializer.serialize(getMessage, outputStream);
    validateGetResponse(socket, protobufProtocolSerializer, TEST_MULTIOP_VALUE1);

    ClientProtocol.Message removeMessage = ProtobufUtilities.createProtobufMessage(
        ProtobufUtilities.createMessageHeader(TEST_REMOVE_CORRELATION_ID),
        ProtobufRequestUtilities.createRemoveRequest(TEST_REGION,
            ProtobufUtilities.createEncodedValue(serializationService, TEST_KEY)));
    protobufProtocolSerializer.serialize(removeMessage, outputStream);
    validateRemoveResponse(socket, protobufProtocolSerializer);
  }

  @Test
  public void testNullResponse() throws Exception {
    // Get request without any data set must return a null
    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    ClientProtocol.Message getMessage = MessageUtil.makeGetRequestMessage(serializationService,
        TEST_KEY, TEST_REGION, ProtobufUtilities.createMessageHeader(TEST_GET_CORRELATION_ID));
    protobufProtocolSerializer.serialize(getMessage, outputStream);

    ClientProtocol.Response response =
        deserializeResponse(socket, protobufProtocolSerializer, TEST_GET_CORRELATION_ID);
    assertEquals(ClientProtocol.Response.ResponseAPICase.GETRESPONSE,
        response.getResponseAPICase());
    RegionAPI.GetResponse getResponse = response.getGetResponse();

    assertFalse(getResponse.hasResult());
  }

  @Test
  public void testConnectionCountIsProperlyDecremented() throws Exception {
    CacheServer cacheServer = this.cache.getCacheServers().stream().findFirst().get();
    AcceptorImpl acceptor = ((CacheServerImpl) cacheServer).getAcceptor();
    Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
      return acceptor.getClientServerCnxCount() == 1;
    });
    // run another test that creates a connection to the server
    testNewProtocolGetRegionNamesCallSucceeds();
    assertFalse(socket.isClosed());
    socket.close();
    Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
      return acceptor.getClientServerCnxCount() == 0;
    });
  }

  @Test
  public void testNewProtocolRespectsMaxConnectionLimit() throws IOException, InterruptedException {
    cache.close();

    CacheFactory cacheFactory = new CacheFactory();
    Cache cache = cacheFactory.create();

    CacheServer cacheServer = cache.addCacheServer();
    final int cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer.setPort(cacheServerPort);
    cacheServer.setMaxConnections(16);
    cacheServer.setMaxThreads(16);
    cacheServer.start();

    AcceptorImpl acceptor = ((CacheServerImpl) cacheServer).getAcceptor();

    Socket[] sockets = new Socket[16];

    for (int i = 0; i < 16; i++) {
      Socket socket = new Socket("localhost", cacheServerPort);
      sockets[i] = socket;
      Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
      socket.getOutputStream()
          .write(CommunicationMode.ProtobufClientServerProtocol.getModeNumber());
    }

    try (Socket socket = new Socket("localhost", cacheServerPort)) {
      Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
      socket.getOutputStream()
          .write(CommunicationMode.ProtobufClientServerProtocol.getModeNumber());
      assertEquals(-1, socket.getInputStream().read());
    }

    for (Socket currentSocket : sockets) {
      currentSocket.close();
    }

    Awaitility.await().atMost(5, TimeUnit.SECONDS)
        .until(() -> acceptor.getClientServerCnxCount() == 0);

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

  @Test
  public void testNewProtocolGetRegionNamesCallSucceeds() throws Exception {
    int correlationId = TEST_GET_CORRELATION_ID; // reuse this value for this test

    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    RegionAPI.GetRegionNamesRequest getRegionNamesRequest =
        ProtobufRequestUtilities.createGetRegionNamesRequest();

    ClientProtocol.Message getRegionsMessage = ProtobufUtilities.createProtobufMessage(
        ProtobufUtilities.createMessageHeader(correlationId),
        ProtobufUtilities.createProtobufRequestWithGetRegionNamesRequest(getRegionNamesRequest));
    protobufProtocolSerializer.serialize(getRegionsMessage, outputStream);
    validateGetRegionNamesResponse(socket, correlationId, protobufProtocolSerializer);
  }

  @Test
  public void useSSL_testNewProtocolHeaderLeadsToNewProtocolServerConnection() throws Exception {
    testNewProtocolHeaderLeadsToNewProtocolServerConnection();
  }

  @Test
  public void testNewProtocolGetRegionCallSucceeds() throws Exception {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    Socket socket = new Socket("localhost", cacheServerPort);
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
    OutputStream outputStream = socket.getOutputStream();
    outputStream.write(CommunicationMode.ProtobufClientServerProtocol.getModeNumber());

    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    ClientProtocol.Message getRegionMessage = MessageUtil.makeGetRegionRequestMessage(TEST_REGION,
        ClientProtocol.MessageHeader.newBuilder().build());
    protobufProtocolSerializer.serialize(getRegionMessage, outputStream);
    ClientProtocol.Message message =
        protobufProtocolSerializer.deserialize(socket.getInputStream());
    assertEquals(ClientProtocol.Message.MessageTypeCase.RESPONSE, message.getMessageTypeCase());
    ClientProtocol.Response response = message.getResponse();
    assertEquals(ClientProtocol.Response.ResponseAPICase.GETREGIONRESPONSE,
        response.getResponseAPICase());
    RegionAPI.GetRegionResponse getRegionResponse = response.getGetRegionResponse();
    BasicTypes.Region region = getRegionResponse.getRegion();

    assertEquals(TEST_REGION, region.getName());
    assertEquals(0, region.getSize());
    assertEquals(false, region.getPersisted());
    assertEquals(DataPolicy.NORMAL.toString(), region.getDataPolicy());
    assertEquals("", region.getKeyConstraint());
    assertEquals("", region.getValueConstraint());
    assertEquals(Scope.DISTRIBUTED_NO_ACK, Scope.fromString(region.getScope()));
  }

  private void validatePutResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer) throws Exception {
    ClientProtocol.Response response =
        deserializeResponse(socket, protobufProtocolSerializer, TEST_PUT_CORRELATION_ID);
    assertEquals(ClientProtocol.Response.ResponseAPICase.PUTRESPONSE,
        response.getResponseAPICase());
  }

  private void validateGetResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer, Object expectedValue)
      throws InvalidProtocolMessageException, IOException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException, CodecAlreadyRegisteredForTypeException {
    ClientProtocol.Response response =
        deserializeResponse(socket, protobufProtocolSerializer, TEST_GET_CORRELATION_ID);

    assertEquals(ClientProtocol.Response.ResponseAPICase.GETRESPONSE,
        response.getResponseAPICase());
    RegionAPI.GetResponse getResponse = response.getGetResponse();
    BasicTypes.EncodedValue result = getResponse.getResult();
    assertEquals(BasicTypes.EncodedValue.ValueCase.STRINGRESULT, result.getValueCase());
    assertEquals(expectedValue, result.getStringResult());
  }

  private ClientProtocol.Response deserializeResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer, int expectedCorrelationId)
      throws InvalidProtocolMessageException, IOException {
    ClientProtocol.Message message =
        protobufProtocolSerializer.deserialize(socket.getInputStream());
    assertEquals(expectedCorrelationId, message.getMessageHeader().getCorrelationId());
    assertEquals(ClientProtocol.Message.MessageTypeCase.RESPONSE, message.getMessageTypeCase());
    return message.getResponse();
  }

  private void validateGetRegionNamesResponse(Socket socket, int correlationId,
      ProtobufProtocolSerializer protobufProtocolSerializer)
      throws InvalidProtocolMessageException, IOException {
    ClientProtocol.Response response =
        deserializeResponse(socket, protobufProtocolSerializer, correlationId);

    assertEquals(ClientProtocol.Response.ResponseAPICase.GETREGIONNAMESRESPONSE,
        response.getResponseAPICase());
    RegionAPI.GetRegionNamesResponse getRegionsResponse = response.getGetRegionNamesResponse();
    assertEquals(1, getRegionsResponse.getRegionsCount());
    assertEquals(TEST_REGION, getRegionsResponse.getRegions(0));
  }

  private void validatePutAllResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer,
      Collection<BasicTypes.EncodedValue> expectedFailedKeys) throws Exception {
    ClientProtocol.Response response =
        deserializeResponse(socket, protobufProtocolSerializer, TEST_PUT_CORRELATION_ID);

    assertEquals(ClientProtocol.Response.ResponseAPICase.PUTALLRESPONSE,
        response.getResponseAPICase());
    assertEquals(expectedFailedKeys.size(), response.getPutAllResponse().getFailedKeysCount());

    Stream<BasicTypes.EncodedValue> failedKeyStream = response.getPutAllResponse()
        .getFailedKeysList().stream().map(BasicTypes.KeyedErrorResponse::getKey);
    assertTrue(failedKeyStream.allMatch(expectedFailedKeys::contains));

  }

  private void validateGetAllResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer)
      throws InvalidProtocolMessageException, IOException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException, CodecAlreadyRegisteredForTypeException {
    ClientProtocol.Response response =
        deserializeResponse(socket, protobufProtocolSerializer, TEST_GET_CORRELATION_ID);
    assertEquals(ClientProtocol.Response.ResponseAPICase.GETALLRESPONSE,
        response.getResponseAPICase());
    RegionAPI.GetAllResponse getAllResponse = response.getGetAllResponse();
    assertEquals(3, getAllResponse.getEntriesCount());
    for (BasicTypes.Entry result : getAllResponse.getEntriesList()) {
      String key = (String) ProtobufUtilities.decodeValue(serializationService, result.getKey());
      String value =
          (String) ProtobufUtilities.decodeValue(serializationService, result.getValue());
      switch (key) {
        case TEST_MULTIOP_KEY1:
          assertEquals(TEST_MULTIOP_VALUE1, value);
          break;
        case TEST_MULTIOP_KEY2:
          assertEquals(TEST_MULTIOP_VALUE2, value);
          break;
        case TEST_MULTIOP_KEY3:
          assertEquals(TEST_MULTIOP_VALUE3, value);
          break;
        default:
          Assert.fail("Unexpected key found by getAll: " + key);
      }
    }
  }

  private void validateRemoveResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer) throws Exception {
    ClientProtocol.Response response =
        deserializeResponse(socket, protobufProtocolSerializer, TEST_REMOVE_CORRELATION_ID);
    assertEquals(ClientProtocol.Response.ResponseAPICase.REMOVERESPONSE,
        response.getResponseAPICase());
  }

  private void updatePropertiesForSSLCache(Properties properties) {
    String keyStore =
        TestUtil.getResourcePath(RoundTripCacheConnectionJUnitTest.class, DEFAULT_STORE);
    String trustStore =
        TestUtil.getResourcePath(RoundTripCacheConnectionJUnitTest.class, DEFAULT_STORE);

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
    String keyStorePath =
        TestUtil.getResourcePath(RoundTripCacheConnectionJUnitTest.class, DEFAULT_STORE);
    String trustStorePath =
        TestUtil.getResourcePath(RoundTripCacheConnectionJUnitTest.class, DEFAULT_STORE);

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
