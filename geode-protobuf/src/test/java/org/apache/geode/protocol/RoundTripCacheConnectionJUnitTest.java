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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.cache.tier.sockets.GenericProtocolServerConnection;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.protocol.exception.InvalidProtocolMessageException;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.ProtobufSerializationService;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.protocol.protobuf.serializer.ProtobufProtocolSerializer;
import org.apache.geode.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.serialization.SerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.util.test.TestUtil;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test that switching on the header byte makes instances of
 * {@link GenericProtocolServerConnection}.
 */
@Category(IntegrationTest.class)
public class RoundTripCacheConnectionJUnitTest {
  public static final String TEST_KEY = "testKey";
  public static final String TEST_VALUE = "testValue";
  public static final String TEST_REGION = "testRegion";
  public static final int TEST_PUT_CORRELATION_ID = 574;
  public static final int TEST_GET_CORRELATION_ID = 68451;
  public static final int TEST_REMOVE_CORRELATION_ID = 51;

  private static final String DEFAULT_STORE = "default.keystore";
  private static final String SSL_PROTOCOLS = "any";
  private static final String SSL_CIPHERS = "any";

  public static final String TEST_MULTIOP_KEY1 = "multiopKey1";
  public static final String TEST_MULTIOP_KEY2 = "multiopKey2";
  public static final String TEST_MULTIOP_KEY3 = "multiopKey3";
  public static final String TEST_MULTIOP_VALUE1 = "multiopValue1";
  public static final String TEST_MULTIOP_VALUE2 = "multiopValue2";
  public static final String TEST_MULTIOP_VALUE3 = "multiopValue3";

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
    cacheFactory.set("mcast-port", "0"); // sometimes it isn't due to other tests.
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
    outputStream.write(110);

    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    Set<BasicTypes.Entry> putEntries = new HashSet<>();
    putEntries.add(ProtobufUtilities.createEntry(serializationService, TEST_MULTIOP_KEY1,
        TEST_MULTIOP_VALUE1));
    putEntries.add(ProtobufUtilities.createEntry(serializationService, TEST_MULTIOP_KEY2,
        TEST_MULTIOP_VALUE2));
    putEntries.add(ProtobufUtilities.createEntry(serializationService, TEST_MULTIOP_KEY3,
        TEST_MULTIOP_VALUE3));
    ClientProtocol.Message putAllMessage = ProtobufUtilities.createProtobufRequest(
        ProtobufUtilities.createMessageHeader(TEST_PUT_CORRELATION_ID),
        ProtobufRequestUtilities.createPutAllRequest(TEST_REGION, putEntries));
    protobufProtocolSerializer.serialize(putAllMessage, outputStream);
    validatePutAllResponse(socket, protobufProtocolSerializer,
        ClientProtocol.Response.ResponseAPICase.PUTALLRESPONSE);

    Set<BasicTypes.EncodedValue> getEntries = new HashSet<>();
    getEntries.add(ProtobufUtilities.createEncodedValue(serializationService, TEST_MULTIOP_KEY1));
    getEntries.add(ProtobufUtilities.createEncodedValue(serializationService, TEST_MULTIOP_KEY2));
    getEntries.add(ProtobufUtilities.createEncodedValue(serializationService, TEST_MULTIOP_KEY3));
    ClientProtocol.Message getAllMessage = ProtobufUtilities.createProtobufRequest(
        ProtobufUtilities.createMessageHeader(TEST_GET_CORRELATION_ID),
        ProtobufRequestUtilities.createGetAllRequest(TEST_REGION, getEntries));
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
    outputStream.write(110);

    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    Set<BasicTypes.Entry> putEntries = new HashSet<>();
    putEntries.add(
        ProtobufUtilities.createEntry(serializationService, new Float(2.2), TEST_MULTIOP_VALUE1));
    putEntries.add(ProtobufUtilities.createEntry(serializationService, TEST_MULTIOP_KEY2,
        TEST_MULTIOP_VALUE2));
    putEntries.add(ProtobufUtilities.createEntry(serializationService, TEST_MULTIOP_KEY3,
        TEST_MULTIOP_VALUE3));
    ClientProtocol.Message putAllMessage = ProtobufUtilities.createProtobufRequest(
        ProtobufUtilities.createMessageHeader(TEST_PUT_CORRELATION_ID),
        ProtobufRequestUtilities.createPutAllRequest(regionName, putEntries));
    protobufProtocolSerializer.serialize(putAllMessage, outputStream);
    validatePutAllResponse(socket, protobufProtocolSerializer,
        ClientProtocol.Response.ResponseAPICase.ERRORRESPONSE);

    ClientProtocol.Message getMessage = MessageUtil.makeGetRequestMessage(serializationService,
        new Float(2.2), regionName, ProtobufUtilities.createMessageHeader(TEST_GET_CORRELATION_ID));
    protobufProtocolSerializer.serialize(getMessage, outputStream);
    validateGetResponse(socket, protobufProtocolSerializer, TEST_MULTIOP_VALUE1);

    ClientProtocol.Message removeMessage = ProtobufUtilities.createProtobufRequest(
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

    ClientProtocol.Message message =
        protobufProtocolSerializer.deserialize(socket.getInputStream());
    assertEquals(TEST_GET_CORRELATION_ID, message.getMessageHeader().getCorrelationId());
    assertEquals(ClientProtocol.Message.MessageTypeCase.RESPONSE, message.getMessageTypeCase());
    ClientProtocol.Response response = message.getResponse();
    assertEquals(ClientProtocol.Response.ResponseAPICase.GETRESPONSE,
        response.getResponseAPICase());
    RegionAPI.GetResponse getResponse = response.getGetResponse();

    // All the following ways of checking a null response are valid.
    assertFalse(getResponse.hasResult());
    assertEquals(BasicTypes.EncodingType.INVALID, getResponse.getResult().getEncodingType());
    assertEquals(null,
        ProtobufUtilities.decodeValue(serializationService, getResponse.getResult()));
  }

  @Test
  public void testNewProtocolGetRegionNamesCallSucceeds() throws Exception {
    int correlationId = TEST_GET_CORRELATION_ID; // reuse this value for this test

    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    ClientProtocol.Message getRegionsMessage = ProtobufUtilities.createProtobufRequest(
        ProtobufUtilities.createMessageHeader(correlationId),
        ProtobufRequestUtilities.createGetRegionNamesRequest());
    protobufProtocolSerializer.serialize(getRegionsMessage, outputStream);
    validateGetRegionNamesResponse(socket, correlationId, protobufProtocolSerializer);
  }

  @Test
  public void useSSL_testNewProtocolHeaderLeadsToNewProtocolServerConnection() throws Exception {
    testNewProtocolHeaderLeadsToNewProtocolServerConnection();
  }

  private void validatePutResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer) throws Exception {
    ClientProtocol.Message message =
        protobufProtocolSerializer.deserialize(socket.getInputStream());
    assertEquals(TEST_PUT_CORRELATION_ID, message.getMessageHeader().getCorrelationId());
    assertEquals(ClientProtocol.Message.MessageTypeCase.RESPONSE, message.getMessageTypeCase());
    ClientProtocol.Response response = message.getResponse();
    assertEquals(ClientProtocol.Response.ResponseAPICase.PUTRESPONSE,
        response.getResponseAPICase());
  }

  private void validateGetResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer, Object expectedValue)
      throws InvalidProtocolMessageException, IOException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException, CodecAlreadyRegisteredForTypeException {
    ClientProtocol.Message message =
        protobufProtocolSerializer.deserialize(socket.getInputStream());
    assertEquals(TEST_GET_CORRELATION_ID, message.getMessageHeader().getCorrelationId());
    assertEquals(ClientProtocol.Message.MessageTypeCase.RESPONSE, message.getMessageTypeCase());
    ClientProtocol.Response response = message.getResponse();
    assertEquals(ClientProtocol.Response.ResponseAPICase.GETRESPONSE,
        response.getResponseAPICase());
    RegionAPI.GetResponse getResponse = response.getGetResponse();
    BasicTypes.EncodedValue result = getResponse.getResult();
    assertEquals(BasicTypes.EncodingType.STRING, result.getEncodingType());
    assertEquals(expectedValue, new ProtobufSerializationService().decode(result.getEncodingType(),
        result.getValue().toByteArray()));
  }

  private void validateGetRegionNamesResponse(Socket socket, int correlationId,
      ProtobufProtocolSerializer protobufProtocolSerializer)
      throws InvalidProtocolMessageException, IOException {
    ClientProtocol.Message message =
        protobufProtocolSerializer.deserialize(socket.getInputStream());
    assertEquals(correlationId, message.getMessageHeader().getCorrelationId());
    assertEquals(ClientProtocol.Message.MessageTypeCase.RESPONSE, message.getMessageTypeCase());
    ClientProtocol.Response response = message.getResponse();
    assertEquals(ClientProtocol.Response.ResponseAPICase.GETREGIONNAMESRESPONSE,
        response.getResponseAPICase());
    RegionAPI.GetRegionNamesResponse getRegionsResponse = response.getGetRegionNamesResponse();
    assertEquals(1, getRegionsResponse.getRegionsCount());
    assertEquals(TEST_REGION, getRegionsResponse.getRegions(0));
  }

  private void validatePutAllResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer,
      ClientProtocol.Response.ResponseAPICase responseCase) throws Exception {
    ClientProtocol.Message message =
        protobufProtocolSerializer.deserialize(socket.getInputStream());
    assertEquals(TEST_PUT_CORRELATION_ID, message.getMessageHeader().getCorrelationId());
    assertEquals(ClientProtocol.Message.MessageTypeCase.RESPONSE, message.getMessageTypeCase());
    ClientProtocol.Response response = message.getResponse();
    assertEquals(responseCase, response.getResponseAPICase());
  }

  private void validateGetAllResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer)
      throws InvalidProtocolMessageException, IOException, UnsupportedEncodingTypeException,
      CodecNotRegisteredForTypeException, CodecAlreadyRegisteredForTypeException {
    ClientProtocol.Message message =
        protobufProtocolSerializer.deserialize(socket.getInputStream());
    assertEquals(TEST_GET_CORRELATION_ID, message.getMessageHeader().getCorrelationId());
    assertEquals(ClientProtocol.Message.MessageTypeCase.RESPONSE, message.getMessageTypeCase());
    ClientProtocol.Response response = message.getResponse();
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
    ClientProtocol.Message message =
        protobufProtocolSerializer.deserialize(socket.getInputStream());
    assertEquals(TEST_REMOVE_CORRELATION_ID, message.getMessageHeader().getCorrelationId());
    assertEquals(ClientProtocol.Message.MessageTypeCase.RESPONSE, message.getMessageTypeCase());
    ClientProtocol.Response response = message.getResponse();
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
