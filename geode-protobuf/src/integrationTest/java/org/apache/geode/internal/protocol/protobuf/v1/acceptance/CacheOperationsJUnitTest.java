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

import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.internal.protocol.protobuf.v1.MessageUtil.validateGetResponse;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.MessageUtil;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufRequestUtilities;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serializer.ProtobufProtocolSerializer;
import org.apache.geode.internal.protocol.protobuf.v1.serializer.exception.InvalidProtocolMessageException;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufUtilities;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.util.test.TestUtil;

/**
 * Test operations using ProtoBuf
 */
@Category(ClientServerTest.class)
public class CacheOperationsJUnitTest {
  private final String TEST_KEY = "testKey";
  private final String TEST_VALUE = "testValue";
  private final String TEST_REGION = "testRegion";

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
  private ProtobufSerializationService serializationService;
  private Socket socket;
  private OutputStream outputStream;

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TestName testName = new TestName();
  private ProtobufProtocolSerializer protobufProtocolSerializer;

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
    await().until(socket::isConnected);
    outputStream = socket.getOutputStream();

    MessageUtil.performAndVerifyHandshake(socket);

    serializationService = new ProtobufSerializationService();
    protobufProtocolSerializer = new ProtobufProtocolSerializer();
  }

  @After
  public void cleanup() throws IOException {
    cache.close();
    socket.close();
    SocketCreatorFactory.close();
  }

  @Test
  public void testNewProtocolWithMultikeyOperations() throws Exception {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    Set<BasicTypes.Entry> putEntries = new HashSet<>();
    putEntries.add(ProtobufUtilities.createEntry(serializationService, TEST_MULTIOP_KEY1,
        TEST_MULTIOP_VALUE1));
    putEntries.add(ProtobufUtilities.createEntry(serializationService, TEST_MULTIOP_KEY2,
        TEST_MULTIOP_VALUE2));
    putEntries.add(ProtobufUtilities.createEntry(serializationService, TEST_MULTIOP_KEY3,
        TEST_MULTIOP_VALUE3));
    ClientProtocol.Message putAllMessage =
        ProtobufRequestUtilities.createPutAllRequest(TEST_REGION, putEntries);
    protobufProtocolSerializer.serialize(putAllMessage, outputStream);
    validatePutAllResponse(socket, protobufProtocolSerializer, new HashSet<>());

    Set<BasicTypes.EncodedValue> getEntries = new HashSet<>();
    getEntries.add(serializationService.encode(TEST_MULTIOP_KEY1));
    getEntries.add(serializationService.encode(TEST_MULTIOP_KEY2));
    getEntries.add(serializationService.encode(TEST_MULTIOP_KEY3));

    RegionAPI.GetAllRequest getAllRequest =
        ProtobufRequestUtilities.createGetAllRequest(TEST_REGION, getEntries);

    ClientProtocol.Message getAllMessage =
        ClientProtocol.Message.newBuilder().setGetAllRequest(getAllRequest).build();
    protobufProtocolSerializer.serialize(getAllMessage, outputStream);
    validateGetAllResponse(socket, protobufProtocolSerializer);

    RegionAPI.KeySetRequest keySetRequest =
        RegionAPI.KeySetRequest.newBuilder().setRegionName(TEST_REGION).build();
    ClientProtocol.Message keySetMessage =
        ClientProtocol.Message.newBuilder().setKeySetRequest(keySetRequest).build();
    protobufProtocolSerializer.serialize(keySetMessage, outputStream);
    validateKeySetResponse(socket, protobufProtocolSerializer);
  }

  @Test
  public void multiKeyOperationErrorsWithClasscastException() throws Exception {
    RegionFactory<Float, Object> regionFactory = cache.createRegionFactory();
    regionFactory.setKeyConstraint(Float.class);
    String regionName = "constraintRegion";
    regionFactory.create(regionName);
    System.setProperty("geode.feature-protobuf-protocol", "true");

    Set<BasicTypes.Entry> putEntries = new HashSet<>();
    putEntries.add(ProtobufUtilities.createEntry(serializationService, 2.2f, TEST_MULTIOP_VALUE1));
    putEntries.add(ProtobufUtilities.createEntry(serializationService, TEST_MULTIOP_KEY2,
        TEST_MULTIOP_VALUE2));
    putEntries.add(ProtobufUtilities.createEntry(serializationService, TEST_MULTIOP_KEY3,
        TEST_MULTIOP_VALUE3));
    ClientProtocol.Message putAllMessage =
        ProtobufRequestUtilities.createPutAllRequest(regionName, putEntries);

    protobufProtocolSerializer.serialize(putAllMessage, outputStream);
    HashSet<BasicTypes.EncodedValue> expectedFailedKeys = new HashSet<BasicTypes.EncodedValue>();
    expectedFailedKeys.add(serializationService.encode(TEST_MULTIOP_KEY2));
    expectedFailedKeys.add(serializationService.encode(TEST_MULTIOP_KEY3));
    validatePutAllResponse(socket, protobufProtocolSerializer, expectedFailedKeys);

    ClientProtocol.Message getMessage =
        MessageUtil.makeGetRequestMessage(serializationService, 2.2f, regionName);
    protobufProtocolSerializer.serialize(getMessage, outputStream);
    validateGetResponse(socket, protobufProtocolSerializer, TEST_MULTIOP_VALUE1);

    ClientProtocol.Message removeMessage = ProtobufRequestUtilities.createRemoveRequest(TEST_REGION,
        serializationService.encode(TEST_KEY));
    protobufProtocolSerializer.serialize(removeMessage, outputStream);
    validateRemoveResponse(socket, protobufProtocolSerializer);
  }

  @Test
  public void testResponseToGetWithNoData() throws Exception {
    // Get request without any data set must return a null
    ClientProtocol.Message getMessage =
        MessageUtil.makeGetRequestMessage(serializationService, TEST_KEY, TEST_REGION);
    protobufProtocolSerializer.serialize(getMessage, outputStream);

    ClientProtocol.Message response = deserializeResponse(socket, protobufProtocolSerializer);
    assertEquals(ClientProtocol.Message.MessageTypeCase.GETRESPONSE, response.getMessageTypeCase());
    RegionAPI.GetResponse getResponse = response.getGetResponse();

    assertEquals(null, serializationService.decode(getResponse.getResult()));
  }

  @Test
  public void testNewProtocolGetRegionNamesCallSucceeds() throws Exception {
    RegionAPI.GetRegionNamesRequest getRegionNamesRequest =
        ProtobufRequestUtilities.createGetRegionNamesRequest();

    ClientProtocol.Message getRegionsMessage =
        ClientProtocol.Message.newBuilder().setGetRegionNamesRequest(getRegionNamesRequest).build();
    protobufProtocolSerializer.serialize(getRegionsMessage, outputStream);
    validateGetRegionNamesResponse(socket, protobufProtocolSerializer);
  }

  @Test
  public void testNewProtocolGetSizeCall() throws Exception {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    ClientProtocol.Message putMessage = ProtobufRequestUtilities.createPutRequest(TEST_REGION,
        ProtobufUtilities.createEntry(serializationService, TEST_KEY, TEST_VALUE));
    protobufProtocolSerializer.serialize(putMessage, outputStream);
    ClientProtocol.Message response = deserializeResponse(socket, protobufProtocolSerializer);
    assertEquals(ClientProtocol.Message.MessageTypeCase.PUTRESPONSE, response.getMessageTypeCase());

    ClientProtocol.Message getRegionMessage = MessageUtil.makeGetSizeRequestMessage(TEST_REGION);
    protobufProtocolSerializer.serialize(getRegionMessage, outputStream);
    ClientProtocol.Message message =
        protobufProtocolSerializer.deserialize(socket.getInputStream());
    assertEquals(ClientProtocol.Message.MessageTypeCase.GETSIZERESPONSE,
        message.getMessageTypeCase());
    RegionAPI.GetSizeResponse getSizeResponse = message.getGetSizeResponse();
    assertEquals(1, getSizeResponse.getSize());
  }


  private ClientProtocol.Message deserializeResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer)
      throws InvalidProtocolMessageException, IOException {
    ClientProtocol.Message message =
        protobufProtocolSerializer.deserialize(socket.getInputStream());
    return message;
  }

  private void validateGetRegionNamesResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer)
      throws InvalidProtocolMessageException, IOException {
    ClientProtocol.Message response = deserializeResponse(socket, protobufProtocolSerializer);

    assertEquals(ClientProtocol.Message.MessageTypeCase.GETREGIONNAMESRESPONSE,
        response.getMessageTypeCase());
    RegionAPI.GetRegionNamesResponse getRegionsResponse = response.getGetRegionNamesResponse();
    assertEquals(1, getRegionsResponse.getRegionsCount());
    assertEquals("/" + TEST_REGION, getRegionsResponse.getRegions(0));
  }

  private void validatePutAllResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer,
      Collection<BasicTypes.EncodedValue> expectedFailedKeys) throws Exception {
    ClientProtocol.Message response = deserializeResponse(socket, protobufProtocolSerializer);

    assertEquals(ClientProtocol.Message.MessageTypeCase.PUTALLRESPONSE,
        response.getMessageTypeCase());
    assertEquals(expectedFailedKeys.size(), response.getPutAllResponse().getFailedKeysCount());

    Stream<BasicTypes.EncodedValue> failedKeyStream = response.getPutAllResponse()
        .getFailedKeysList().stream().map(BasicTypes.KeyedError::getKey);
    assertTrue(failedKeyStream.allMatch(expectedFailedKeys::contains));

  }

  private void validateGetAllResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer)
      throws InvalidProtocolMessageException, IOException, EncodingException {
    ClientProtocol.Message response = deserializeResponse(socket, protobufProtocolSerializer);
    assertEquals(ClientProtocol.Message.MessageTypeCase.GETALLRESPONSE,
        response.getMessageTypeCase());
    RegionAPI.GetAllResponse getAllResponse = response.getGetAllResponse();
    assertEquals(3, getAllResponse.getEntriesCount());
    for (BasicTypes.Entry result : getAllResponse.getEntriesList()) {
      String key = null;
      try {
        key = (String) serializationService.decode(result.getKey());
      } catch (org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException e) {
        e.printStackTrace();
      }
      String value = null;
      try {
        value = (String) serializationService.decode(result.getValue());
      } catch (org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException e) {
        e.printStackTrace();
      }
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

  private void validateKeySetResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer) throws Exception {
    ClientProtocol.Message response = deserializeResponse(socket, protobufProtocolSerializer);

    assertEquals(ClientProtocol.Message.MessageTypeCase.KEYSETRESPONSE,
        response.getMessageTypeCase());
    RegionAPI.KeySetResponse keySetResponse = response.getKeySetResponse();
    assertEquals(3, keySetResponse.getKeysCount());
    List responseKeys = keySetResponse.getKeysList().stream().map(serializationService::decode)
        .collect(Collectors.toList());
    assertTrue(responseKeys.contains(TEST_MULTIOP_KEY1));
    assertTrue(responseKeys.contains(TEST_MULTIOP_KEY2));
    assertTrue(responseKeys.contains(TEST_MULTIOP_KEY3));
  }

  private void validateRemoveResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer) throws Exception {
    ClientProtocol.Message response = deserializeResponse(socket, protobufProtocolSerializer);
    assertEquals(ClientProtocol.Message.MessageTypeCase.REMOVERESPONSE,
        response.getMessageTypeCase());
  }

  private void updatePropertiesForSSLCache(Properties properties) {
    String keyStore = TestUtil.getResourcePath(CacheOperationsJUnitTest.class, DEFAULT_STORE);
    String trustStore = TestUtil.getResourcePath(CacheOperationsJUnitTest.class, DEFAULT_STORE);

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
    String keyStorePath = TestUtil.getResourcePath(CacheOperationsJUnitTest.class, DEFAULT_STORE);
    String trustStorePath = TestUtil.getResourcePath(CacheOperationsJUnitTest.class, DEFAULT_STORE);

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
