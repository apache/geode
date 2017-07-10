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
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.tier.sockets.GenericProtocolServerConnection;
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
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

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

  private Cache cache;
  private int cacheServerPort;
  private SerializationService serializationService;

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setup() throws Exception {
    CacheFactory cacheFactory = new CacheFactory();
    cacheFactory.set("mcast-port", "0"); // sometimes it isn't due to other tests.
    cache = cacheFactory.create();

    CacheServer cacheServer = cache.addCacheServer();
    cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer.setPort(cacheServerPort);
    cacheServer.start();

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    Region<Object, Object> testRegion = regionFactory.create(TEST_REGION);
    serializationService = new ProtobufSerializationService();
  }

  @After
  public void cleanup() {
    cache.close();
  }

  @Test
  public void testNewProtocolHeaderLeadsToNewProtocolServerConnection() throws Exception {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    Socket socket = new Socket("localhost", cacheServerPort);
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
    OutputStream outputStream = socket.getOutputStream();
    outputStream.write(110);

    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    ClientProtocol.Message putMessage =
        MessageUtil.makePutRequestMessage(serializationService, TEST_KEY, TEST_VALUE, TEST_REGION,
            ProtobufUtilities.createMessageHeader(TEST_PUT_CORRELATION_ID));
    protobufProtocolSerializer.serialize(putMessage, outputStream);
    validatePutResponse(socket, protobufProtocolSerializer);

    ClientProtocol.Message getMessage = MessageUtil.makeGetRequestMessage(serializationService,
        TEST_KEY, TEST_REGION, ProtobufUtilities.createMessageHeader(TEST_GET_CORRELATION_ID));
    protobufProtocolSerializer.serialize(getMessage, outputStream);
    validateGetResponse(socket, protobufProtocolSerializer);
  }

  @Test
  public void testNullResponse() throws Exception {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    Socket socket = new Socket("localhost", cacheServerPort);
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
    OutputStream outputStream = socket.getOutputStream();
    outputStream.write(110);

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
    System.setProperty("geode.feature-protobuf-protocol", "true");

    Socket socket = new Socket("localhost", cacheServerPort);
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
    OutputStream outputStream = socket.getOutputStream();
    outputStream.write(110);
    int correlationId = TEST_GET_CORRELATION_ID; // reuse this value for this test

    ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
    ClientProtocol.Message getRegionsMessage = ProtobufUtilities.createProtobufRequest(
        ProtobufUtilities.createMessageHeader(correlationId),
        ProtobufRequestUtilities.createGetRegionNamesRequest());
    protobufProtocolSerializer.serialize(getRegionsMessage, outputStream);
    validateGetRegionNamesResponse(socket, correlationId, protobufProtocolSerializer);
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
    RegionAPI.PutResponse putResponse = response.getPutResponse();
  }

  private void validateGetResponse(Socket socket,
      ProtobufProtocolSerializer protobufProtocolSerializer)
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
    assertEquals(TEST_VALUE, new ProtobufSerializationService().decode(result.getEncodingType(),
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
}
