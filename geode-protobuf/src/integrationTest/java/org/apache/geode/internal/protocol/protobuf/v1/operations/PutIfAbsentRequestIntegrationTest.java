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
package org.apache.geode.internal.protocol.protobuf.v1.operations;

import static org.apache.geode.internal.protocol.protobuf.v1.MessageUtil.validateGetResponse;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Properties;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.MessageUtil;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufRequestUtilities;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.serializer.ProtobufProtocolSerializer;
import org.apache.geode.internal.protocol.protobuf.v1.serializer.exception.InvalidProtocolMessageException;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class PutIfAbsentRequestIntegrationTest {
  private static final String TEST_REGION = "testRegion";
  private static final Object TEST_KEY = "testKey";
  private Cache cache;
  private Socket socket;
  private OutputStream outputStream;
  private ProtobufSerializationService serializationService;
  private ProtobufProtocolSerializer protobufProtocolSerializer;

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  private InputStream inputStream;

  private void doSetup(DataPolicy dataPolicy) throws IOException {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    CacheFactory cacheFactory = new CacheFactory(new Properties());
    cacheFactory.set(ConfigurationProperties.MCAST_PORT, "0");
    cacheFactory.set(ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION, "false");
    cacheFactory.set(ConfigurationProperties.USE_CLUSTER_CONFIGURATION, "false");
    cache = cacheFactory.create();

    CacheServer cacheServer = cache.addCacheServer();
    final int cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer.setPort(cacheServerPort);
    cacheServer.start();

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    regionFactory.setDataPolicy(dataPolicy);
    regionFactory.create(TEST_REGION);


    socket = new Socket("localhost", cacheServerPort);
    await().until(socket::isConnected);
    outputStream = socket.getOutputStream();
    inputStream = socket.getInputStream();

    MessageUtil.performAndVerifyHandshake(socket);

    serializationService = new ProtobufSerializationService();
    protobufProtocolSerializer = new ProtobufProtocolSerializer();
  }

  @After
  public void cleanUp() throws IOException {
    cache.close();
    socket.close();
    SocketCreatorFactory.close();
  }

  @Test
  public void testPutIfAbsentRequest() throws Exception {
    doSetup(DataPolicy.REPLICATE);

    final BasicTypes.EncodedValue encodedKey = serializationService.encode(TEST_KEY);
    final String testValue = "testValue";
    final String testValue2 = "testValue2";
    final BasicTypes.Entry entry1 = BasicTypes.Entry.newBuilder().setKey(encodedKey)
        .setValue(serializationService.encode(testValue)).build();
    assertNull(serializationService.decode(doPutIfAbsent(entry1).getOldValue()));

    protobufProtocolSerializer.serialize(
        ProtobufRequestUtilities.createGetRequest(TEST_REGION, encodedKey),
        socket.getOutputStream());
    validateGetResponse(socket, protobufProtocolSerializer, testValue);

    final BasicTypes.Entry entry2 = BasicTypes.Entry.newBuilder().setKey(encodedKey)
        .setValue(serializationService.encode(testValue2)).build();

    // same value still present
    assertEquals(testValue, serializationService.decode(doPutIfAbsent(entry2).getOldValue()));
    protobufProtocolSerializer.serialize(
        ProtobufRequestUtilities.createGetRequest(TEST_REGION, encodedKey),
        socket.getOutputStream());
    validateGetResponse(socket, protobufProtocolSerializer, testValue);
  }

  /**
   * This should fail because DataPolicy.NORMAL doesn't allow concurrent cache ops.
   */
  @Test
  public void testPutIfAbsentRequestOnDataPolicyNormal() throws Exception {
    doSetup(DataPolicy.NORMAL);

    final BasicTypes.EncodedValue encodedKey = serializationService.encode(TEST_KEY);
    final String testValue = "testValue";
    final BasicTypes.EncodedValue encodedValue = serializationService.encode(testValue);
    final BasicTypes.Entry entry =
        BasicTypes.Entry.newBuilder().setKey(encodedKey).setValue(encodedValue).build();
    ProtobufRequestUtilities.createPutIfAbsentRequest(TEST_REGION, entry)
        .writeDelimitedTo(outputStream);

    final ClientProtocol.Message response = ClientProtocol.Message.parseDelimitedFrom(inputStream);

    assertEquals(ClientProtocol.Message.MessageTypeCase.ERRORRESPONSE,
        response.getMessageTypeCase());
    assertEquals(BasicTypes.ErrorCode.UNSUPPORTED_OPERATION,
        response.getErrorResponse().getError().getErrorCode());
  }

  private RegionAPI.PutIfAbsentResponse doPutIfAbsent(BasicTypes.Entry entry)
      throws IOException, InvalidProtocolMessageException {
    final ClientProtocol.Message putIfAbsentRequest =
        ProtobufRequestUtilities.createPutIfAbsentRequest(TEST_REGION, entry);

    protobufProtocolSerializer.serialize(putIfAbsentRequest, outputStream);
    ClientProtocol.Message response = protobufProtocolSerializer.deserialize(inputStream);

    assertEquals(ClientProtocol.Message.MessageTypeCase.PUTIFABSENTRESPONSE,
        response.getMessageTypeCase());
    return response.getPutIfAbsentResponse();
  }


}
