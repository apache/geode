/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.protocol.client;

import com.google.protobuf.ByteString;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Properties;

import static org.apache.geode.protocol.protobuf.ClientProtocol.Message.MessageTypeCase.RESPONSE;
import static org.apache.geode.protocol.protobuf.ClientProtocol.Response.ResponseAPICase.GETRESPONSE;
import static org.apache.geode.protocol.protobuf.ClientProtocol.Response.ResponseAPICase.PUTRESPONSE;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class ProtobufProtocolIntegrationTest {
  @Test
  public void testRoundTripPutRequest() throws IOException {
    try (Cache cache = createCacheOnPort(40404);
        NewClientProtocolTestClient client = new NewClientProtocolTestClient("localhost", 40404)) {
      final String testRegion = "testRegion";
      final String testKey = "testKey";
      final String testValue = "testValue";
      Region<Object, Object> region = cache.createRegionFactory().create("testRegion");

      ClientProtocol.Message message =
          MessageUtils.makePutMessageFor(testRegion, testKey, testValue);
      ClientProtocol.Message response = client.blockingSendMessage(message);
      client.printResponse(response);

      assertEquals(RESPONSE, response.getMessageTypeCase());
      assertEquals(PUTRESPONSE,
      response.getResponse().getResponseAPICase());
      assertTrue(response.getResponse().getPutResponse().getSuccess());

      assertEquals(1, region.size());
      assertTrue(region.containsKey(testKey));
      assertEquals(testValue, region.get(testKey));
    }
  }

  @Test
  public void testRoundTripEmptyGetRequest() throws IOException {
    try (Cache cache = createCacheOnPort(40404);
         NewClientProtocolTestClient client = new NewClientProtocolTestClient("localhost", 40404)) {
      final String testRegion = "testRegion";
      final String testKey = "testKey";
      Region<Object, Object> region = cache.createRegionFactory().create("testRegion");

      ClientProtocol.Message message = MessageUtils.makeGetMessageFor(testRegion, testKey);
      ClientProtocol.Message response = client.blockingSendMessage(message);

      assertEquals(RESPONSE, response.getMessageTypeCase());
      assertEquals(GETRESPONSE,
        response.getResponse().getResponseAPICase());
      BasicTypes.Value value = response.getResponse().getGetResponse().getResult();

      assertTrue(value.getValue().isEmpty());
    }
  }

  @Test
  public void testRoundTripNonEmptyGetRequest() throws IOException {
    try (Cache cache = createCacheOnPort(40404);
         NewClientProtocolTestClient client = new NewClientProtocolTestClient("localhost", 40404)) {
      final String testRegion = "testRegion";
      final String testKey = "testKey";
      final String testValue = "testValue";
      Region<Object, Object> region = cache.createRegionFactory().create("testRegion");


      ClientProtocol.Message putMessage =
        MessageUtils.makePutMessageFor(testRegion, testKey, testValue);
      ClientProtocol.Message putResponse = client.blockingSendMessage(putMessage);
      client.printResponse(putResponse);

      ClientProtocol.Message getMessage = MessageUtils.makeGetMessageFor(testRegion, testKey);
      ClientProtocol.Message getResponse = client.blockingSendMessage(getMessage);

      assertEquals(RESPONSE, getResponse.getMessageTypeCase());
      assertEquals(GETRESPONSE,
        getResponse.getResponse().getResponseAPICase());
      BasicTypes.Value value = getResponse.getResponse().getGetResponse().getResult();

      assertEquals(value.getValue().toStringUtf8(), testValue);
    }
  }

  @Test
  public void startCache() throws IOException {
    try (Cache cache = createCacheOnPort(40404)) {
      while (true) {
        try {
          Thread.sleep(100000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private Cache createCacheOnPort(int port) throws IOException {
    Properties props = new Properties();
    props.setProperty(ConfigurationProperties.TCP_PORT, Integer.toString(port));
    props.setProperty(ConfigurationProperties.BIND_ADDRESS, "localhost");
    CacheFactory cf = new CacheFactory(props);
    Cache cache = cf.create();
    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setBindAddress("localhost");
    cacheServer.start();
    return cache;
  }

}
