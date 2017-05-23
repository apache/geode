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

import static org.apache.geode.protocol.protobuf.ClientProtocol.Message.MessageTypeCase.RESPONSE;
import static org.apache.geode.protocol.protobuf.ClientProtocol.Response.ResponseAPICase.GETRESPONSE;
import static org.apache.geode.protocol.protobuf.ClientProtocol.Response.ResponseAPICase.PUTRESPONSE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Properties;

@Category(IntegrationTest.class)
@RunWith(JUnitQuickcheck.class)
public class ProtobufProtocolIntegrationTest {
  private final static String testRegion = "testRegion";
  private final static String testKey = "testKey";
  private final static String testValue = "testValue";
  private Cache cache;
  private NewClientProtocolTestClient testClient;
  private Region<Object, Object> regionUnderTest;

  @Before
  public void setup() throws IOException {
    cache = createCacheOnPort(40404);
    testClient = new NewClientProtocolTestClient("localhost", 40404);
    regionUnderTest = cache.createRegionFactory().create(testRegion);
  }

  @After
  public void shutdown() throws IOException {
    if (testClient != null) {
      testClient.close();
    }
    if (cache != null) {
      cache.close();
    }
  }

  @Test
  public void testRoundTripPutRequest() throws IOException {
      ClientProtocol.Message message =
          MessageUtils.INSTANCE.makePutMessageFor(testRegion, testKey, testValue);
      ClientProtocol.Message response = testClient.blockingSendMessage(message);
      testClient.printResponse(response);

      assertEquals(RESPONSE, response.getMessageTypeCase());
      assertEquals(PUTRESPONSE, response.getResponse().getResponseAPICase());
      assertTrue(response.getResponse().getPutResponse().getSuccess());

      assertEquals(1, regionUnderTest.size());
      assertTrue(regionUnderTest.containsKey(testKey));
      assertEquals(testValue, regionUnderTest.get(testKey));
  }

  @Test
  public void testRoundTripEmptyGetRequest() throws IOException {
      ClientProtocol.Message message = MessageUtils.INSTANCE.makeGetMessageFor(testRegion, testKey);
      ClientProtocol.Message response = testClient.blockingSendMessage(message);

      assertEquals(RESPONSE, response.getMessageTypeCase());
      assertEquals(GETRESPONSE, response.getResponse().getResponseAPICase());
      BasicTypes.EncodedValue value = response.getResponse().getGetResponse().getResult();

      assertTrue(value.getValue().isEmpty());
  }

  @Test
  public void testRoundTripNonEmptyGetRequest() throws IOException {
      ClientProtocol.Message putMessage =
          MessageUtils.INSTANCE.makePutMessageFor(testRegion, testKey, testValue);
      ClientProtocol.Message putResponse = testClient.blockingSendMessage(putMessage);
      testClient.printResponse(putResponse);

      ClientProtocol.Message getMessage = MessageUtils.INSTANCE
          .makeGetMessageFor(testRegion, testKey);
      ClientProtocol.Message getResponse = testClient.blockingSendMessage(getMessage);

      assertEquals(RESPONSE, getResponse.getMessageTypeCase());
      assertEquals(GETRESPONSE, getResponse.getResponse().getResponseAPICase());
      BasicTypes.EncodedValue value = getResponse.getResponse().getGetResponse().getResult();

      assertEquals(value.getValue().toStringUtf8(), testValue);
    }

    @Test
    public void objectSerializationIntegrationTest() {
      Object[] inputs = new Object[]{
        "Foobar", 1000L, 22, (short) 231, (byte) -107, new byte[]{1,2,3,54,99}
      };
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
