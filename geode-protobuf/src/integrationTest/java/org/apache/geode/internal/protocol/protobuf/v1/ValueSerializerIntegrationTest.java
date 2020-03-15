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
package org.apache.geode.internal.protocol.protobuf.v1;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ValueSerializerIntegrationTest {

  private static final String TEST_REGION = "region";
  private Cache cache;
  private Socket socket;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  private Region<String, Object> region;

  @Before
  public void setUp() throws Exception {
    CacheFactory cacheFactory = new CacheFactory(new Properties());
    cacheFactory.set(ConfigurationProperties.MCAST_PORT, "0");
    cacheFactory.set(ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION, "false");
    cacheFactory.set(ConfigurationProperties.USE_CLUSTER_CONFIGURATION, "false");

    cache = cacheFactory.create();

    CacheServer cacheServer = cache.addCacheServer();
    int cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer.setPort(cacheServerPort);
    cacheServer.start();

    RegionFactory<String, Object> regionFactory = cache.createRegionFactory();
    regionFactory.setDataPolicy(DataPolicy.PARTITION);
    region = regionFactory.create(TEST_REGION);


    System.setProperty("geode.feature-protobuf-protocol", "true");

    socket = new Socket("localhost", cacheServerPort);

    await().until(socket::isConnected);

    MessageUtil.performAndVerifyHandshake(socket);

  }

  @After
  public void tearDown() {
    cache.close();
    try {
      socket.close();
    } catch (IOException ignore) {
    }
  }


  @Test
  public void failWithInvalidValueFormat() throws IOException {
    ClientProtocol.Message.newBuilder()
        .setHandshakeRequest(
            ConnectionAPI.HandshakeRequest.newBuilder().setValueFormat("BAD_FORMAT"))
        .build().writeDelimitedTo(socket.getOutputStream());

    ClientProtocol.Message response =
        ClientProtocol.Message.parseDelimitedFrom(socket.getInputStream());

    assertEquals(BasicTypes.ErrorCode.INVALID_REQUEST,
        response.getErrorResponse().getError().getErrorCode());
  }


  @Test
  public void canGetCustomValueWithSerializerWithoutPrimitiveSupport()
      throws IOException, ClassNotFoundException {
    testGetWithValueSerializer(new TestValueSerializer().getID());
  }


  @Test
  public void canGetCustomValueWithSerializerWithPrimitiveSupport()
      throws IOException, ClassNotFoundException {
    testGetWithValueSerializer(new TestSerializeAllSerializer().getID());
  }

  public void testGetWithValueSerializer(String valueFormat)
      throws IOException, ClassNotFoundException {
    sendHandshake(valueFormat);

    DataSerializableObject value = new DataSerializableObject("field");
    region.put("key", value);

    ClientProtocol.Message response = get("key");

    assertEquals(value, new TestValueSerializer()
        .deserialize(response.getGetResponse().getResult().getCustomObjectResult()));
  }

  @Test
  public void canSerializePrimitivesWithCustomSerializer()
      throws IOException, ClassNotFoundException {
    sendHandshake(new TestSerializeAllSerializer().getID());

    region.put("key", "value");

    ClientProtocol.Message response = get("key");

    assertEquals("value", new TestValueSerializer()
        .deserialize(response.getGetResponse().getResult().getCustomObjectResult()));
  }

  @Test
  public void serializerWithoutPrimitiveSupportIsNotInvokedForPrimitives()
      throws IOException {
    sendHandshake(new TestValueSerializer().getID());
    region.put("key", "value");

    ClientProtocol.Message response = get("key");

    assertEquals("value", response.getGetResponse().getResult().getStringResult());
  }


  private ClientProtocol.Message get(String key) throws IOException {
    ClientProtocol.Message response;
    ClientProtocol.Message.newBuilder()
        .setGetRequest(RegionAPI.GetRequest.newBuilder().setRegionName(TEST_REGION)
            .setKey(BasicTypes.EncodedValue.newBuilder().setStringResult(key)).build())
        .build().writeDelimitedTo(socket.getOutputStream());

    response = ClientProtocol.Message.parseDelimitedFrom(socket.getInputStream());
    return response;
  }

  private void sendHandshake(String valueFormat) throws IOException {
    ClientProtocol.Message.newBuilder()
        .setHandshakeRequest(
            ConnectionAPI.HandshakeRequest.newBuilder().setValueFormat(valueFormat))
        .build().writeDelimitedTo(socket.getOutputStream());

    ClientProtocol.Message response =
        ClientProtocol.Message.parseDelimitedFrom(socket.getInputStream());

    assertTrue("Got response: " + response.getHandshakeResponse(), response.hasHandshakeResponse());
  }

  public static class DataSerializableObject implements DataSerializable {

    public String field;

    @SuppressWarnings("unused")
    public DataSerializableObject() {

    }

    public DataSerializableObject(String field) {
      this.field = field;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeUTF(field);

    }

    @Override
    public void fromData(DataInput in) throws IOException {
      field = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      DataSerializableObject that = (DataSerializableObject) o;

      return field != null ? field.equals(that.field) : that.field == null;
    }

    @Override
    public int hashCode() {
      return field != null ? field.hashCode() : 0;
    }
  }
}
