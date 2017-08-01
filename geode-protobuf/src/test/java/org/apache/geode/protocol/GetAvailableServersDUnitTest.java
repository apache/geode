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

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.protocol.exception.InvalidProtocolMessageException;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.ServerAPI;
import org.apache.geode.protocol.protobuf.serializer.ProtobufProtocolSerializer;
import org.apache.geode.protocol.protobuf.utilities.ProtobufRequestUtilities;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(DistributedTest.class)
public class GetAvailableServersDUnitTest extends JUnit4CacheTestCase {

  @Rule
  public DistributedRestoreSystemProperties distributedRestoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setup() {

  }

  @Test
  public void testGetAllAvailableServersRequest()
      throws IOException, InvalidProtocolMessageException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    List<Integer> cacheServerPorts = new ArrayList<Integer>();

    int cacheServer1Port = startCacheWithCacheServer();
    cacheServerPorts.add(cacheServer1Port);
    cacheServerPorts.add(vm1.invoke("Start Cache2", () -> startCacheWithCacheServer()));
    cacheServerPorts.add(vm2.invoke("Start Cache3", () -> startCacheWithCacheServer()));

    vm0.invoke(() -> {
      Socket socket = new Socket(host.getHostName(), cacheServer1Port);
      socket.getOutputStream().write(110);

      ClientProtocol.Request.Builder protobufRequestBuilder =
          ProtobufUtilities.createProtobufRequestBuilder();
      ClientProtocol.Message getAvailableServersRequestMessage =
          ProtobufUtilities.createProtobufMessage(ProtobufUtilities.createMessageHeader(1233445),
              protobufRequestBuilder.setGetAvailableServersRequest(
                  ProtobufRequestUtilities.createGetAvailableServersRequest()).build());

      ProtobufProtocolSerializer protobufProtocolSerializer = new ProtobufProtocolSerializer();
      protobufProtocolSerializer.serialize(getAvailableServersRequestMessage,
          socket.getOutputStream());

      ClientProtocol.Message getAvailableServersResponseMessage =
          protobufProtocolSerializer.deserialize(socket.getInputStream());
      assertEquals(1233445,
          getAvailableServersResponseMessage.getMessageHeader().getCorrelationId());
      assertEquals(ClientProtocol.Message.MessageTypeCase.RESPONSE,
          getAvailableServersResponseMessage.getMessageTypeCase());
      ClientProtocol.Response messageResponse = getAvailableServersResponseMessage.getResponse();
      assertEquals(ClientProtocol.Response.ResponseAPICase.GETAVAILABLESERVERSRESPONSE,
          messageResponse.getResponseAPICase());
      ServerAPI.GetAvailableServersResponse getAvailableServersResponse =
          messageResponse.getGetAvailableServersResponse();
      assertEquals(3, getAvailableServersResponse.getServersCount());
      assertTrue(cacheServerPorts.contains(getAvailableServersResponse.getServers(0).getPort()));
      assertTrue(cacheServerPorts.contains(getAvailableServersResponse.getServers(1).getPort()));
      assertTrue(cacheServerPorts.contains(getAvailableServersResponse.getServers(2).getPort()));
    });
  }

  private Integer startCacheWithCacheServer() throws IOException {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    InternalCache cache = getCache();
    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    return cacheServer.getPort();
  }

}
