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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.protocol.protobuf.v1.serializer.ProtobufProtocolSerializer;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class LocatorIntegrationTest {
  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private OutputStream outputStream;
  private InputStream inputStream;
  private ProtobufProtocolSerializer protobufProtocolSerializer;
  private Socket socket;
  private SocketChannel socketChannel;
  private LocatorLauncher locatorLauncher;

  @Before
  public void setUp() throws Exception {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    int locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();

    LocatorLauncher.Builder builder = new LocatorLauncher.Builder();
    builder.setPort(locatorPort);
    locatorLauncher = builder.build();
    locatorLauncher.start();

    InetSocketAddress localhost = new InetSocketAddress("localhost", locatorPort);
    socketChannel = SocketChannel.open(localhost);

    socket = socketChannel.socket();

    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
    outputStream = socket.getOutputStream();
    inputStream = socket.getInputStream();

    protobufProtocolSerializer = new ProtobufProtocolSerializer();
  }

  @After
  public void tearDown() throws IOException {
    if (socket.isConnected()) {
      socket.close();
    }
    locatorLauncher.stop();
  }

  @Test
  public void testLocatorMessageSucceedsAndDisconnects() throws Exception {
    outputStream.write(CommunicationMode.ProtobufClientServerProtocol.getModeNumber());
    outputStream.write(ConnectionAPI.MajorVersions.CURRENT_MAJOR_VERSION_VALUE);

    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder()
            .setGetAvailableServersRequest(LocatorAPI.GetAvailableServersRequest.newBuilder()))
        .build().writeDelimitedTo(outputStream);
    ClientProtocol.Message response = protobufProtocolSerializer.deserialize(inputStream);
    assertEquals(ClientProtocol.Response.GETAVAILABLESERVERSRESPONSE_FIELD_NUMBER,
        response.getResponse().getResponseAPICase().getNumber());

    // Set timeout to ensure test doesn't block forever if socket isn't closed
    socket.setSoTimeout(5000);
    assertEquals(-1, inputStream.read());
  }
}
