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
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.protocol.protobuf.AuthenticationAPI;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.ProtobufSerializationService;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.protocol.protobuf.serializer.ProtobufProtocolSerializer;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.awaitility.Awaitility;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(IntegrationTest.class)
public class AuthenticationIntegrationTest {

  private static final String TEST_USERNAME = "bob";
  private static final String TEST_PASSWORD = "bobspassword";

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private Cache cache;
  private int cacheServerPort;
  private CacheServer cacheServer;
  private Socket socket;
  private OutputStream outputStream;
  private ProtobufSerializationService serializationService;
  private InputStream inputStream;
  private ProtobufProtocolSerializer protobufProtocolSerializer;
  private Object securityPrincipal;
  private SecurityManager mockSecurityManager;

  public void setUp(String authenticationMode)
      throws IOException, CodecAlreadyRegisteredForTypeException {
    Properties expectedAuthProperties = new Properties();
    expectedAuthProperties.setProperty("username", TEST_USERNAME);
    expectedAuthProperties.setProperty("password", TEST_PASSWORD);

    securityPrincipal = new Object();
    mockSecurityManager = mock(SecurityManager.class);
    when(mockSecurityManager.authenticate(expectedAuthProperties)).thenReturn(securityPrincipal);
    when(mockSecurityManager.authorize(same(securityPrincipal), any())).thenReturn(true);

    Properties properties = new Properties();
    properties.setProperty(DistributionConfig.PROTOBUF_PROTOCOL_AUTHENTICATION_MODE_NAME,
        authenticationMode);
    CacheFactory cacheFactory = new CacheFactory(properties);
    cacheFactory.set("mcast-port", "0"); // sometimes it isn't due to other tests.

    cacheFactory.setSecurityManager(mockSecurityManager);
    cache = cacheFactory.create();

    cacheServer = cache.addCacheServer();
    cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer.setPort(cacheServerPort);
    cacheServer.start();


    System.setProperty("geode.feature-protobuf-protocol", "true");
    socket = new Socket("localhost", cacheServerPort);

    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
    outputStream = socket.getOutputStream();
    inputStream = socket.getInputStream();
    outputStream.write(110);

    serializationService = new ProtobufSerializationService();
    protobufProtocolSerializer = new ProtobufProtocolSerializer();
  }

  @Test
  public void noopAuthenticationSucceeds() throws Exception {
    setUp("NOOP");
    ClientProtocol.Message getRegionsMessage =
        ClientProtocol.Message.newBuilder().setRequest(ClientProtocol.Request.newBuilder()
            .setGetRegionNamesRequest(RegionAPI.GetRegionNamesRequest.newBuilder())).build();
    protobufProtocolSerializer.serialize(getRegionsMessage, outputStream);

    ClientProtocol.Message regionsResponse = protobufProtocolSerializer.deserialize(inputStream);
    assertEquals(ClientProtocol.Response.ResponseAPICase.GETREGIONNAMESRESPONSE,
        regionsResponse.getResponse().getResponseAPICase());
  }

  @Test
  public void simpleAuthenticationSucceeds() throws Exception {
    setUp("SIMPLE");
    AuthenticationAPI.SimpleAuthenticationRequest authenticationRequest =
        AuthenticationAPI.SimpleAuthenticationRequest.newBuilder().setUsername(TEST_USERNAME)
            .setPassword(TEST_PASSWORD).build();
    authenticationRequest.writeDelimitedTo(outputStream);

    AuthenticationAPI.SimpleAuthenticationResponse authenticationResponse =
        AuthenticationAPI.SimpleAuthenticationResponse.parseDelimitedFrom(inputStream);
    assertTrue(authenticationResponse.getAuthenticated());

    ClientProtocol.Message getRegionsMessage =
        ClientProtocol.Message.newBuilder().setRequest(ClientProtocol.Request.newBuilder()
            .setGetRegionNamesRequest(RegionAPI.GetRegionNamesRequest.newBuilder())).build();
    protobufProtocolSerializer.serialize(getRegionsMessage, outputStream);

    ClientProtocol.Message regionsResponse = protobufProtocolSerializer.deserialize(inputStream);
    assertEquals(ClientProtocol.Response.ResponseAPICase.GETREGIONNAMESRESPONSE,
        regionsResponse.getResponse().getResponseAPICase());

  }
}
