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
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
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
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class DisconnectClientIntegrationTest {
  public static final String SECURITY_PRINCIPAL = "principle";
  private Socket socket;
  private Cache cache;
  private SecurityManager securityManager;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    CacheFactory cacheFactory = new CacheFactory(new Properties());
    cacheFactory.set(ConfigurationProperties.MCAST_PORT, "0");
    cacheFactory.set(ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION, "false");
    cacheFactory.set(ConfigurationProperties.USE_CLUSTER_CONFIGURATION, "false");

    securityManager = mock(SecurityManager.class);
    cacheFactory.setSecurityManager(securityManager);
    when(securityManager.authenticate(any())).thenReturn(SECURITY_PRINCIPAL);
    when(securityManager.authorize(eq(SECURITY_PRINCIPAL), any())).thenReturn(true);

    cache = cacheFactory.create();

    CacheServer cacheServer = cache.addCacheServer();
    int cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer.setPort(cacheServerPort);
    cacheServer.start();

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    regionFactory.setDataPolicy(DataPolicy.PARTITION);


    System.setProperty("geode.feature-protobuf-protocol", "true");

    socket = new Socket("localhost", cacheServerPort);

    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);

    MessageUtil.performAndVerifyHandshake(socket);
  }

  @After
  public void tearDown() {
    cache.close();
    try {
      socket.close();
    } catch (IOException ignore) {
      // NOP
    }
  }

  @Test
  public void handlesResultFunction() throws Exception {
    authenticateWithServer();

    final ClientProtocol.Message requestMessage = createRequestMessageBuilder(
        ConnectionAPI.DisconnectClientRequest.newBuilder().setReason("Normal termination")).build();

    final ClientProtocol.Message responseMessage = writeMessage(requestMessage);
    assertEquals(responseMessage.toString(),
        ClientProtocol.Message.MessageTypeCase.DISCONNECTCLIENTRESPONSE,
        responseMessage.getMessageTypeCase());
    final ConnectionAPI.DisconnectClientResponse disconnectClientResponse =
        responseMessage.getDisconnectClientResponse();
    assertNotNull(disconnectClientResponse);
    assertEquals(true, disconnectClientResponse.getAcknowledged());
  }

  private void authenticateWithServer() throws IOException {
    ClientProtocol.Message.Builder request = ClientProtocol.Message.newBuilder()
        .setAuthenticationRequest(ConnectionAPI.AuthenticationRequest.newBuilder()
            .putCredentials(ResourceConstants.USER_NAME, "someuser")
            .putCredentials(ResourceConstants.PASSWORD, "somepassword"));

    ClientProtocol.Message response = writeMessage(request.build());
    assertEquals(response.toString(), true,
        response.getAuthenticationResponse().getAuthenticated());
  }

  private ClientProtocol.Message.Builder createRequestMessageBuilder(
      ConnectionAPI.DisconnectClientRequest.Builder disconnectClientRequest) {
    return ClientProtocol.Message.newBuilder().setDisconnectClientRequest(disconnectClientRequest);
  }

  private ClientProtocol.Message writeMessage(ClientProtocol.Message request) throws IOException {
    request.writeDelimitedTo(socket.getOutputStream());

    return ClientProtocol.Message.parseDelimitedFrom(socket.getInputStream());
  }
}
