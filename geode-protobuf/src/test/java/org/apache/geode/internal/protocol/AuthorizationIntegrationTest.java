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
package org.apache.geode.internal.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.protocol.protobuf.AuthenticationAPI;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.serializer.ProtobufProtocolSerializer;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class AuthorizationIntegrationTest {

  private static final String TEST_USERNAME = "bob";
  private static final String TEST_PASSWORD = "bobspassword";
  public static final String TEST_REGION = "testRegion";

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
  public static final ResourcePermission READ_PERMISSION =
      new ResourcePermission(ResourcePermission.Resource.DATA, ResourcePermission.Operation.READ);
  public static final ResourcePermission WRITE_PERMISSION =
      new ResourcePermission(ResourcePermission.Resource.DATA, ResourcePermission.Operation.WRITE);

  @Before
  public void setUp() throws IOException {
    Properties expectedAuthProperties = new Properties();
    expectedAuthProperties.setProperty(ResourceConstants.USER_NAME, TEST_USERNAME);
    expectedAuthProperties.setProperty(ResourceConstants.PASSWORD, TEST_PASSWORD);

    securityPrincipal = "mockSecurityPrincipal";
    mockSecurityManager = mock(SecurityManager.class);
    when(mockSecurityManager.authenticate(expectedAuthProperties)).thenReturn(securityPrincipal);

    Properties properties = new Properties();
    CacheFactory cacheFactory = new CacheFactory(properties);
    cacheFactory.set("mcast-port", "0"); // sometimes it isn't due to other tests.

    cacheFactory.setSecurityManager(mockSecurityManager);
    cache = cacheFactory.create();

    cacheServer = cache.addCacheServer();
    cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer.setPort(cacheServerPort);
    cacheServer.start();

    cache.createRegionFactory().create(TEST_REGION);

    System.setProperty("geode.feature-protobuf-protocol", "true");
    System.setProperty("geode.protocol-authentication-mode", "SIMPLE");
    socket = new Socket("localhost", cacheServerPort);

    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(socket::isConnected);
    outputStream = socket.getOutputStream();
    inputStream = socket.getInputStream();
    outputStream.write(110);

    serializationService = new ProtobufSerializationService();
    protobufProtocolSerializer = new ProtobufProtocolSerializer();

    when(mockSecurityManager.authorize(same(securityPrincipal), any())).thenReturn(false);
    ClientProtocol.Message authenticationRequest = ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder()
            .setAuthenticationRequest(AuthenticationAPI.AuthenticationRequest.newBuilder()
                .putCredentials(ResourceConstants.USER_NAME, TEST_USERNAME)
                .putCredentials(ResourceConstants.PASSWORD, TEST_PASSWORD)))
        .build();
    authenticationRequest.writeDelimitedTo(outputStream);

    ClientProtocol.Message responseMessage = ClientProtocol.Message.parseDelimitedFrom(inputStream);
    assertEquals(ClientProtocol.Message.RESPONSE_FIELD_NUMBER,
        responseMessage.getMessageTypeCase().getNumber());
    assertEquals(ClientProtocol.Response.AUTHENTICATIONRESPONSE_FIELD_NUMBER,
        responseMessage.getResponse().getResponseAPICase().getNumber());
    AuthenticationAPI.AuthenticationResponse authenticationResponse =
        responseMessage.getResponse().getAuthenticationResponse();
    assertTrue(authenticationResponse.getAuthenticated());
  }

  @After
  public void shutDown() throws IOException {
    cache.close();
    socket.close();
  }


  @Test
  public void validateNoPermissions() throws Exception {
    when(mockSecurityManager.authorize(securityPrincipal, READ_PERMISSION)).thenReturn(false);
    when(mockSecurityManager.authorize(securityPrincipal, WRITE_PERMISSION)).thenReturn(false);

    verifyOperations(false, false);
  }

  @Test
  public void validateWritePermission() throws Exception {
    when(mockSecurityManager.authorize(securityPrincipal, READ_PERMISSION)).thenReturn(false);
    when(mockSecurityManager.authorize(securityPrincipal, WRITE_PERMISSION)).thenReturn(true);

    verifyOperations(false, true);
  }

  @Test
  public void validateReadPermission() throws Exception {
    when(mockSecurityManager.authorize(securityPrincipal, READ_PERMISSION)).thenReturn(true);
    when(mockSecurityManager.authorize(securityPrincipal, WRITE_PERMISSION)).thenReturn(false);

    verifyOperations(true, false);
  }

  @Test
  public void validateReadAndWritePermission() throws Exception {
    when(mockSecurityManager.authorize(securityPrincipal, READ_PERMISSION)).thenReturn(true);
    when(mockSecurityManager.authorize(securityPrincipal, WRITE_PERMISSION)).thenReturn(true);

    verifyOperations(true, true);
  }

  private void verifyOperations(boolean readAllowed, boolean writeAllowed) throws Exception {
    ClientProtocol.Message getRegionsMessage =
        ClientProtocol.Message.newBuilder().setRequest(ClientProtocol.Request.newBuilder()
            .setGetRegionNamesRequest(RegionAPI.GetRegionNamesRequest.newBuilder())).build();
    validateOperationAuthorized(getRegionsMessage, inputStream, outputStream,
        readAllowed ? ClientProtocol.Response.ResponseAPICase.GETREGIONNAMESRESPONSE
            : ClientProtocol.Response.ResponseAPICase.ERRORRESPONSE);

    ClientProtocol.Message putMessage = ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder()
            .setPutRequest(RegionAPI.PutRequest.newBuilder().setRegionName(TEST_REGION).setEntry(
                ProtobufUtilities.createEntry(serializationService, "TEST_KEY", "TEST_VALUE"))))
        .build();
    validateOperationAuthorized(putMessage, inputStream, outputStream,
        writeAllowed ? ClientProtocol.Response.ResponseAPICase.PUTRESPONSE
            : ClientProtocol.Response.ResponseAPICase.ERRORRESPONSE);

    ClientProtocol.Message removeMessage = ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder()
            .setRemoveRequest(RegionAPI.RemoveRequest.newBuilder().setRegionName(TEST_REGION)
                .setKey(ProtobufUtilities.createEncodedValue(serializationService, "TEST_KEY"))))
        .build();
    validateOperationAuthorized(removeMessage, inputStream, outputStream,
        writeAllowed ? ClientProtocol.Response.ResponseAPICase.REMOVERESPONSE
            : ClientProtocol.Response.ResponseAPICase.ERRORRESPONSE);
  }

  private void validateOperationAuthorized(ClientProtocol.Message message, InputStream inputStream,
      OutputStream outputStream, ClientProtocol.Response.ResponseAPICase expectedResponseType)
      throws Exception {
    protobufProtocolSerializer.serialize(message, outputStream);
    ClientProtocol.Message response = protobufProtocolSerializer.deserialize(inputStream);
    assertEquals(expectedResponseType, response.getResponse().getResponseAPICase());
    if (expectedResponseType == ClientProtocol.Response.ResponseAPICase.ERRORRESPONSE) {
      assertEquals(ProtocolErrorCode.AUTHORIZATION_FAILED.codeValue,
          response.getResponse().getErrorResponse().getError().getErrorCode());
    }
  }
}
