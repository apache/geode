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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.protocol.protobuf.v1.serializer.ProtobufProtocolSerializer;
import org.apache.geode.internal.protocol.protobuf.v1.serializer.exception.InvalidProtocolMessageException;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufUtilities;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class AuthorizationIntegrationTest {

  private static final String TEST_USERNAME = "bob";
  private static final String TEST_PASSWORD = "bobspassword";

  private static final String TEST_REGION1 = "testRegion1";
  private static final String TEST_REGION2 = "testRegion2";
  private static final String TEST_KEY1 = "testKey1";
  private static final String TEST_KEY2 = "testKey2";

  private final String OQLTwoRegionTestQuery =
      "select * from /" + TEST_REGION1 + " one, /" + TEST_REGION2 + " two where one.id = two.id";

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
  private TestSecurityManager securityManager;

  // Read access to all regions, all keys
  public static final ResourcePermission READ_PERMISSION = ResourcePermissions.DATA_READ;

  // Write access to all regions, all keys
  public static final ResourcePermission WRITE_PERMISSION = ResourcePermissions.DATA_WRITE;

  private class TestSecurityManager implements SecurityManager {
    private Set<ResourcePermission> allowedPermissions = new HashSet<>();

    void addAllowedPermission(ResourcePermission permission) {
      allowedPermissions.add(permission);
    }

    @Override
    public Object authenticate(Properties credentials) throws AuthenticationFailedException {
      return securityPrincipal;
    }

    @Override
    public boolean authorize(Object principal, ResourcePermission permission) {
      // Only allow data operations and only from the expected principal
      if (principal != securityPrincipal
          || permission.getResource() != ResourcePermission.Resource.DATA) {
        return false;
      }
      // Succeed if user has permission for all regions and all keys for the given operation
      if (allowedPermissions.contains(
          new ResourcePermission(ResourcePermission.Resource.DATA, permission.getOperation()))) {
        return true;
      }
      // Succeed if user has permission for all keys in the given region for the given operation
      if (allowedPermissions.contains(new ResourcePermission(ResourcePermission.Resource.DATA,
          permission.getOperation(), permission.getTarget()))) {
        return true;
      }

      return allowedPermissions.contains(permission);
    }
  }

  @Before
  public void setUp() throws IOException, InvalidProtocolMessageException {
    Properties expectedAuthProperties = new Properties();
    expectedAuthProperties.setProperty(ResourceConstants.USER_NAME, TEST_USERNAME);
    expectedAuthProperties.setProperty(ResourceConstants.PASSWORD, TEST_PASSWORD);

    securityPrincipal = "mockSecurityPrincipal";
    securityManager = new TestSecurityManager();

    Properties properties = new Properties();
    CacheFactory cacheFactory = new CacheFactory(properties);
    cacheFactory.set("mcast-port", "0"); // sometimes it isn't due to other tests.

    cacheFactory.setSecurityManager(securityManager);
    cache = cacheFactory.create();

    cacheServer = cache.addCacheServer();
    cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer.setPort(cacheServerPort);
    cacheServer.start();

    cache.createRegionFactory().create(TEST_REGION1);
    cache.createRegionFactory().create(TEST_REGION2);

    System.setProperty("geode.feature-protobuf-protocol", "true");
    System.setProperty("geode.protocol-authentication-mode", "SIMPLE");
    socket = new Socket("localhost", cacheServerPort);

    await().until(socket::isConnected);
    outputStream = socket.getOutputStream();
    inputStream = socket.getInputStream();

    serializationService = new ProtobufSerializationService();
    protobufProtocolSerializer = new ProtobufProtocolSerializer();

    MessageUtil.performAndVerifyHandshake(socket);

    ClientProtocol.Message authenticationRequest = ClientProtocol.Message.newBuilder()
        .setHandshakeRequest(ConnectionAPI.HandshakeRequest.newBuilder()
            .putCredentials(ResourceConstants.USER_NAME, TEST_USERNAME)
            .putCredentials(ResourceConstants.PASSWORD, TEST_PASSWORD))
        .build();
    authenticationRequest.writeDelimitedTo(outputStream);

    ClientProtocol.Message responseMessage = ClientProtocol.Message.parseDelimitedFrom(inputStream);
    assertEquals(ClientProtocol.Message.HANDSHAKERESPONSE_FIELD_NUMBER,
        responseMessage.getMessageTypeCase().getNumber());
    ConnectionAPI.HandshakeResponse authenticationResponse = responseMessage.getHandshakeResponse();
    assertTrue(authenticationResponse.getAuthenticated());
  }

  @After
  public void shutDown() throws IOException {
    cache.close();
    socket.close();
  }


  @Test
  public void validateNoPermissions() throws Exception {
    verifyLocatorOperation(false);
    verifyOperations(false, false, TEST_REGION1, TEST_KEY1);
  }

  @Test
  public void validateWritePermission() throws Exception {
    securityManager.addAllowedPermission(WRITE_PERMISSION);

    verifyLocatorOperation(false);
    verifyOperations(false, true, TEST_REGION1, TEST_KEY1);
  }

  @Test
  public void validateReadPermission() throws Exception {
    securityManager.addAllowedPermission(READ_PERMISSION);

    verifyLocatorOperation(true);
    verifyOperations(true, false, TEST_REGION1, TEST_KEY1);
  }

  @Test
  public void validateReadAndWritePermission() throws Exception {
    securityManager.addAllowedPermission(WRITE_PERMISSION);
    securityManager.addAllowedPermission(READ_PERMISSION);

    verifyLocatorOperation(true);
    verifyOperations(true, true, TEST_REGION1, TEST_KEY1);
  }

  @Test
  public void validateRegionLevelPermissions() throws Exception {
    securityManager.addAllowedPermission(new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.WRITE, TEST_REGION1));
    securityManager.addAllowedPermission(new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.READ, TEST_REGION2));

    verifyOperations(false, true, TEST_REGION1, TEST_KEY1);
    verifyOperations(false, true, TEST_REGION1, TEST_KEY2);
    verifyOperations(true, false, TEST_REGION2, TEST_KEY1);
    verifyOperations(true, false, TEST_REGION2, TEST_KEY2);
  }

  @Test
  public void validateKeyLevelPermissions() throws Exception {
    securityManager.addAllowedPermission(new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.WRITE, TEST_REGION1, TEST_KEY1));
    securityManager.addAllowedPermission(new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.READ, TEST_REGION2, TEST_KEY2));

    verifyOperations(false, true, TEST_REGION1, TEST_KEY1);
    verifyOperations(false, false, TEST_REGION1, TEST_KEY2);
    verifyOperations(false, false, TEST_REGION2, TEST_KEY1);
    verifyOperations(true, false, TEST_REGION2, TEST_KEY2);
  }

  @Test
  public void validateRegionLevelPermissionsOnBatchOperations() throws Exception {
    securityManager.addAllowedPermission(new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.WRITE, TEST_REGION1));
    securityManager.addAllowedPermission(new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.READ, TEST_REGION2));

    verifyBatchOperation(true, TEST_REGION1, false, false);
    verifyBatchOperation(false, TEST_REGION1, true, true);
    verifyBatchOperation(true, TEST_REGION2, true, true);
    verifyBatchOperation(false, TEST_REGION2, false, false);
  }

  @Test
  public void validateKeyLevelPermissionsOnBatchOperations() throws Exception {
    securityManager.addAllowedPermission(new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.WRITE, TEST_REGION1, TEST_KEY1));
    securityManager.addAllowedPermission(new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.READ, TEST_REGION2, TEST_KEY2));

    verifyBatchOperation(true, TEST_REGION1, false, false);
    verifyBatchOperation(false, TEST_REGION1, true, false);
    verifyBatchOperation(true, TEST_REGION2, false, true);
    verifyBatchOperation(false, TEST_REGION2, false, false);
  }

  @Test
  public void testOQLAuthorization() throws Exception {
    securityManager.addAllowedPermission(new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.READ, TEST_REGION1));
    securityManager.addAllowedPermission(new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.READ, TEST_REGION2));

    ClientProtocol.Message request = ClientProtocol.Message.newBuilder()
        .setOqlQueryRequest(RegionAPI.OQLQueryRequest.newBuilder().setQuery(OQLTwoRegionTestQuery))
        .build();
    protobufProtocolSerializer.serialize(request, outputStream);

    ClientProtocol.Message response = protobufProtocolSerializer.deserialize(inputStream);
    assertEquals(ClientProtocol.Message.MessageTypeCase.OQLQUERYRESPONSE,
        response.getMessageTypeCase());
  }

  @Test
  public void testOQLRequiresAuthorizationForMultipleRegions() throws Exception {
    securityManager.addAllowedPermission(new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.READ, TEST_REGION1));

    ClientProtocol.Message request = ClientProtocol.Message.newBuilder()
        .setOqlQueryRequest(RegionAPI.OQLQueryRequest.newBuilder().setQuery(OQLTwoRegionTestQuery))
        .build();
    protobufProtocolSerializer.serialize(request, outputStream);

    ClientProtocol.Message response = protobufProtocolSerializer.deserialize(inputStream);
    assertEquals(ClientProtocol.Message.MessageTypeCase.ERRORRESPONSE,
        response.getMessageTypeCase());
    assertEquals(BasicTypes.ErrorCode.AUTHORIZATION_FAILED,
        response.getErrorResponse().getError().getErrorCode());
  }

  private void verifyBatchOperation(boolean testRead, String region, boolean expectedKey1Success,
      boolean expectedKey2Success) throws Exception {
    ClientProtocol.Message request;
    if (testRead) {
      request = ClientProtocol.Message.newBuilder()
          .setGetAllRequest(RegionAPI.GetAllRequest.newBuilder().setRegionName(region)
              .addKey(serializationService.encode(TEST_KEY1))
              .addKey(serializationService.encode(TEST_KEY2)))
          .build();
    } else {
      request = ClientProtocol.Message.newBuilder().setPutAllRequest(RegionAPI.PutAllRequest
          .newBuilder().setRegionName(region)
          .addEntry(ProtobufUtilities.createEntry(serializationService, TEST_KEY1, "TEST_VALUE"))
          .addEntry(ProtobufUtilities.createEntry(serializationService, TEST_KEY2, "TEST_VALUE")))
          .build();
    }

    protobufProtocolSerializer.serialize(request, outputStream);
    ClientProtocol.Message response = protobufProtocolSerializer.deserialize(inputStream);
    assertNotEquals(ClientProtocol.Message.MessageTypeCase.ERRORRESPONSE,
        response.getMessageTypeCase());

    List<BasicTypes.KeyedError> keyedErrors =
        testRead ? response.getGetAllResponse().getFailuresList()
            : response.getPutAllResponse().getFailedKeysList();
    String operation = testRead ? "getAll" : "putAll";
    if (errorListContainsKey(keyedErrors, serializationService.encode(TEST_KEY1))) {
      if (expectedKey1Success) {
        fail("Unexpectedly failed " + operation + " operation for key " + TEST_KEY1);
      }
    } else if (!expectedKey1Success) {
      fail("Unexpected success in " + operation + " operation for key " + TEST_KEY1);
    }
    if (errorListContainsKey(keyedErrors, serializationService.encode(TEST_KEY2))) {
      if (expectedKey2Success) {
        fail("Unexpectedly failed " + operation + " operation for key " + TEST_KEY2);
      }
    } else if (!expectedKey2Success) {
      fail("Unexpected success in " + operation + " operation for key " + TEST_KEY2);
    }
  }

  private boolean errorListContainsKey(List<BasicTypes.KeyedError> errors,
      BasicTypes.EncodedValue key) {
    for (BasicTypes.KeyedError error : errors) {
      if (error.getKey().equals(key)) {
        return true;
      }
    }
    return false;
  }

  private void verifyLocatorOperation(boolean readAllowed) throws Exception {
    ClientProtocol.Message getRegionsMessage = ClientProtocol.Message.newBuilder()
        .setGetRegionNamesRequest(RegionAPI.GetRegionNamesRequest.newBuilder()).build();
    validateOperationAuthorized(getRegionsMessage, inputStream, outputStream,
        readAllowed ? ClientProtocol.Message.MessageTypeCase.GETREGIONNAMESRESPONSE
            : ClientProtocol.Message.MessageTypeCase.ERRORRESPONSE);
  }

  private void verifyOperations(boolean readAllowed, boolean writeAllowed, String region,
      String key) throws Exception {
    ClientProtocol.Message getMessage =
        ClientProtocol.Message.newBuilder().setGetRequest(RegionAPI.GetRequest.newBuilder()
            .setRegionName(region).setKey(serializationService.encode(key))).build();
    validateOperationAuthorized(getMessage, inputStream, outputStream,
        readAllowed ? ClientProtocol.Message.MessageTypeCase.GETRESPONSE
            : ClientProtocol.Message.MessageTypeCase.ERRORRESPONSE);

    ClientProtocol.Message putMessage = ClientProtocol.Message.newBuilder()
        .setPutRequest(RegionAPI.PutRequest.newBuilder().setRegionName(region)
            .setEntry(ProtobufUtilities.createEntry(serializationService, key, "TEST_VALUE")))
        .build();
    validateOperationAuthorized(putMessage, inputStream, outputStream,
        writeAllowed ? ClientProtocol.Message.MessageTypeCase.PUTRESPONSE
            : ClientProtocol.Message.MessageTypeCase.ERRORRESPONSE);

    ClientProtocol.Message removeMessage =
        ClientProtocol.Message.newBuilder().setRemoveRequest(RegionAPI.RemoveRequest.newBuilder()
            .setRegionName(region).setKey(serializationService.encode(key))).build();
    validateOperationAuthorized(removeMessage, inputStream, outputStream,
        writeAllowed ? ClientProtocol.Message.MessageTypeCase.REMOVERESPONSE
            : ClientProtocol.Message.MessageTypeCase.ERRORRESPONSE);
  }

  private void validateOperationAuthorized(ClientProtocol.Message message, InputStream inputStream,
      OutputStream outputStream, ClientProtocol.Message.MessageTypeCase expectedResponseType)
      throws Exception {
    protobufProtocolSerializer.serialize(message, outputStream);
    ClientProtocol.Message response = protobufProtocolSerializer.deserialize(inputStream);
    assertEquals(expectedResponseType, response.getMessageTypeCase());
    if (expectedResponseType == ClientProtocol.Message.MessageTypeCase.ERRORRESPONSE) {
      Assert.assertEquals(BasicTypes.ErrorCode.AUTHORIZATION_FAILED,
          response.getErrorResponse().getError().getErrorCode());
    }
  }
}
