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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ExecuteFunctionOnMemberIntegrationTest {
  private static final String TEST_REGION = "testRegion";
  private static final String TEST_FUNCTION_ID = "testFunction";
  private static final String SECURITY_PRINCIPAL = "principle";
  private static final String SERVER_NAME = "pericles";
  private ProtobufSerializationService serializationService;
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
    cacheFactory.set(ConfigurationProperties.NAME, SERVER_NAME);

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
    regionFactory.create(TEST_REGION);


    System.setProperty("geode.feature-protobuf-protocol", "true");

    socket = new Socket("localhost", cacheServerPort);

    await().until(socket::isConnected);

    MessageUtil.performAndVerifyHandshake(socket);

    serializationService = new ProtobufSerializationService();
  }

  private static class TestFunction<T> implements Function<T> {
    private final java.util.function.Function<FunctionContext<T>, Object> executeFunction;
    // non-null iff function has been executed.
    private final AtomicReference<FunctionContext> context = new AtomicReference<>();
    private final boolean hasResult;

    TestFunction() {
      this.executeFunction = arg -> null;
      this.hasResult = true;
    }

    TestFunction(java.util.function.Function<FunctionContext<T>, Object> executeFunction,
        boolean hasResult) {
      this.executeFunction = executeFunction;
      this.hasResult = hasResult;
    }

    @Override
    public String getId() {
      return TEST_FUNCTION_ID;
    }

    @Override
    public void execute(FunctionContext<T> context) {
      this.context.set(context);
      context.getResultSender().lastResult(executeFunction.apply(context));
    }

    @Override
    public boolean hasResult() {
      return hasResult;
    }

    @Override
    public boolean isHA() {
      // set for testing; we shouldn't need to test with isHA true because that's function service
      // details.
      return false;
    }

    FunctionContext getContext() {
      return context.get();
    }
  }

  @After
  public void tearDown() {
    cache.close();
    try {
      socket.close();
    } catch (IOException ignore) {
    }
    FunctionService.unregisterFunction(TEST_FUNCTION_ID);
  }

  @Test
  public void handlesNoResultFunction() throws IOException {
    TestFunction<Object> testFunction = new TestFunction<>(context -> null, false);
    FunctionService.registerFunction(testFunction);
    final ClientProtocol.Message responseMessage = authenticateAndSendMessage();

    assertNotNull(responseMessage);
    assertEquals(ClientProtocol.Message.MessageTypeCase.EXECUTEFUNCTIONONMEMBERRESPONSE,
        responseMessage.getMessageTypeCase());
    final FunctionAPI.ExecuteFunctionOnMemberResponse executeFunctionOnMemberResponse =
        responseMessage.getExecuteFunctionOnMemberResponse();

    assertEquals(0, executeFunctionOnMemberResponse.getResultsCount());

    await().until(() -> testFunction.getContext() != null);
  }

  @Test
  public void handlesResultFunction() throws Exception {
    final TestFunction<Object> testFunction =
        new TestFunction<>(functionContext -> Integer.valueOf(22), true);
    FunctionService.registerFunction(testFunction);
    final ClientProtocol.Message responseMessage = authenticateAndSendMessage();

    final FunctionAPI.ExecuteFunctionOnMemberResponse executeFunctionOnMemberResponse =
        getFunctionResponse(responseMessage);

    assertEquals(1, executeFunctionOnMemberResponse.getResultsCount());

    final Object responseValue =
        serializationService.decode(executeFunctionOnMemberResponse.getResults(0));
    assertTrue(responseValue instanceof Integer);
    assertEquals(22, responseValue);
  }

  @Test
  public void handlesException() throws IOException {
    final TestFunction<Object> testFunction = new TestFunction<>(context -> {
      throw new FunctionException();
    }, true);
    FunctionService.registerFunction(testFunction);

    final ClientProtocol.Message message = authenticateAndSendMessage();

    assertEquals(ClientProtocol.Message.MessageTypeCase.ERRORRESPONSE,
        message.getMessageTypeCase());
    final BasicTypes.Error error = message.getErrorResponse().getError();
    assertEquals(BasicTypes.ErrorCode.SERVER_ERROR, error.getErrorCode());
  }

  @Test
  public void handlesObjectThatCannotBeDecoded() throws IOException {
    final TestFunction<Object> testFunction = new TestFunction<>(context -> {
      return new Object();
    }, true);
    FunctionService.registerFunction(testFunction);

    final ClientProtocol.Message message = authenticateAndSendMessage();


    assertEquals(ClientProtocol.Message.MessageTypeCase.ERRORRESPONSE,
        message.getMessageTypeCase());
    final BasicTypes.Error error = message.getErrorResponse().getError();

    assertEquals(BasicTypes.ErrorCode.SERVER_ERROR, error.getErrorCode());

  }

  @Test
  public void handlesNullReturnValues() throws Exception {
    final TestFunction<Object> testFunction = new TestFunction<>(functionContext -> null, true);
    FunctionService.registerFunction(testFunction);
    final ClientProtocol.Message responseMessage = authenticateAndSendMessage();

    final FunctionAPI.ExecuteFunctionOnMemberResponse executeFunctionOnMemberResponse =
        getFunctionResponse(responseMessage);

    assertEquals(1, executeFunctionOnMemberResponse.getResultsCount());

    final Object responseValue =
        serializationService.decode(executeFunctionOnMemberResponse.getResults(0));
    assertNull(responseValue);
  }

  @Test
  public void argumentsArePassedToFunction() throws Exception {
    final TestFunction<Object> testFunction =
        new TestFunction<>(functionContext -> functionContext.getArguments(), true);
    FunctionService.registerFunction(testFunction);
    ClientProtocol.Message.Builder message = createRequestMessageBuilder(
        FunctionAPI.ExecuteFunctionOnMemberRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .addMemberName(SERVER_NAME).setArguments(serializationService.encode("hello")));

    authenticateWithServer();
    final ClientProtocol.Message responseMessage = writeMessage(message.build());

    FunctionAPI.ExecuteFunctionOnMemberResponse response = getFunctionResponse(responseMessage);

    assertEquals("hello", serializationService.decode(response.getResults(0)));
  }

  @Test
  public void permissionsAreRequiredToExecute() throws IOException {
    final ResourcePermission requiredPermission = new ResourcePermission(
        ResourcePermission.Resource.DATA, ResourcePermission.Operation.MANAGE);

    final TestFunction<Object> testFunction = new TestFunction<Object>() {
      @Override
      public Collection<ResourcePermission> getRequiredPermissions(String regionName) {
        return Arrays.asList(requiredPermission);
      }
    };
    FunctionService.registerFunction(testFunction);

    when(securityManager.authenticate(any())).thenReturn(SECURITY_PRINCIPAL);

    when(securityManager.authorize(eq(SECURITY_PRINCIPAL), eq(requiredPermission)))
        .thenReturn(false);

    final ClientProtocol.Message message = authenticateAndSendMessage();
    assertEquals("message=" + message, BasicTypes.ErrorCode.AUTHORIZATION_FAILED,
        message.getErrorResponse().getError().getErrorCode());

    verify(securityManager).authorize(eq(SECURITY_PRINCIPAL), eq(requiredPermission));
  }

  private FunctionAPI.ExecuteFunctionOnMemberResponse getFunctionResponse(
      ClientProtocol.Message responseMessage) {
    assertEquals(responseMessage.toString(),
        ClientProtocol.Message.MessageTypeCase.EXECUTEFUNCTIONONMEMBERRESPONSE,
        responseMessage.getMessageTypeCase());
    return responseMessage.getExecuteFunctionOnMemberResponse();
  }

  private void authenticateWithServer() throws IOException {
    ClientProtocol.Message.Builder request = ClientProtocol.Message.newBuilder()
        .setHandshakeRequest(ConnectionAPI.HandshakeRequest.newBuilder()
            .putCredentials(ResourceConstants.USER_NAME, "someuser")
            .putCredentials(ResourceConstants.PASSWORD, "somepassword"));

    ClientProtocol.Message response = writeMessage(request.build());
    assertEquals(response.toString(), true, response.getHandshakeResponse().getAuthenticated());
  }


  private ClientProtocol.Message authenticateAndSendMessage() throws IOException {
    authenticateWithServer();

    final ClientProtocol.Message request =
        createRequestMessageBuilder(FunctionAPI.ExecuteFunctionOnMemberRequest.newBuilder()
            .setFunctionID(TEST_FUNCTION_ID).addMemberName(SERVER_NAME)).build();

    return writeMessage(request);
  }


  private ClientProtocol.Message.Builder createRequestMessageBuilder(
      FunctionAPI.ExecuteFunctionOnMemberRequest.Builder functionRequest) {
    return ClientProtocol.Message.newBuilder().setExecuteFunctionOnMemberRequest(functionRequest);
  }

  private ClientProtocol.Message writeMessage(ClientProtocol.Message request) throws IOException {
    request.writeDelimitedTo(socket.getOutputStream());

    return ClientProtocol.Message.parseDelimitedFrom(socket.getInputStream());
  }

}
