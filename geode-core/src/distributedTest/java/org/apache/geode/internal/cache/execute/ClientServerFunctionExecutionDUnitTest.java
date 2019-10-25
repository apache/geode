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
package org.apache.geode.internal.cache.execute;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.FunctionServiceTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({ClientServerTest.class, FunctionServiceTest.class})
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class ClientServerFunctionExecutionDUnitTest extends PRClientServerTestBase {
  private static final Logger logger = LogService.getLogger();

  private static final String TEST_FUNCTION1 = TestFunction.TEST_FUNCTION1;

  private Boolean isByName = null;
  Function function = null;
  private Boolean toRegister = null;
  private static final String retryRegionName = "RetryDataRegion";
  private static Region metaDataRegion;

  public ClientServerFunctionExecutionDUnitTest() {
    super();
  }

  @Override
  protected final void postSetUpPRClientServerTestBase() {
    IgnoredException.addIgnoredException("java.net.ConnectException");
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties result = super.getDistributedSystemProperties();
    result.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.internal.cache.execute.**;org.apache.geode.test.dunit.**");
    return result;
  }

  @Test
  public void throwsExceptionWhenFunctionNotRegisteredOnServer() {
    createScenario();
    try {
      client.invoke(ClientServerFunctionExecutionDUnitTest::executeRegisteredFunction);
    } catch (Exception e) {
      assertTrue((e.getCause() instanceof ServerOperationException));
      assertTrue(
          e.getCause().getMessage().contains("The function is not registered for function id"));
    }
  }

  @Test
  public void noExceptionWhenFunctionRegisteredOnServer() {
    createScenario();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
    registerFunctionAtServer(function);
    client.invoke(ClientServerFunctionExecutionDUnitTest::executeRegisteredFunction);
  }

  /*
   * Execution of the function on server using the name of the function
   */
  @Test
  public void testServerExecution_byName() {
    createScenario();

    // function = new TestFunction1();
    function = new TestFunction(true, TEST_FUNCTION1);

    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    toRegister = Boolean.TRUE;
    logger.info(
        "ClientServerFFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.serverExecution(isByName, function,
        toRegister));
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.allServerExecution(isByName,
        function, toRegister));
  }


  @Test
  public void testServerExecution_sendException() {
    createScenario();

    // function = new TestFunction1();
    function = new TestFunction(true, TestFunction.TEST_FUNCTION_SEND_EXCEPTION);

    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    toRegister = Boolean.TRUE;
    logger.info(
        "ClientServerFFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest
        .serverExecution_SendException(isByName, function, toRegister));
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest
        .allServerExecution_SendException(isByName, function, toRegister));
  }

  /*
   * Execution of the function on server using the name of the function
   */
  @Test
  public void testServerExecution_NoLastResult() {
    createScenario();

    // function = new TestFunction1();
    function = new TestFunction(true, TestFunction.TEST_FUNCTION_NO_LASTRESULT);

    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    toRegister = Boolean.TRUE;
    logger.info(
        "ClientServerFFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest
        .serverExecution_NoLastResult(isByName, function, toRegister));
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest
        .allServerExecution_NoLastResult(isByName, function, toRegister));
  }

  @Test
  public void testServerExecution_byName_WithoutRegister() {
    createScenario();

    // function = new TestFunction1();
    function = new TestFunction(true, TEST_FUNCTION1);

    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    toRegister = Boolean.FALSE;
    logger.info(
        "ClientServerFFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.serverExecution(isByName, function,
        toRegister));
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.allServerExecution(isByName,
        function, toRegister));
  }

  /*
   * Execution of the inline function on server
   */
  @Test
  public void testServerExecution_byInlineFunction() {
    createScenario();
    logger.info(
        "ClientServerFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(ClientServerFunctionExecutionDUnitTest::serverExecution_Inline);
    client.invoke(ClientServerFunctionExecutionDUnitTest::allServerExecution_Inline);
  }


  /*
   * Execution of the inline function on server
   */
  @Test
  public void testServerExecution_byInlineFunction_InvalidAttrbiutes() {
    createScenario();
    logger.info(
        "ClientServerFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(
        ClientServerFunctionExecutionDUnitTest::serverExecution_Inline_InvalidAttributes);
  }

  /*
   * Execution of the inline function on server
   */
  @Test
  public void testBug40714() {
    createScenario();
    logger
        .info("ClientServerFunctionExecutionDUnitTest#testBug40714 : Starting test");

    server1.invoke(
        (SerializableRunnableIF) ClientServerFunctionExecutionDUnitTest::registerFunction);
    server1.invoke(
        (SerializableRunnableIF) ClientServerFunctionExecutionDUnitTest::registerFunction);
    server1.invoke(
        (SerializableRunnableIF) ClientServerFunctionExecutionDUnitTest::registerFunction);
    client
        .invoke((SerializableRunnableIF) ClientServerFunctionExecutionDUnitTest::registerFunction);
    client.invoke(ClientServerFunctionExecutionDUnitTest::FunctionExecution_Inline_Bug40714);

  }

  public static void registerFunction() {
    FunctionService.registerFunction(new FunctionAdapter() {
      @Override
      public void execute(FunctionContext context) {
        @SuppressWarnings("unchecked")
        final ResultSender<Object> resultSender = context.getResultSender();
        if (context.getArguments() instanceof String) {
          resultSender.lastResult("Failure");
        } else if (context.getArguments() instanceof Boolean) {
          resultSender.lastResult(Boolean.FALSE);
        }
      }

      @Override
      public String getId() {
        return "Function";
      }

      @Override
      public boolean hasResult() {
        return true;
      }
    });
  }

  private static void FunctionExecution_Inline_Bug40714() {
    DistributedSystem.setThreadsSocketPolicy(false);
    Execution member = FunctionService.onServers(pool);
    try {
      ResultCollector rs = member.setArguments(Boolean.TRUE).execute(new FunctionAdapter() {
        @Override
        public void execute(FunctionContext context) {
          @SuppressWarnings("unchecked")
          final ResultSender<Object> resultSender = context.getResultSender();
          if (context.getArguments() instanceof String) {
            resultSender.lastResult("Success");
          } else if (context.getArguments() instanceof Boolean) {
            resultSender.lastResult(Boolean.TRUE);
          }
        }

        @Override
        public String getId() {
          return "Function";
        }

        @Override
        public boolean hasResult() {
          return true;
        }
      });
      List resultList = (List) rs.getResult();
      assertEquals(3, resultList.size());
      assertEquals(Boolean.TRUE, resultList.get(0));
      assertEquals(Boolean.TRUE, resultList.get(1));
      assertEquals(Boolean.TRUE, resultList.get(2));

    } catch (Exception ex) {
      ex.printStackTrace();
      logger.info("Exception : ", ex);
      fail("Test failed after the execute operation.");
    }
  }

  /*
   * Execution of the function on server using the name of the function
   */
  @Test
  public void testServerExecution_SocketTimeOut() {
    createScenario();
    function = new TestFunction(true, TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);

    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    toRegister = Boolean.TRUE;
    logger.info(
        "ClientServerFFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.serverExecution(isByName, function,
        toRegister));
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.allServerExecution(isByName,
        function, toRegister));
  }

  @Test
  public void testServerExecution_SocketTimeOut_WithoutRegister() {
    createScenario();
    function = new TestFunction(true, TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);

    registerFunctionAtServer(function);
    isByName = Boolean.TRUE;
    toRegister = Boolean.FALSE;
    logger.info(
        "ClientServerFFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.serverExecution(isByName, function,
        toRegister));
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.allServerExecution(isByName,
        function, toRegister));
  }


  /*
   * Ensure that the while executing the function if the servers is down then the execution is
   * failover to other available server
   */
  @SuppressWarnings("rawtypes")
  @Test
  public void testOnServerFailoverWithOneServerDownHA() {
    // The test code appears to trigger this because the first
    // call to the function disconnects from the DS but does not call
    // last result;
    IgnoredException.addIgnoredException("did not send last result");
    createScenario();

    server1.invoke(ClientServerFunctionExecutionDUnitTest::createReplicatedRegion);

    server2.invoke(ClientServerFunctionExecutionDUnitTest::createReplicatedRegion);

    server3.invoke(ClientServerFunctionExecutionDUnitTest::createReplicatedRegion);

    client.invoke(ClientServerFunctionExecutionDUnitTest::createProxyRegion);

    function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA_SERVER);
    registerFunctionAtServer(function);

    client.invoke(() -> ClientServerFunctionExecutionDUnitTest
        .serverExecutionHAOneServerDown(Boolean.FALSE, function, Boolean.FALSE));

    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.verifyMetaData(1,
        1));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testOnServerFailoverWithTwoServerDownHA() {
    // The test code appears to trigger this because the first
    // call to the function disconnects from the DS but does not call
    // last result;
    IgnoredException.addIgnoredException("Socket Closed");
    IgnoredException.addIgnoredException("did not send last result");
    createScenario();

    server1.invoke(ClientServerFunctionExecutionDUnitTest::createReplicatedRegion);

    server2.invoke(ClientServerFunctionExecutionDUnitTest::createReplicatedRegion);

    server3.invoke(ClientServerFunctionExecutionDUnitTest::createReplicatedRegion);

    client.invoke(ClientServerFunctionExecutionDUnitTest::createProxyRegion);

    function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA_SERVER);
    registerFunctionAtServer(function);

    client.invoke(() -> ClientServerFunctionExecutionDUnitTest
        .serverExecutionHATwoServerDown(Boolean.FALSE, function, Boolean.FALSE));

    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.verifyMetaData(2,
        0));
  }


  /*
   * Ensure that the while executing the function if the servers are down then the execution
   * shouldn't failover to other available server
   */
  @Test
  public void testOnServerFailoverNonHA() {
    // The test code appears to trigger this because the first
    // call to the function disconnects from the DS but does not call
    // last result;
    IgnoredException.addIgnoredException("did not send last result");
    createScenario();
    server1.invoke(ClientServerFunctionExecutionDUnitTest::createReplicatedRegion);

    server2.invoke(ClientServerFunctionExecutionDUnitTest::createReplicatedRegion);

    server3.invoke(ClientServerFunctionExecutionDUnitTest::createReplicatedRegion);

    client.invoke(ClientServerFunctionExecutionDUnitTest::createProxyRegion);

    function = new TestFunction(true, TestFunction.TEST_FUNCTION_NONHA_SERVER);
    registerFunctionAtServer(function);

    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.serverExecutionNonHA(Boolean.FALSE,
        function, Boolean.FALSE));
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.verifyMetaData(1,
        0));
  }


  /*
   * Execution of the function on a server.Function throws the FunctionInvocationTargetException. As
   * this is the case of HA then system should retry the function execution. After 5th attempt
   * function will send Boolean as last result.
   */
  @Test
  public void testOnServerExecution_FunctionInvocationTargetException() {
    createScenario();
    function = new TestFunction(true, TestFunction.TEST_FUNCTION_ONSERVER_REEXECUTE_EXCEPTION);
    registerFunctionAtServer(function);

    client.invoke(() -> ClientServerFunctionExecutionDUnitTest
        .serverFunctionExecution_FunctionInvocationTargetException(Boolean.FALSE, function,
            Boolean.FALSE));
  }

  @Test
  public void onRegionShouldThrowExceptionWhenThePoolAssociatedWithTheRegionCanNotBeFound() {
    function = new TestFunction(true, TEST_FUNCTION1);
    createScenario();
    registerFunctionAtServer(function);

    server1.invoke(ClientServerFunctionExecutionDUnitTest::createReplicatedRegion);
    server2.invoke(ClientServerFunctionExecutionDUnitTest::createReplicatedRegion);
    server3.invoke(ClientServerFunctionExecutionDUnitTest::createReplicatedRegion);

    client.invoke(() -> {
      ClientServerFunctionExecutionDUnitTest.createProxyRegion();
      assertThatThrownBy(() -> HijackedFunctionService.onRegion(metaDataRegion).execute(function))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageMatching("Could not find a pool named (.*)");
    });
  }

  private static class HijackedFunctionService extends FunctionService {
    public HijackedFunctionService(FunctionExecutionService functionExecutionService) {
      super(functionExecutionService);
    }

    public static Execution onRegion(Region region) {
      return new HijackedInternalFunctionServiceImpl().onRegion(region);
    }
  }

  private static class HijackedInternalFunctionServiceImpl
      extends InternalFunctionExecutionServiceImpl {
    @Override
    protected Pool findPool(String poolName) {
      return null;
    }
  }

  private void createScenario() {
    logger
        .info("ClientServerFFunctionExecutionDUnitTest#createScenario : creating scenario");
    createClientServerScenarionWithoutRegion();
  }

  private static void serverExecution(Boolean isByName, Function function, Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister) {
      FunctionService.registerFunction(function);
    }
    Execution member = FunctionService.onServer(pool);

    try {
      ResultCollector rs = execute(member, Boolean.TRUE, function, isByName);
      assertEquals(Boolean.TRUE, ((List) rs.getResult()).get(0));

    } catch (Exception ex) {
      ex.printStackTrace();
      logger.info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }

    try {
      final HashSet<String> testKeysSet = new HashSet<>();
      for (int i = 0; i < 20; i++) {
        testKeysSet.add("execKey-" + i);
      }

      ResultCollector rs = execute(member, testKeysSet, function, isByName);

      List resultList = (List) rs.getResult();
      for (int i = 0; i < 20; i++) {
        assertTrue(((List) (resultList.get(0))).contains("execKey-" + i));
      }

    } catch (Exception ex) {
      ex.printStackTrace();
      logger.info("Exception : ", ex);
      fail("Test failed after the execute operations");
    }
  }


  private static void executeRegisteredFunction() {
    DistributedSystem.setThreadsSocketPolicy(false);
    Execution member = FunctionService.onServer(pool);

    // remove any existing attributes
    ((AbstractExecution) member).removeFunctionAttributes(TestFunction.TEST_FUNCTION1);
    ResultCollector rs = member.setArguments(Boolean.TRUE).execute(TestFunction.TEST_FUNCTION1);
    assertEquals(Boolean.TRUE, ((List) rs.getResult()).get(0));
    byte[] functionAttributes =
        ((AbstractExecution) member).getFunctionAttributes(TestFunction.TEST_FUNCTION1);
    assertNotNull(functionAttributes);
  }


  private static void serverExecution_SendException(Boolean isByName, Function function,
      Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister) {
      FunctionService.registerFunction(function);
    }
    Execution member = FunctionService.onServer(pool);

    try {
      ResultCollector rs = execute(member, Boolean.TRUE, function, isByName);
      assertTrue(((List) rs.getResult()).get(0) instanceof MyFunctionExecutionException);

    } catch (Exception ex) {
      ex.printStackTrace();
      logger.info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }

    try {
      final HashSet<String> testKeysSet = new HashSet<>();
      for (int i = 0; i < 20; i++) {
        testKeysSet.add("execKey-" + i);
      }

      ResultCollector rs = execute(member, testKeysSet, function, isByName);

      List resultList = (List) rs.getResult();
      assertEquals((testKeysSet.size() + 1), resultList.size());
      Iterator resultIterator = resultList.iterator();
      int exceptionCount = 0;
      while (resultIterator.hasNext()) {
        Object o = resultIterator.next();
        if (o instanceof MyFunctionExecutionException) {
          exceptionCount++;
        }
      }
      assertEquals(1, exceptionCount);

    } catch (Exception ex) {
      ex.printStackTrace();
      logger.info("Exception : ", ex);
      fail("Test failed after the execute operations");
    }
  }

  private static void createReplicatedRegion() {
    metaDataRegion = cache.createRegionFactory(RegionShortcut.REPLICATE).create(retryRegionName);
  }

  public static void createProxyRegion() {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(pool.getName());
    RegionAttributes attrs = factory.create();
    metaDataRegion = cache.createRegion(retryRegionName, attrs);
    assertNotNull(metaDataRegion);
  }

  private static void verifyMetaData(Integer arg1, Integer arg2) {
    try {
      if (arg1 == 0) {
        assertNull(metaDataRegion.get("stopped"));
      } else {
        assertEquals(metaDataRegion.get("stopped"), arg1);
      }

      if (arg2 == 0) {
        assertNull(metaDataRegion.get("sentresult"));
      } else {
        assertEquals(metaDataRegion.get("sentresult"), arg2);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("The metadata doesn't match with the expected value.");
    }
  }

  public static void verifyDeadAndLiveServers(final Integer expectedLiveServers) {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;

      @Override
      public boolean done() {
        int sz = pool.getConnectedServerCount();
        logger.info("Checking for the Live Servers : Expected  : " + expectedLiveServers
            + " Available :" + sz);
        if (sz == expectedLiveServers) {
          return true;
        }
        excuse = "Expected " + expectedLiveServers + " but found " + sz;
        return false;
      }

      @Override
      public String description() {
        return excuse;
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
  }

  private static Object serverExecutionHAOneServerDown(Boolean isByName, Function function,
      Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister) {
      FunctionService.registerFunction(function);
    }
    Execution member = FunctionService.onServer(pool);
    ResultCollector rs = null;
    try {
      ArrayList<String> args = new ArrayList<>();
      args.add(retryRegionName);
      args.add("serverExecutionHAOneServerDown");
      rs = execute(member, args, function, isByName);
      assertEquals(retryRegionName, ((List) rs.getResult()).get(0));
    } catch (Exception ex) {
      ex.printStackTrace();
      logger.info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }
    return rs.getResult();
  }

  private static void serverExecutionHATwoServerDown(Boolean isByName, Function function,
      Boolean toRegister) {
    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister) {
      FunctionService.registerFunction(function);
    }
    Execution member = FunctionService.onServer(pool);
    try {
      ArrayList<String> args = new ArrayList<>();
      args.add(retryRegionName);
      args.add("serverExecutionHATwoServerDown");
      execute(member, args, function, isByName);
      fail("Expected ServerConnectivityException not thrown!");
    } catch (Exception ex) {
      if (!(ex instanceof ServerConnectivityException)) {
        ex.printStackTrace();
        logger.info("Exception : ", ex);
        fail("Test failed after the execute operation");
      }
    }
  }

  private static Object serverExecutionNonHA(Boolean isByName, Function function,
      Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister) {
      FunctionService.registerFunction(function);
    }
    Execution member = FunctionService.onServer(pool);
    try {
      ArrayList<String> args = new ArrayList<>();
      args.add(retryRegionName);
      args.add("serverExecutionNonHA");
      execute(member, args, function, isByName);
      fail("Expected ServerConnectivityException not thrown!");
    } catch (Exception ex) {
      if (!(ex instanceof ServerConnectivityException)) {
        ex.printStackTrace();
        logger.info("Exception : ", ex);
        fail("Test failed after the execute operation");
      }
    }
    return null;
  }

  @SuppressWarnings("rawtypes")
  private static void serverFunctionExecution_FunctionInvocationTargetException(Boolean isByName,
      Function function,
      Boolean toRegister) {
    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister) {
      FunctionService.registerFunction(function);
    }
    Execution member = FunctionService.onServer(pool);
    try {
      ResultCollector rs = execute(member, Boolean.TRUE, function, isByName);
      ArrayList list = (ArrayList) rs.getResult();
      assertEquals("Value of send result of the executed function : " + list.get(0)
          + "does not match the expected value : " + 1, 1, (int) ((Integer) list.get(0)));
      assertTrue("Value of last result of the executed function : " + list.get(0)
          + "is not equal or more than expected value : " + 5, ((Integer) list.get(1)) >= 5);
    } catch (Exception ex) {
      ex.printStackTrace();
      Assert.fail("This is not expected Exception", ex);
    }
  }

  private static void serverExecution_NoLastResult(Boolean isByName, Function function,
      Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister) {
      FunctionService.registerFunction(function);
    }
    Execution member = FunctionService.onServer(pool);

    try {
      ResultCollector rs = execute(member, Boolean.TRUE, function, isByName);
      assertEquals(Boolean.TRUE, ((List) rs.getResult()).get(0));
      fail("Expected FunctionException : Function did not send last result");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("did not send last result"));
    }


  }

  private static void serverExecution_Inline() {

    DistributedSystem.setThreadsSocketPolicy(false);
    Execution member = FunctionService.onServer(pool);

    try {
      ResultCollector rs = member.setArguments(Boolean.TRUE).execute(new FunctionAdapter() {
        @Override
        public void execute(FunctionContext context) {
          @SuppressWarnings("unchecked")
          final ResultSender<Object> resultSender = context.getResultSender();
          if (context.getArguments() instanceof String) {
            resultSender.lastResult("Success");
          } else if (context.getArguments() instanceof Boolean) {
            resultSender.lastResult(Boolean.TRUE);
          }
        }

        @Override
        public String getId() {
          return getClass().getName();
        }

        @Override
        public boolean hasResult() {
          return true;
        }
      });
      assertEquals(Boolean.TRUE, ((List) rs.getResult()).get(0));

    } catch (Exception ex) {
      ex.printStackTrace();
      logger.info("Exception : ", ex);
      fail("Test failed after the execute operation nn TRUE");
    }
  }

  private static void serverExecution_Inline_InvalidAttributes() {

    DistributedSystem.setThreadsSocketPolicy(false);
    Execution member = FunctionService.onServer(pool);

    try {
      member.setArguments(Boolean.TRUE).execute(new FunctionAdapter() {
        @Override
        public void execute(FunctionContext context) {
          @SuppressWarnings("unchecked")
          final ResultSender<Object> resultSender = context.getResultSender();
          if (context.getArguments() instanceof String) {
            resultSender.lastResult("Success");
          } else if (context.getArguments() instanceof Boolean) {
            resultSender.lastResult(Boolean.TRUE);
          }
        }

        @Override
        public String getId() {
          return getClass().getName();
        }

        @Override
        public boolean hasResult() {
          return false;
        }

        @Override
        public boolean isHA() {
          return true;
        }
      });

      fail("Should have failed with Invalid attributes.");

    } catch (Exception ex) {
      logger.info("Exception : ", ex);
      assertTrue(
          ex.getMessage().contains("For Functions with isHA true, hasResult must also be true."));
    }
  }


  private static void allServerExecution(Boolean isByName, Function function, Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);
    if (toRegister) {
      FunctionService.registerFunction(function);
    } else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }
    Execution member = FunctionService.onServers(pool);
    try {
      ResultCollector rs = execute(member, Boolean.TRUE, function, isByName);

      List resultList = (List) rs.getResult();
      assertEquals(Boolean.TRUE, resultList.get(0));
      assertEquals(Boolean.TRUE, resultList.get(1));
      assertEquals(Boolean.TRUE, resultList.get(2));

    } catch (Exception ex) {
      ex.printStackTrace();
      logger.info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }

    try {

      final HashSet<String> testKeysSet = new HashSet<>();
      for (int i = 0; i < 20; i++) {
        testKeysSet.add("execKey-" + i);
      }

      ResultCollector rs = execute(member, testKeysSet, function, isByName);
      List resultList = (List) rs.getResult();
      assertEquals(3, resultList.size());

      for (int j = 0; j < 3; j++) {
        for (int k = 0; k < 20; k++) {
          assertTrue((((List) (resultList).get(j)).contains("execKey-" + k)));
        }
      }

    } catch (Exception ex) {
      ex.printStackTrace();
      logger.info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }

  }

  private static void allServerExecution_SendException(Boolean isByName, Function function,
      Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);
    if (toRegister) {
      FunctionService.registerFunction(function);
    } else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }
    Execution member = FunctionService.onServers(pool);
    try {
      ResultCollector rs = execute(member, Boolean.TRUE, function, isByName);

      List resultList = (List) rs.getResult();
      assertTrue(resultList.get(0) instanceof MyFunctionExecutionException);
      assertTrue(resultList.get(1) instanceof MyFunctionExecutionException);
      assertTrue(resultList.get(2) instanceof MyFunctionExecutionException);

    } catch (Exception ex) {
      ex.printStackTrace();
      logger.info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }

    try {
      final HashSet<String> testKeysSet = new HashSet<>();
      for (int i = 0; i < 20; i++) {
        testKeysSet.add("execKey-" + i);
      }

      ResultCollector rs = execute(member, testKeysSet, function, isByName);
      List resultList = (List) rs.getResult();
      assertEquals(((testKeysSet.size() * 3) + 3), resultList.size());
      Iterator resultIterator = resultList.iterator();
      int exceptionCount = 0;
      while (resultIterator.hasNext()) {
        Object o = resultIterator.next();
        if (o instanceof MyFunctionExecutionException) {
          exceptionCount++;
        }
      }
      assertEquals(3, exceptionCount);

    } catch (Exception ex) {
      ex.printStackTrace();
      logger.info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }

  }

  private static void allServerExecution_NoLastResult(Boolean isByName, Function function,
      Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);
    if (toRegister) {
      FunctionService.registerFunction(function);
    } else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }
    Execution member = FunctionService.onServers(pool);
    try {
      ResultCollector rs = execute(member, Boolean.TRUE, function, isByName);
      rs.getResult();
      fail("Expected FunctionException : Function did not send last result");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("did not send last result"));
    }
  }

  private static void allServerExecution_Inline() {
    DistributedSystem.setThreadsSocketPolicy(false);
    Execution member = FunctionService.onServers(pool);
    try {
      ResultCollector rs = member.setArguments(Boolean.TRUE).execute(new FunctionAdapter() {
        @Override
        public void execute(FunctionContext context) {
          @SuppressWarnings("unchecked")
          final ResultSender<Object> resultSender = context.getResultSender();
          if (context.getArguments() instanceof String) {
            resultSender.lastResult("Success");
          } else if (context.getArguments() instanceof Boolean) {
            resultSender.lastResult(Boolean.TRUE);
          }
        }

        @Override
        public String getId() {
          return getClass().getName();
        }

        @Override
        public boolean hasResult() {
          return true;
        }
      });
      List resultList = (List) rs.getResult();
      assertEquals(Boolean.TRUE, resultList.get(0));
      assertEquals(Boolean.TRUE, resultList.get(1));
      assertEquals(Boolean.TRUE, resultList.get(2));

    } catch (Exception ex) {
      ex.printStackTrace();
      logger.info("Exception : ", ex);
      fail("Test failed after the execute operation asdfasdfa   ");
    }
  }

  private static ResultCollector execute(Execution member, Serializable args, Function function,
      Boolean isByName) {
    if (isByName) {// by name
      logger.info("The function name to execute : " + function.getId());
      Execution me = member.setArguments(args);
      logger.info("The args passed  : " + args);
      return me.execute(function.getId());
    } else { // By Instance
      return member.setArguments(args).execute(function);
    }
  }
}
