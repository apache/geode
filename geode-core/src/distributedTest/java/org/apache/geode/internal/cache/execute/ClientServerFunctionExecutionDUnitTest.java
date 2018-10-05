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

import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.FunctionServiceTest;

@Category({ClientServerTest.class, FunctionServiceTest.class})
public class ClientServerFunctionExecutionDUnitTest extends PRClientServerTestBase {
  private static final String TEST_FUNCTION1 = TestFunction.TEST_FUNCTION1;

  Boolean isByName = null;
  Function function = null;
  Boolean toRegister = null;
  static final String retryRegionName = "RetryDataRegion";
  static Region metaDataRegion;

  public ClientServerFunctionExecutionDUnitTest() {
    super();
  }

  @Override
  protected final void postSetUpPRClientServerTestBase() throws Exception {
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
  public void test_Bug_43126_Function_Not_Registered() throws InterruptedException {
    createScenario();
    try {
      client.invoke(() -> ClientServerFunctionExecutionDUnitTest.executeRegisteredFunction());
    } catch (Exception e) {
      assertEquals(true, (e.getCause() instanceof ServerOperationException));
      assertTrue(
          e.getCause().getMessage().contains("The function is not registered for function id"));
    }
  }

  @Test
  public void test_Bug43126() throws InterruptedException {
    createScenario();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
    registerFunctionAtServer(function);
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.executeRegisteredFunction());
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
    isByName = new Boolean(true);
    toRegister = new Boolean(true);
    LogWriterUtils.getLogWriter().info(
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
    isByName = new Boolean(true);
    toRegister = new Boolean(true);
    LogWriterUtils.getLogWriter().info(
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
    isByName = new Boolean(true);
    toRegister = new Boolean(true);
    LogWriterUtils.getLogWriter().info(
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
    isByName = new Boolean(true);
    toRegister = new Boolean(false);
    LogWriterUtils.getLogWriter().info(
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
    LogWriterUtils.getLogWriter().info(
        "ClientServerFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.serverExecution_Inline());
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.allServerExecution_Inline());
  }


  /*
   * Execution of the inline function on server
   */
  @Test
  public void testServerExecution_byInlineFunction_InvalidAttrbiutes() {
    createScenario();
    LogWriterUtils.getLogWriter().info(
        "ClientServerFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(
        () -> ClientServerFunctionExecutionDUnitTest.serverExecution_Inline_InvalidAttributes());
  }

  /*
   * Execution of the inline function on server
   */
  @Test
  public void testBug40714() {
    createScenario();
    LogWriterUtils.getLogWriter()
        .info("ClientServerFunctionExecutionDUnitTest#testBug40714 : Starting test");

    server1.invoke(() -> ClientServerFunctionExecutionDUnitTest.registerFunction());
    server1.invoke(() -> ClientServerFunctionExecutionDUnitTest.registerFunction());
    server1.invoke(() -> ClientServerFunctionExecutionDUnitTest.registerFunction());
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.registerFunction());
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.FunctionExecution_Inline_Bug40714());

  }

  public static void registerFunction() {
    FunctionService.registerFunction(new FunctionAdapter() {
      public void execute(FunctionContext context) {
        if (context.getArguments() instanceof String) {
          context.getResultSender().lastResult("Failure");
        } else if (context.getArguments() instanceof Boolean) {
          context.getResultSender().lastResult(Boolean.FALSE);
        }
      }

      public String getId() {
        return "Function";
      }

      public boolean hasResult() {
        return true;
      }
    });
  }

  public static void FunctionExecution_Inline_Bug40714() {
    DistributedSystem.setThreadsSocketPolicy(false);
    Execution member = FunctionService.onServers(pool);
    try {
      ResultCollector rs = member.setArguments(Boolean.TRUE).execute(new FunctionAdapter() {
        public void execute(FunctionContext context) {
          if (context.getArguments() instanceof String) {
            context.getResultSender().lastResult("Success");
          } else if (context.getArguments() instanceof Boolean) {
            context.getResultSender().lastResult(Boolean.TRUE);
          }
        }

        public String getId() {
          return "Function";
        }

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
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
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
    isByName = new Boolean(true);
    toRegister = new Boolean(true);
    LogWriterUtils.getLogWriter().info(
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
    isByName = new Boolean(true);
    toRegister = new Boolean(false);
    LogWriterUtils.getLogWriter().info(
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
  public void testOnServerFailoverWithOneServerDownHA() throws InterruptedException {
    // The test code appears to trigger this because the first
    // call to the function disconnects from the DS but does not call
    // last result;
    IgnoredException.addIgnoredException("did not send last result");
    createScenario();

    server1.invoke(() -> ClientServerFunctionExecutionDUnitTest.createReplicatedRegion());

    server2.invoke(() -> ClientServerFunctionExecutionDUnitTest.createReplicatedRegion());

    server3.invoke(() -> ClientServerFunctionExecutionDUnitTest.createReplicatedRegion());

    client.invoke(() -> ClientServerFunctionExecutionDUnitTest
        .createProxyRegion(NetworkUtils.getServerHostName(server1.getHost())));

    function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA_SERVER);
    registerFunctionAtServer(function);

    client.invoke(() -> ClientServerFunctionExecutionDUnitTest
        .serverExecutionHAOneServerDown(Boolean.FALSE, function, Boolean.FALSE));

    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.verifyMetaData(new Integer(1),
        new Integer(1)));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testOnServerFailoverWithTwoServerDownHA() throws InterruptedException {
    // The test code appears to trigger this because the first
    // call to the function disconnects from the DS but does not call
    // last result;
    IgnoredException.addIgnoredException("Socket Closed");
    IgnoredException.addIgnoredException("did not send last result");
    createScenario();

    server1.invoke(() -> ClientServerFunctionExecutionDUnitTest.createReplicatedRegion());

    server2.invoke(() -> ClientServerFunctionExecutionDUnitTest.createReplicatedRegion());

    server3.invoke(() -> ClientServerFunctionExecutionDUnitTest.createReplicatedRegion());

    client.invoke(() -> ClientServerFunctionExecutionDUnitTest
        .createProxyRegion(NetworkUtils.getServerHostName(server1.getHost())));

    function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA_SERVER);
    registerFunctionAtServer(function);

    client.invoke(() -> ClientServerFunctionExecutionDUnitTest
        .serverExecutionHATwoServerDown(Boolean.FALSE, function, Boolean.FALSE));

    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.verifyMetaData(new Integer(2),
        new Integer(0)));
  }


  /*
   * Ensure that the while executing the function if the servers are down then the execution
   * shouldn't failover to other available server
   */
  @Test
  public void testOnServerFailoverNonHA() throws InterruptedException {
    // The test code appears to trigger this because the first
    // call to the function disconnects from the DS but does not call
    // last result;
    IgnoredException.addIgnoredException("did not send last result");
    createScenario();
    server1.invoke(() -> ClientServerFunctionExecutionDUnitTest.createReplicatedRegion());

    server2.invoke(() -> ClientServerFunctionExecutionDUnitTest.createReplicatedRegion());

    server3.invoke(() -> ClientServerFunctionExecutionDUnitTest.createReplicatedRegion());

    client.invoke(() -> ClientServerFunctionExecutionDUnitTest
        .createProxyRegion(NetworkUtils.getServerHostName(server1.getHost())));

    function = new TestFunction(true, TestFunction.TEST_FUNCTION_NONHA_SERVER);
    registerFunctionAtServer(function);

    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.serverExecutionNonHA(Boolean.FALSE,
        function, Boolean.FALSE));
    client.invoke(() -> ClientServerFunctionExecutionDUnitTest.verifyMetaData(new Integer(1),
        new Integer(0)));
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


  private void createScenario() {
    LogWriterUtils.getLogWriter()
        .info("ClientServerFFunctionExecutionDUnitTest#createScenario : creating scenario");
    createClientServerScenarionWithoutRegion();
  }

  public static void serverExecution(Boolean isByName, Function function, Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister.booleanValue()) {
      FunctionService.registerFunction(function);
    }
    Execution member = FunctionService.onServer(pool);

    try {
      ResultCollector rs = execute(member, Boolean.TRUE, function, isByName);
      assertEquals(Boolean.TRUE, ((List) rs.getResult()).get(0));

    } catch (Exception ex) {
      ex.printStackTrace();
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }

    try {
      final HashSet testKeysSet = new HashSet();
      for (int i = 0; i < 20; i++) {
        testKeysSet.add("execKey-" + i);
      }

      ResultCollector rs = execute(member, testKeysSet, function, isByName);

      List resultList = (List) ((List) rs.getResult());
      for (int i = 0; i < 20; i++) {
        assertEquals(true, ((List) (resultList.get(0))).contains("execKey-" + i));
      }

    } catch (Exception ex) {
      ex.printStackTrace();
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operations");
    }
  }


  public static void executeRegisteredFunction() {
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


  public static void serverExecution_SendException(Boolean isByName, Function function,
      Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister.booleanValue()) {
      FunctionService.registerFunction(function);
    }
    Execution member = FunctionService.onServer(pool);

    try {
      ResultCollector rs = execute(member, Boolean.TRUE, function, isByName);
      assertTrue(((List) rs.getResult()).get(0) instanceof MyFunctionExecutionException);

    } catch (Exception ex) {
      ex.printStackTrace();
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }

    try {
      final HashSet testKeysSet = new HashSet();
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
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operations");
    }
  }

  public static void createReplicatedRegion() {
    metaDataRegion = cache.createRegionFactory(RegionShortcut.REPLICATE).create(retryRegionName);
  }

  public static void createProxyRegion(String hostName) {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(pool.getName());
    RegionAttributes attrs = factory.create();
    metaDataRegion = cache.createRegion(retryRegionName, attrs);
    assertNotNull(metaDataRegion);
  }

  public static void verifyMetaData(Integer arg1, Integer arg2) {
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

  public static void verifyDeadAndLiveServers(final Integer expectedDeadServers,
      final Integer expectedLiveServers) {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;

      public boolean done() {
        int sz = pool.getConnectedServerCount();
        getLogWriter().info("Checking for the Live Servers : Expected  : "
            + expectedLiveServers + " Available :" + sz);
        if (sz == expectedLiveServers.intValue()) {
          return true;
        }
        excuse = "Expected " + expectedLiveServers.intValue() + " but found " + sz;
        return false;
      }

      public String description() {
        return excuse;
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
  }

  public static Object serverExecutionHAOneServerDown(Boolean isByName, Function function,
      Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister.booleanValue()) {
      FunctionService.registerFunction(function);
    }
    Execution member = FunctionService.onServer(pool);
    ResultCollector rs = null;
    try {
      ArrayList<String> args = new ArrayList<String>();
      args.add(retryRegionName);
      args.add("serverExecutionHAOneServerDown");
      rs = execute(member, args, function, isByName);
      assertEquals(retryRegionName, ((List) rs.getResult()).get(0));
    } catch (Exception ex) {
      ex.printStackTrace();
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }
    return rs.getResult();
  }

  public static void serverExecutionHATwoServerDown(Boolean isByName, Function function,
      Boolean toRegister) {
    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister.booleanValue()) {
      FunctionService.registerFunction(function);
    }
    Execution member = FunctionService.onServer(pool);
    try {
      ArrayList<String> args = new ArrayList<String>();
      args.add(retryRegionName);
      args.add("serverExecutionHATwoServerDown");
      ResultCollector rs = execute(member, args, function, isByName);
      fail("Expected ServerConnectivityException not thrown!");
    } catch (Exception ex) {
      if (!(ex instanceof ServerConnectivityException)) {
        ex.printStackTrace();
        LogWriterUtils.getLogWriter().info("Exception : ", ex);
        fail("Test failed after the execute operation");
      }
    }
  }

  public static Object serverExecutionNonHA(Boolean isByName, Function function,
      Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister.booleanValue()) {
      FunctionService.registerFunction(function);
    }
    Execution member = FunctionService.onServer(pool);
    try {
      ArrayList<String> args = new ArrayList<String>();
      args.add(retryRegionName);
      args.add("serverExecutionNonHA");
      ResultCollector rs = execute(member, args, function, isByName);
      fail("Expected ServerConnectivityException not thrown!");
    } catch (Exception ex) {
      if (!(ex instanceof ServerConnectivityException)) {
        ex.printStackTrace();
        LogWriterUtils.getLogWriter().info("Exception : ", ex);
        fail("Test failed after the execute operation");
      }
    }
    return null;
  }

  @SuppressWarnings("rawtypes")
  public static void serverFunctionExecution_FunctionInvocationTargetException(Boolean isByName,
      Function function, Boolean toRegister) {
    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister.booleanValue()) {
      FunctionService.registerFunction(function);
    }
    Execution member = FunctionService.onServer(pool);
    try {
      ResultCollector rs = execute(member, Boolean.TRUE, function, isByName);
      ArrayList list = (ArrayList) rs.getResult();
      assertTrue("Value of send result of the executed function : " + list.get(0)
          + "does not match the expected value : " + 1, ((Integer) list.get(0)) == 1);
      assertTrue("Value of last result of the executed function : " + list.get(0)
          + "is not equal or more than expected value : " + 5, ((Integer) list.get(1)) >= 5);
    } catch (Exception ex) {
      ex.printStackTrace();
      Assert.fail("This is not expected Exception", ex);
    }
  }

  public static void serverExecution_NoLastResult(Boolean isByName, Function function,
      Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister.booleanValue()) {
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

  public static void serverExecution_Inline() {

    DistributedSystem.setThreadsSocketPolicy(false);
    Execution member = FunctionService.onServer(pool);

    try {
      ResultCollector rs = member.setArguments(Boolean.TRUE).execute(new FunctionAdapter() {
        public void execute(FunctionContext context) {
          if (context.getArguments() instanceof String) {
            context.getResultSender().lastResult("Success");
          } else if (context.getArguments() instanceof Boolean) {
            context.getResultSender().lastResult(Boolean.TRUE);
          }
        }

        public String getId() {
          return getClass().getName();
        }

        public boolean hasResult() {
          return true;
        }
      });
      assertEquals(Boolean.TRUE, ((List) rs.getResult()).get(0));

    } catch (Exception ex) {
      ex.printStackTrace();
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operation nn TRUE");
    }
  }

  public static void serverExecution_Inline_InvalidAttributes() {

    DistributedSystem.setThreadsSocketPolicy(false);
    Execution member = FunctionService.onServer(pool);

    try {
      ResultCollector rs = member.setArguments(Boolean.TRUE).execute(new FunctionAdapter() {
        public void execute(FunctionContext context) {
          if (context.getArguments() instanceof String) {
            context.getResultSender().lastResult("Success");
          } else if (context.getArguments() instanceof Boolean) {
            context.getResultSender().lastResult(Boolean.TRUE);
          }
        }

        public String getId() {
          return getClass().getName();
        }

        public boolean hasResult() {
          return false;
        }

        public boolean isHA() {
          return true;
        }
      });

      fail("Should have failed with Invalid attributes.");

    } catch (Exception ex) {
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      assertTrue(
          ex.getMessage().contains("For Functions with isHA true, hasResult must also be true."));
    }
  }


  public static void allServerExecution(Boolean isByName, Function function, Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);
    if (toRegister.booleanValue()) {
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
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }

    try {

      final HashSet testKeysSet = new HashSet();
      for (int i = 0; i < 20; i++) {
        testKeysSet.add("execKey-" + i);
      }

      ResultCollector rs = execute(member, testKeysSet, function, isByName);
      List resultList = (List) rs.getResult();
      assertEquals(3, resultList.size());

      for (int j = 0; j < 3; j++) {
        for (int k = 0; k < 20; k++) {
          assertEquals(true, (((List) (resultList).get(j)).contains("execKey-" + k)));
        }
      }

    } catch (Exception ex) {
      ex.printStackTrace();
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }

  }

  public static void allServerExecution_SendException(Boolean isByName, Function function,
      Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);
    if (toRegister.booleanValue()) {
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
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }

    try {
      final HashSet testKeysSet = new HashSet();
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
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }

  }

  public static void allServerExecution_NoLastResult(Boolean isByName, Function function,
      Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);
    if (toRegister.booleanValue()) {
      FunctionService.registerFunction(function);
    } else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }
    Execution member = FunctionService.onServers(pool);
    try {
      ResultCollector rs = execute(member, Boolean.TRUE, function, isByName);
      List resultList = (List) rs.getResult();
      fail("Expected FunctionException : Function did not send last result");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("did not send last result"));
    }
  }

  public static void allServerExecution_Inline() {
    DistributedSystem.setThreadsSocketPolicy(false);
    Execution member = FunctionService.onServers(pool);
    try {
      ResultCollector rs = member.setArguments(Boolean.TRUE).execute(new FunctionAdapter() {
        public void execute(FunctionContext context) {
          if (context.getArguments() instanceof String) {
            context.getResultSender().lastResult("Success");
          } else if (context.getArguments() instanceof Boolean) {
            context.getResultSender().lastResult(Boolean.TRUE);
          }
        }

        public String getId() {
          return getClass().getName();
        }

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
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operation asdfasdfa   ");
    }
  }

  private static ResultCollector execute(Execution member, Serializable args, Function function,
      Boolean isByName) throws Exception {
    if (isByName.booleanValue()) {// by name
      LogWriterUtils.getLogWriter().info("The function name to execute : " + function.getId());
      Execution me = member.setArguments(args);
      LogWriterUtils.getLogWriter().info("The args passed  : " + args);
      return me.execute(function.getId());
    } else { // By Instance
      return member.setArguments(args).execute(function);
    }
  }
}
