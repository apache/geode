/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.execute;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.functions.TestFunction;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

public class ClientServerFunctionExecutionDUnitTest extends PRClientServerTestBase {
  private static final String TEST_FUNCTION1 = TestFunction.TEST_FUNCTION1;

  static Boolean isByName = null;
  static Function function = null;
  static Boolean toRegister = null;
  static final String retryRegionName = "RetryDataRegion";
  static Region metaDataRegion;
  
  public ClientServerFunctionExecutionDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    addExpectedException("java.net.ConnectException");
  }

  
  public void test_Bug_43126_Function_Not_Registered()
      throws InterruptedException {
    createScenario();
    try {
      client.invoke(ClientServerFunctionExecutionDUnitTest.class,
          "executeRegisteredFunction");
    }
    catch (Exception e) {
      assertEquals(true, (e.getCause() instanceof ServerOperationException));
      assertTrue(e.getCause().getMessage().contains(
          "The function is not registered for function id"));
    }
  }

  public void test_Bug43126() throws InterruptedException {
    createScenario();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
    registerFunctionAtServer(function);
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "executeRegisteredFunction");
  }

  /*
   * Execution of the function on server using the name of the function
   */   
  public void testServerExecution_byName() {
    createScenario();

    //function = new TestFunction1();
    function = new TestFunction(true,TEST_FUNCTION1);

    registerFunctionAtServer(function);
    isByName = new Boolean(true);   
    toRegister = new Boolean(true);
    getLogWriter().info("ClientServerFFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "serverExecution", new Object[] { isByName, function, toRegister});
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "allServerExecution", new Object[] { isByName, function, toRegister});
  }
  
  
  public void testServerExecution_sendException() {
    createScenario();

    //function = new TestFunction1();
    function = new TestFunction(true,TestFunction.TEST_FUNCTION_SEND_EXCEPTION);

    registerFunctionAtServer(function);
    isByName = new Boolean(true);   
    toRegister = new Boolean(true);
    getLogWriter().info("ClientServerFFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "serverExecution_SendException", new Object[] { isByName, function, toRegister});
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "allServerExecution_SendException", new Object[] { isByName, function, toRegister});
  }
  
  /*
   * Execution of the function on server using the name of the function
   */   
  public void testServerExecution_NoLastResult() {
    createScenario();

    //function = new TestFunction1();
    function = new TestFunction(true,TestFunction.TEST_FUNCTION_NO_LASTRESULT);

    registerFunctionAtServer(function);
    isByName = new Boolean(true);   
    toRegister = new Boolean(true);
    getLogWriter().info("ClientServerFFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "serverExecution_NoLastResult", new Object[] { isByName, function, toRegister});
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "allServerExecution_NoLastResult", new Object[] { isByName, function, toRegister});
  }

  public void testServerExecution_byName_WithoutRegister() {
    createScenario();

    //function = new TestFunction1();
    function = new TestFunction(true,TEST_FUNCTION1);

    registerFunctionAtServer(function);
    isByName = new Boolean(true);  
    toRegister = new Boolean(false);
    getLogWriter().info("ClientServerFFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "serverExecution", new Object[] { isByName, function, toRegister});
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "allServerExecution", new Object[] { isByName, function, toRegister});
  }
  /*
   * Execution of the inline function on server 
   */   
  public void testServerExecution_byInlineFunction() {
    createScenario();
    getLogWriter().info("ClientServerFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "serverExecution_Inline");
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "allServerExecution_Inline");
  }
  
  
  /*
   * Execution of the inline function on server 
   */   
  public void testServerExecution_byInlineFunction_InvalidAttrbiutes() {
    createScenario();
    getLogWriter().info("ClientServerFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "serverExecution_Inline_InvalidAttributes");
  }
  
  /*
   * Execution of the inline function on server
   */
  public void testBug40714() {
    createScenario();
    getLogWriter()
        .info(
            "ClientServerFunctionExecutionDUnitTest#testBug40714 : Starting test");

    server1.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "registerFunction");
    server1.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "registerFunction");
    server1.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "registerFunction");
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "registerFunction");
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "FunctionExecution_Inline_Bug40714");

  }

  public static void registerFunction() {
    FunctionService.registerFunction(new FunctionAdapter() {
      public void execute(FunctionContext context) {
        if (context.getArguments() instanceof String) {
          context.getResultSender().lastResult("Failure");
        }
        else if (context.getArguments() instanceof Boolean) {
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
      ResultCollector rs = member.withArgs(Boolean.TRUE).execute(
          new FunctionAdapter() {
            public void execute(FunctionContext context) {
              if (context.getArguments() instanceof String) {
                context.getResultSender().lastResult("Success");
              }
              else if (context.getArguments() instanceof Boolean) {
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
      List resultList = (List)rs.getResult();
      assertEquals(3, resultList.size());
      assertEquals(Boolean.TRUE, resultList.get(0));
      assertEquals(Boolean.TRUE, resultList.get(1));
      assertEquals(Boolean.TRUE, resultList.get(2));

    }
    catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operation.");
    }
  }
  
  /*
   * Execution of the function on server using the name of the function
   */   
  public void testServerExecution_SocketTimeOut() {
    createScenario();
    function = new TestFunction(true,TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);

    registerFunctionAtServer(function);
    isByName = new Boolean(true);
    toRegister = new Boolean(true);    
    getLogWriter().info("ClientServerFFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "serverExecution", new Object[] { isByName, function, toRegister});
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "allServerExecution", new Object[] { isByName, function, toRegister});
  }

  public void testServerExecution_SocketTimeOut_WithoutRegister() {
    createScenario();
    function = new TestFunction(true,TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);

    registerFunctionAtServer(function);
    isByName = new Boolean(true);
    toRegister = new Boolean(false);
    getLogWriter().info("ClientServerFFunctionExecutionDUnitTest#testServerSingleKeyExecution_byName : Starting test");
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "serverExecution", new Object[] { isByName, function, toRegister});
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "allServerExecution", new Object[] { isByName, function, toRegister});
  }
  
  
  /*
   * Ensure that the while executing the function if the servers is down then 
   * the execution is failover to other available server
   */
  @SuppressWarnings("rawtypes")
  public void testOnServerFailoverWithOneServerDownHA()
      throws InterruptedException {
    //The test code appears to trigger this because the first
    //call to the function disconnects from the DS but does not call
    //last result;
    addExpectedException("did not send last result");
    createScenario();
    
    server1.invoke(
        ClientServerFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");
    
    server2.invoke(
        ClientServerFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");
    
    server3.invoke(
        ClientServerFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");
    
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "createProxyRegion",
        new Object[] { getServerHostName(server1.getHost()) });
    
    function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA_SERVER);
    registerFunctionAtServer(function);
    
    client.invoke(
        ClientServerFunctionExecutionDUnitTest.class,
        "serverExecutionHAOneServerDown", new Object[]{Boolean.FALSE,function,Boolean.FALSE});

    client.invoke(
        ClientServerFunctionExecutionDUnitTest.class,
        "verifyMetaData",new Object[]{new Integer(1), new Integer(1)});
  }

  @SuppressWarnings("rawtypes")
  public void testOnServerFailoverWithTwoServerDownHA()
      throws InterruptedException {
    //The test code appears to trigger this because the first
    //call to the function disconnects from the DS but does not call
    //last result;
    addExpectedException("Socket Closed");
    addExpectedException("did not send last result");
    createScenario();
    
    server1.invoke(
        ClientServerFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");
    
    server2.invoke(
        ClientServerFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");
    
    server3.invoke(
        ClientServerFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");
    
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "createProxyRegion",
        new Object[] { getServerHostName(server1.getHost()) });
    
    function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA_SERVER);
    registerFunctionAtServer(function);
    
    client.invoke(
        ClientServerFunctionExecutionDUnitTest.class,
        "serverExecutionHATwoServerDown", new Object[]{Boolean.FALSE,function,Boolean.FALSE});

    client.invoke(
        ClientServerFunctionExecutionDUnitTest.class,
        "verifyMetaData",new Object[]{new Integer(2), new Integer(0)});
  }

  
  /*
   * Ensure that the while executing the function if the servers are down then 
   * the execution shouldn't failover to other available server
   */
  public void testOnServerFailoverNonHA()
      throws InterruptedException {
    //The test code appears to trigger this because the first
    //call to the function disconnects from the DS but does not call
    //last result;
    addExpectedException("did not send last result");
    createScenario();
    server1.invoke(
        ClientServerFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");
    
    server2.invoke(
        ClientServerFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");
    
    server3.invoke(
        ClientServerFunctionExecutionDUnitTest.class,
        "createReplicatedRegion");
    
    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "createProxyRegion",
        new Object[] { getServerHostName(server1.getHost()) });
    
    function = new TestFunction(true, TestFunction.TEST_FUNCTION_NONHA_SERVER);
    registerFunctionAtServer(function);
    
    client.invoke(
        ClientServerFunctionExecutionDUnitTest.class,
        "serverExecutionNonHA", new Object[]{Boolean.FALSE,function,Boolean.FALSE});
    client.invoke(
        ClientServerFunctionExecutionDUnitTest.class,
        "verifyMetaData",new Object[]{new Integer(1), new Integer(0)});
  }

  
  /*
   * Execution of the function on a server.Function throws the FunctionInvocationTargetException. 
   * As this is the case of HA then system should retry the function execution. After 5th attempt
   * function will send Boolean as last result.
   */
  public void testOnServerExecution_FunctionInvocationTargetException() {
    createScenario();
    function = new TestFunction(true, TestFunction.TEST_FUNCTION_ONSERVER_REEXECUTE_EXCEPTION);
    registerFunctionAtServer(function);

    client.invoke(ClientServerFunctionExecutionDUnitTest.class,
        "serverFunctionExecution_FunctionInvocationTargetException",
        new Object[] { Boolean.FALSE, function, Boolean.FALSE });
  }
  
  
  private void createScenario() {
    getLogWriter().info("ClientServerFFunctionExecutionDUnitTest#createScenario : creating scenario");
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
      assertEquals(Boolean.TRUE, ((List)rs.getResult()).get(0));
      
    }catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : " , ex);
      fail("Test failed after the execute operation");
    }
    
    try {
      final HashSet testKeysSet = new HashSet();
      for (int i = 0; i <20; i++) {
        testKeysSet.add("execKey-" + i);
      }
      
      ResultCollector rs = execute(member, testKeysSet,
          function, isByName);
                  
      List resultList = (List)((List)rs.getResult());
      for (int i = 0; i < 20; i++) {
        assertEquals(true, ((List)(resultList.get(0))).contains("execKey-" + i));
      }
      
    }catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : " , ex);
      fail("Test failed after the execute operations");
    }
  }
  
  
  public static void executeRegisteredFunction() {
    DistributedSystem.setThreadsSocketPolicy(false);
    Execution member = FunctionService.onServer(pool);
    
    // remove any existing attributes
    ((AbstractExecution)member)
        .removeFunctionAttributes(TestFunction.TEST_FUNCTION1);
    ResultCollector rs = member.withArgs(Boolean.TRUE).execute(
        TestFunction.TEST_FUNCTION1);
    assertEquals(Boolean.TRUE, ((List)rs.getResult()).get(0));
    byte[] functionAttributes = ((AbstractExecution)member)
        .getFunctionAttributes(TestFunction.TEST_FUNCTION1);
    assertNotNull(functionAttributes);
  }
  
  
  public static void serverExecution_SendException(Boolean isByName, Function function, Boolean toRegister) {
    
    DistributedSystem.setThreadsSocketPolicy(false);
    
    if (toRegister.booleanValue()) {
      FunctionService.registerFunction(function);
    }
    Execution member = FunctionService.onServer(pool);
        
    try {
      ResultCollector rs = execute(member, Boolean.TRUE, function, isByName);
      assertTrue(((List)rs.getResult()).get(0) instanceof MyFunctionExecutionException);
      
    }catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : " , ex);
      fail("Test failed after the execute operation");
    }
    
    try {
      final HashSet testKeysSet = new HashSet();
      for (int i = 0; i <20; i++) {
        testKeysSet.add("execKey-" + i);
      }
      
      ResultCollector rs = execute(member, testKeysSet,
          function, isByName);
                  
      List resultList = (List)rs.getResult();
      assertEquals((testKeysSet.size()+1), resultList.size());
      Iterator resultIterator = resultList.iterator();
      int exceptionCount = 0;
      while(resultIterator.hasNext()){
        Object o = resultIterator.next();
        if(o instanceof MyFunctionExecutionException){
          exceptionCount++;
        }
      }
      assertEquals(1, exceptionCount);
      
    }catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : " , ex);
      fail("Test failed after the execute operations");
    }
  }
  
  public static void createReplicatedRegion(){
    metaDataRegion = cache.createRegionFactory(RegionShortcut.REPLICATE).create(retryRegionName);
  }
  
  public static void createProxyRegion(String hostName){
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
      final Integer expectedLiveServers){
    WaitCriterion wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        int sz = pool.getConnectedServerCount();
        getLogWriter().info(
            "Checking for the Live Servers : Expected  : " + expectedLiveServers
                + " Available :" + sz);
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
    DistributedTestCase.waitForCriterion(wc, 3 * 60 * 1000, 1000, true);
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
      assertEquals(retryRegionName, ((List)rs.getResult()).get(0));
    } catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }
    return rs.getResult();
  }

  public static void serverExecutionHATwoServerDown(Boolean isByName, Function function,
      Boolean toRegister){
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
        getLogWriter().info("Exception : ", ex);
        fail("Test failed after the execute operation");
      }
    }
  }
  
  public static Object serverExecutionNonHA(Boolean isByName,
      Function function, Boolean toRegister) {

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
        getLogWriter().info("Exception : ", ex);
        fail("Test failed after the execute operation");
      }
    }
    return null;
  }

  @SuppressWarnings("rawtypes")
  public static void serverFunctionExecution_FunctionInvocationTargetException(
      Boolean isByName, Function function, Boolean toRegister) {
    DistributedSystem.setThreadsSocketPolicy(false);
    
    if (toRegister.booleanValue()) {
      FunctionService.registerFunction(function);
    }
    Execution member = FunctionService.onServer(pool);
    try {
      ResultCollector rs = execute(member, Boolean.TRUE, function, isByName);
      ArrayList list = (ArrayList)rs.getResult();
      assertTrue(((Integer)list.get(0)) == 1);
      assertTrue(((Integer)list.get(1)) == 5);
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("This is not expected Exception", ex);
    }
  }

  public static void serverExecution_NoLastResult(Boolean isByName, Function function, Boolean toRegister) {
    
    DistributedSystem.setThreadsSocketPolicy(false);
    
    if (toRegister.booleanValue()) {
      FunctionService.registerFunction(function);
    }
    Execution member = FunctionService.onServer(pool);
        
    try {
      ResultCollector rs = execute(member, Boolean.TRUE, function, isByName);
      assertEquals(Boolean.TRUE, ((List)rs.getResult()).get(0));
      fail("Expected FunctionException : Function did not send last result");
    }catch (Exception ex) {
      assertTrue(ex.getMessage().contains("did not send last result"));
    }
    
    
  }
  
  public static void serverExecution_Inline() {
    
    DistributedSystem.setThreadsSocketPolicy(false);
    Execution member = FunctionService.onServer(pool);
        
    try {
      ResultCollector rs = member.withArgs(Boolean.TRUE).execute(new FunctionAdapter(){
        public void execute(FunctionContext context) {
          if (context.getArguments() instanceof String) {
            context.getResultSender().lastResult( "Success");
          }else if(context.getArguments() instanceof Boolean){
            context.getResultSender().lastResult( Boolean.TRUE);
          }
        }

        public String getId() {
          return getClass().getName();
        }

        public boolean hasResult() {
          return true;
        }
      });      
      assertEquals(Boolean.TRUE, ((List)rs.getResult()).get(0));
      
    }catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : " , ex);
      fail("Test failed after the execute operation nn TRUE");
    }
  }

public static void serverExecution_Inline_InvalidAttributes() {
    
    DistributedSystem.setThreadsSocketPolicy(false);
    Execution member = FunctionService.onServer(pool);
        
    try {
      ResultCollector rs = member.withArgs(Boolean.TRUE).execute(new FunctionAdapter(){
        public void execute(FunctionContext context) {
          if (context.getArguments() instanceof String) {
            context.getResultSender().lastResult( "Success");
          }else if(context.getArguments() instanceof Boolean){
            context.getResultSender().lastResult( Boolean.TRUE);
          }
        }

        public String getId() {
          return getClass().getName();
        }

        public boolean hasResult() {
          return false;
        }
        public boolean isHA(){
          return true;
        }
      });      
      
      fail("Should have failed with Invalid attributes.");
      
    }catch (Exception ex) {
      getLogWriter().info("Exception : " , ex);
      assertTrue(ex.getMessage().contains(
          "For Functions with isHA true, hasResult must also be true."));
    }
  }


  public static void allServerExecution(Boolean isByName, Function function, Boolean toRegister) {
    
    DistributedSystem.setThreadsSocketPolicy(false);
    if(toRegister.booleanValue()){
      FunctionService.registerFunction(function);
    }
    else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }
    Execution member = FunctionService.onServers(pool);
    try {
      ResultCollector rs = execute(member, Boolean.TRUE,
          function, isByName);
      
      List resultList = (List)rs.getResult();
      assertEquals(Boolean.TRUE, resultList.get(0));
      assertEquals(Boolean.TRUE, resultList.get(1));
      assertEquals(Boolean.TRUE, resultList.get(2));
      
    }catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : " , ex);
      fail("Test failed after the execute operation");
    }
    
    try {
      
      final HashSet testKeysSet = new HashSet();
      for (int i = 0; i <20; i++) {
        testKeysSet.add("execKey-" + i);
      }
      
      ResultCollector rs = execute(member, testKeysSet,
          function, isByName);
      List resultList = (List)rs.getResult();
      assertEquals(3, resultList.size());      
      
      for (int j = 0; j < 3; j++) {
        for (int k = 0; k < 20; k++) {
          assertEquals(true, (((List)(resultList).get(j))
              .contains("execKey-" + k)));
        }
      }      
          
    }catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : " , ex);
      fail("Test failed after the execute operation");
    }
    
  }
  
public static void allServerExecution_SendException(Boolean isByName, Function function, Boolean toRegister) {
    
    DistributedSystem.setThreadsSocketPolicy(false);
    if(toRegister.booleanValue()){
      FunctionService.registerFunction(function);
    }
    else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }
    Execution member = FunctionService.onServers(pool);
    try {
      ResultCollector rs = execute(member, Boolean.TRUE,
          function, isByName);
      
      List resultList = (List)rs.getResult();
      assertTrue(resultList.get(0) instanceof MyFunctionExecutionException);
      assertTrue(resultList.get(1) instanceof MyFunctionExecutionException);
      assertTrue(resultList.get(2) instanceof MyFunctionExecutionException);
      
    }catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : " , ex);
      fail("Test failed after the execute operation");
    }
    
    try {
      final HashSet testKeysSet = new HashSet();
      for (int i = 0; i <20; i++) {
        testKeysSet.add("execKey-" + i);
      }
      
      ResultCollector rs = execute(member, testKeysSet,
          function, isByName);
      List resultList = (List)rs.getResult();
      assertEquals(((testKeysSet.size()*3)+3), resultList.size());
      Iterator resultIterator = resultList.iterator();
      int exceptionCount = 0;
      while(resultIterator.hasNext()){
        Object o = resultIterator.next();
        if(o instanceof MyFunctionExecutionException){
          exceptionCount++;
        }
      }
      assertEquals(3, exceptionCount);
      
    }catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : " , ex);
      fail("Test failed after the execute operation");
    }
    
  }
  
  public static void allServerExecution_NoLastResult(Boolean isByName, Function function, Boolean toRegister) {
    
    DistributedSystem.setThreadsSocketPolicy(false);
    if(toRegister.booleanValue()){
      FunctionService.registerFunction(function);
    }
    else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }
    Execution member = FunctionService.onServers(pool);
    try {
      ResultCollector rs = execute(member, Boolean.TRUE, function, isByName);
      List resultList = (List)rs.getResult();
      fail("Expected FunctionException : Function did not send last result");
    }
    catch (Exception ex) {
      assertTrue(ex.getMessage().contains("did not send last result"));
    }
  }
  
  public static void allServerExecution_Inline() {
    DistributedSystem.setThreadsSocketPolicy(false);
    Execution member = FunctionService.onServers(pool);
    try {
      ResultCollector rs = member.withArgs(Boolean.TRUE).execute(new FunctionAdapter(){
        public void execute(FunctionContext context) {
          if (context.getArguments() instanceof String) {
            context.getResultSender().lastResult( "Success");
          }else if(context.getArguments() instanceof Boolean){
            context.getResultSender().lastResult( Boolean.TRUE);
          }
        }

        public String getId() {
          return getClass().getName();
        }

        public boolean hasResult() {
          return true;
        }
      });  
      List resultList = (List)rs.getResult();
      assertEquals(Boolean.TRUE, resultList.get(0));
      assertEquals(Boolean.TRUE, resultList.get(1));
      assertEquals(Boolean.TRUE, resultList.get(2));
           
    }catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : " , ex);
      fail("Test failed after the execute operation asdfasdfa   ");
    }
  }
  
  private static ResultCollector execute(Execution member,
      Serializable args, Function function, Boolean isByName) throws Exception {
    if (isByName.booleanValue()) {// by name
      getLogWriter().info("The function name to execute : " + function.getId());
      Execution me = member.withArgs(args);   
      getLogWriter().info("The args passed  : " + args);
      return me.execute(function.getId()); 
    }
    else { // By Instance
      return member.withArgs(args).execute(function);
    }
  }
  
  public void tearDown2() throws Exception {
    super.tearDown2();
  }
}
