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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.rmi.ServerException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheClosedException;
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
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.functions.TestFunction;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;

public class PRClientServerRegionFunctionExecutionDUnitTest extends PRClientServerTestBase {
  private static final String TEST_FUNCTION7 = TestFunction.TEST_FUNCTION7;

  private static final String TEST_FUNCTION2 = TestFunction.TEST_FUNCTION2;
  static Boolean isByName = null;
  
  private static int retryCount = 0;
  static Boolean toRegister = null;

  private static Region metaDataRegion;

  static final String retryRegionName = "RetryDataRegion";
  
  public PRClientServerRegionFunctionExecutionDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public void test_Bug_43126_Function_Not_Registered()
      throws InterruptedException {
    createScenario();
    try {
      client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
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
    Function function = new TestFunction(true, TEST_FUNCTION2);
    registerFunctionAtServer(function);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "executeRegisteredFunction");
  }

  /*
   * Execution of the function on server with single key as the routing
   * object and using the name of the function
   */   
  public void testServerSingleKeyExecution_byName() {
    createScenario();
    Function function = new TestFunction(true,TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = new Boolean(true);
    toRegister = new Boolean(true);
    SerializableRunnable suspect = new SerializableRunnable() {
      public void run() {
        cache.getLogger().info("<ExpectedException action=add>" +
            "No target node found for KEY = " + 
            "|Server could not send the reply" +
            "|Unexpected exception during" +
            "</ExpectedException>");
      }
    };
    runOnAllServers(suspect);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverSingleKeyExecution", new Object[] { isByName, toRegister});
    SerializableRunnable endSuspect = new SerializableRunnable() {
      public void run() {
        cache.getLogger().info("<ExpectedException action=remove>" +
            "No target node found for KEY = " + 
            "|Server could not send the reply" +
            "|Unexpected exception during" +
            "</ExpectedException>");
      }
    };
    runOnAllServers(endSuspect);
  }
  
  public void testServerSingleKeyExecution_Bug43513_OnRegion() {
    createScenario_SingleConnection();
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverSingleKeyExecutionOnRegion_SingleConnection");
  }
  
  public void Bug47584_testServerSingleKeyExecution_Bug43513_OnServer() {
    createScenario_SingleConnection();
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverSingleKeyExecutionOnServer_SingleConnection");
  }
  
  public void testServerSingleKeyExecution_SendException() {
    createScenario();
    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
    registerFunctionAtServer(function);
    isByName = new Boolean(true);
    toRegister = new Boolean(true);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverSingleKeyExecution_SendException", new Object[] { isByName, toRegister});
  }
  
  public void testServerSingleKeyExecution_ThrowException() {
    createScenario();
    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_THROW_EXCEPTION);
    registerFunctionAtServer(function);
    isByName = new Boolean(true);
    toRegister = new Boolean(true);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverSingleKeyExecution_ThrowException", new Object[] { isByName, toRegister});
  }
  
  public void testClientWithoutPool_Bug41832() {
    createScenarioWith2Regions();
    Function function = new TestFunction(true,TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = new Boolean(true);
    toRegister = new Boolean(true);
    SerializableRunnable suspect = new SerializableRunnable() {
      public void run() {
        cache.getLogger().info("<ExpectedException action=add>" +
            "No target node found for KEY = " + 
            "|Server could not send the reply" +
            "|Unexpected exception during" +
            "</ExpectedException>");
      }
    };
    runOnAllServers(suspect);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverSingleKeyExecutionWith2Regions", new Object[] { isByName, toRegister});
    SerializableRunnable endSuspect = new SerializableRunnable() {
      public void run() {
        cache.getLogger().info("<ExpectedException action=remove>" +
            "No target node found for KEY = " + 
            "|Server could not send the reply" +
            "|Unexpected exception during" +
            "</ExpectedException>");
      }
    };
    runOnAllServers(endSuspect);
  }
  
  /*
   * Execution of the function on server with single key as the routing
   * object and using the name of the function
   */   
  public void testServerExecution_NoLastResult() {
    createScenario();
    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_NO_LASTRESULT);
    registerFunctionAtServer(function);
    isByName = new Boolean(true);
    toRegister = new Boolean(true);

    final ExpectedException ex = addExpectedException("did not send last result");
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverSingleKeyExecution_NoLastResult", new Object[] { isByName,
            toRegister });
    ex.remove();
  }

  public void testServerSingleKeyExecution_byName_WithoutRegister() {
    createScenario();
    Function function = new TestFunction(true,TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = new Boolean(true);
    toRegister = new Boolean(false);
    SerializableRunnable suspect = new SerializableRunnable() {
      public void run() {
        cache.getLogger().info("<ExpectedException action=add>" +
            "No target node found for KEY = " + 
            "|Server could not send the reply" +
            "|Unexpected exception during" +
            "</ExpectedException>");
      }
    };
    runOnAllServers(suspect);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverSingleKeyExecution", new Object[] { isByName, toRegister});
    SerializableRunnable endSuspect = new SerializableRunnable() {
      public void run() {
        cache.getLogger().info("<ExpectedException action=remove>" +
            "No target node found for KEY = " + 
            "|Server could not send the reply" +
            "|Unexpected exception during" +
            "</ExpectedException>");
      }
    };
    runOnAllServers(endSuspect);
  }
  
  /*
   * Execution of the function on server with single key as the routing.
   * Function throws the FunctionInvocationTargetException. As this is the case
   * of HA then system should retry the function execution. After 5th attempt
   * function will send Boolean as last result.
   */
  public void testserverSingleKeyExecution_FunctionInvocationTargetException() {
    createScenario();
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverSingleKeyExecution_FunctionInvocationTargetException");
  }
  
  public void testServerSingleKeyExecution_SocketTimeOut() {
    createScenario();
    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    registerFunctionAtServer(function);
    isByName = new Boolean(true);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverSingleKeyExecutionSocketTimeOut", new Object[] { isByName});
  }
  
  /*
   * Execution of the function on server with single key as the routing
   * object and using the instance of the function
   */
  public void testServerSingleKeyExecution_byInstance() {
    createScenario();
    Function function = new TestFunction(true,TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = new Boolean(false);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverSingleKeyExecution", new Object[] { isByName , toRegister});
  } 
  
  /*
   * Execution of the inline function on server with single key as the routing
   * object
   */   
  public void testServerSingleKeyExecution_byInlineFunction() {
    createScenario();
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverSingleKeyExecution_Inline");
  }
  
  /*
   * Execution of the function on server with set multiple keys as the routing
   * object and using the name of the function
   */
  public void testserverMultiKeyExecution_byName(){
    createScenario();
    Function function = new TestFunction(true,TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = new Boolean(true);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverMultiKeyExecution",
        new Object[] { isByName});
    server1.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class, "checkBucketsOnServer");
    server2.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class, "checkBucketsOnServer");
    server3.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class, "checkBucketsOnServer");
  }
  
  /*
   * Execution of the function on server with bucket as filter
   */
  public void testBucketFilter(){
    createScenarioForBucketFilter();
    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_BUCKET_FILTER);
    registerFunctionAtServer(function);
    // test multi key filter
    Set<Integer> bucketFilterSet = new HashSet<Integer>();
    bucketFilterSet.add(3);
    bucketFilterSet.add(6);
    bucketFilterSet.add(8);
    client.invoke(PRClientServerTestBase.class,
        "serverBucketFilterExecution",
        new Object[]{bucketFilterSet});
    bucketFilterSet.clear();
    //Test single filter
    bucketFilterSet.add(7);
    client.invoke(PRClientServerTestBase.class,
        "serverBucketFilterExecution",
        new Object[]{bucketFilterSet});
    
  }
  
  public void testBucketFilterOverride(){
    createScenarioForBucketFilter();
    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_BUCKET_FILTER);
    registerFunctionAtServer(function);
    // test multi key filter
    Set<Integer> bucketFilterSet = new HashSet<Integer>();
    bucketFilterSet.add(3);
    bucketFilterSet.add(6);
    bucketFilterSet.add(8);
    
    Set<Integer> keyFilterSet = new HashSet<Integer>();
    keyFilterSet.add(75);
    keyFilterSet.add(25);
    
    client.invoke(PRClientServerTestBase.class,
        "serverBucketFilterOverrideExecution",
        new Object[]{bucketFilterSet, keyFilterSet});   
    
  }
  
  public void testserverMultiKeyExecution_SendException(){
    createScenario();
    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
    registerFunctionAtServer(function);
    isByName = new Boolean(true);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverMultiKeyExecution_SendException",
        new Object[] { isByName});
  }
  
  public void testserverMultiKeyExecution_ThrowException(){
    createScenario();
    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_THROW_EXCEPTION);
    registerFunctionAtServer(function);
    isByName = new Boolean(true);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverMultiKeyExecution_ThrowException",
        new Object[] { isByName});
  }
  

  
  /*
   * Execution of the inline function on server with set multiple keys as the routing
   * object
   */
  public void testserverMultiKeyExecution_byInlineFunction(){
    createScenario();
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverMultiKeyExecution_Inline");
  }
  
  /*
   * Execution of the inline function on server with set multiple keys as the
   * routing object Function throws the FunctionInvocationTargetException. As
   * this is the case of HA then system should retry the function execution.
   * After 5th attempt function will send Boolean as last result.
   */
  public void testserverMultiKeyExecution_FunctionInvocationTargetException() {
    createScenario();
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverMultiKeyExecution_FunctionInvocationTargetException");
  }
  
  /*
   * Execution of the function on server with set multiple keys as the routing
   * object and using the name of the function
   */
  public void testserverMultiKeyExecutionNoResult_byName(){
    createScenario();
    Function function = new TestFunction(false,TEST_FUNCTION7);
    registerFunctionAtServer(function);
    isByName = new Boolean(true);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverMultiKeyExecutionNoResult",
        new Object[] { isByName});
  }

  /*
   * Execution of the function on server with set multiple keys as the routing
   * object and using the instance of the function
   */
  public void testserverMultiKeyExecution_byInstance(){
    createScenario();
    Function function = new TestFunction(true,TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = new Boolean(false);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverMultiKeyExecution",
        new Object[] { isByName});
  }
  
  /*
   * Ensure that the execution is limited to a single bucket put another way,
   * that the routing logic works correctly such that there is not extra
   * execution
   */
  public void testserverMultiKeyExecutionOnASingleBucket_byName(){
    createScenario();
    Function function = new TestFunction(true,TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = new Boolean(true);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
      "serverMultiKeyExecutionOnASingleBucket", new Object[] { isByName});
  }
  
  /*
   * Ensure that the execution is limited to a single bucket put another way,
   * that the routing logic works correctly such that there is not extra
   * execution
   */
  public void testserverMultiKeyExecutionOnASingleBucket_byInstance(){
    createScenario();
    Function function = new TestFunction(true,TEST_FUNCTION2);
    registerFunctionAtServer(function);
    isByName = new Boolean(false);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
      "serverMultiKeyExecutionOnASingleBucket", new Object[] { isByName});
  }


  public static void regionSingleKeyExecutionNonHA(Boolean isByName,
      Function function, Boolean toRegister) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister.booleanValue()) {
      FunctionService.registerFunction(function);
    } else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }

    Execution dataSet = FunctionService.onRegion(region);

    region.put(testKey, new Integer(1));
    try {
      ArrayList<String> args = new ArrayList<String>();
      args.add(retryRegionName);
      args.add("regionSingleKeyExecutionNonHA");

      ResultCollector rs = execute(dataSet, testKeysSet, args,
          function, isByName);
      fail("Expected ServerConnectivityException not thrown!");
    } catch (Exception ex) {
      if (!(ex.getCause() instanceof ServerConnectivityException)
          && !(ex.getCause() instanceof FunctionInvocationTargetException)) {
        ex.printStackTrace();
        getLogWriter().info("Exception : ", ex);
        fail("Test failed after the execute operation");
      }
    }
  }
  
  public static void regionExecutionHAOneServerDown (Boolean isByName,
      Function function, Boolean toRegister) {

    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister.booleanValue()) {
      FunctionService.registerFunction(function);
    } else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }

    Execution dataSet = FunctionService.onRegion(region);

    region.put(testKey, new Integer(1));
    try {
      ArrayList<String> args = new ArrayList<String>();
      args.add(retryRegionName);
      args.add("regionExecutionHAOneServerDown");

      ResultCollector rs = execute(dataSet, testKeysSet, args,
          function, isByName);
      assertEquals(1, ((List)rs.getResult()).size());
    } catch (Exception ex) {
        ex.printStackTrace();
        getLogWriter().info("Exception : ", ex);
        fail("Test failed after the execute operation", ex);
    }
  }
  
  public static void regionExecutionHATwoServerDown (Boolean isByName,
      Function function, Boolean toRegister) {

    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    if (toRegister.booleanValue()) {
      FunctionService.registerFunction(function);
    } else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }

    Execution dataSet = FunctionService.onRegion(region);

    region.put(testKey, new Integer(1));
    try {
      ArrayList<String> args = new ArrayList<String>();
      args.add(retryRegionName);
      args.add("regionExecutionHATwoServerDown");

      ResultCollector rs = execute(dataSet, testKeysSet, args,
          function, isByName);
      assertEquals(1, ((List)rs.getResult()).size());
    } catch (Exception ex) {
        ex.printStackTrace();
        getLogWriter().info("Exception : ", ex);
        fail("Test failed after the execute operation");
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
  
  public static void registerFunction() {
    FunctionService.registerFunction(new FunctionAdapter() {
      @Override
      public void execute(FunctionContext context) {
        if (context.getArguments() instanceof String) {
          context.getResultSender().lastResult("Failure");
        }
        else if (context.getArguments() instanceof Boolean) {
          context.getResultSender().lastResult(Boolean.FALSE);
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

  public static void FunctionExecution_Inline_Bug40714() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 10); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    int j = 0;
    for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
      Integer val = new Integer(j++);
      region.put(i.next(), val);
    }
    List list = (List)FunctionService.onRegion(region).withArgs(Boolean.TRUE)
        .execute(new FunctionAdapter() {
          @Override
          public void execute(FunctionContext context) {
            if (context.getArguments() instanceof String) {
              context.getResultSender().lastResult("Success");
            }
            else if (context.getArguments() instanceof Boolean) {
              context.getResultSender().lastResult(Boolean.TRUE);
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
        }).getResult();
    assertEquals(3, list.size());
    Iterator iterator = list.iterator();
    for (int i = 0; i < 3; i++) {
      Boolean res = (Boolean)iterator.next();
      assertEquals(Boolean.TRUE, res);
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
  
  public static void executeFunction() throws ServerException,
      InterruptedException {

    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 10); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true,TEST_FUNCTION2);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      ResultCollector rc1 = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(
          function.getId());
      List l = ((List)rc1.getResult());
      getLogWriter().info("Result size : " + l.size());
      assertEquals(3, l.size());

      for (Iterator i = l.iterator(); i.hasNext();) {
        assertEquals(Boolean.TRUE, i.next());
      }
    }
    catch (Exception e) {
      getLogWriter().info("Got an exception : " + e.getMessage());
      assertTrue(e instanceof EOFException || e instanceof SocketException
          || e instanceof SocketTimeoutException
          || e instanceof ServerException || e instanceof IOException
          || e instanceof CacheClosedException);
    }
  }
  
  public static Object executeFunctionHA() throws Exception {
    Region region = cache.getRegion(PartitionedRegionName);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 10); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_HA);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    ResultCollector rc1 = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(
        function.getId());
    List l = ((List)rc1.getResult());
    getLogWriter().info("Result size : " + l.size());
    return l;
  }
  
  public static void putOperation(){
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 10); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    int j = 0;
    HashSet origVals = new HashSet();
    for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
      Integer val = new Integer(j++);
      origVals.add(val);
      region.put(i.next(), val);
    }
  }
  
  protected void createScenario() {
    ArrayList commonAttributes =  createCommonServerAttributes("TestPartitionedRegion", null, 0, 13, null);
    createClientServerScenarion(commonAttributes,20, 20, 20);
  }
  
  protected void createScenarioForBucketFilter() {
    ArrayList commonAttributes =  createCommonServerAttributes("TestPartitionedRegion", new BucketFilterPRResolver(), 
        0, 113, null);
    createClientServerScenarion(commonAttributes,20, 20, 20);
  }
  
  private void createScenario_SingleConnection() {
    ArrayList commonAttributes =  createCommonServerAttributes("TestPartitionedRegion", null, 0, 13, null);
    createClientServerScenarion_SingleConnection(commonAttributes,0, 20, 20);
  }
  
  
  
  private void createScenarioWith2Regions() {
    ArrayList commonAttributes =  createCommonServerAttributes(PartitionedRegionName, null, 0, 13, null);
    createClientServerScenarionWith2Regions(commonAttributes,20, 20, 20);
    
  }
  
  public static void checkBucketsOnServer(){
    PartitionedRegion region = (PartitionedRegion)cache.getRegion(PartitionedRegionName);
    HashMap localBucket2RegionMap = (HashMap)region
    .getDataStore().getSizeLocally();
    getLogWriter().info(
    "Size of the " + PartitionedRegionName + " in this VM :- "
        + localBucket2RegionMap.size());
    Set entrySet = localBucket2RegionMap.entrySet();
    assertNotNull(entrySet);
  }

  public static void serverMultiKeyExecutionOnASingleBucket(Boolean isByName) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    int j = 0;
    for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
      Integer val = new Integer(j++);
      region.put(i.next(), val);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    for (Iterator kiter = testKeysSet.iterator(); kiter.hasNext();) {
      try {
        Set singleKeySet = Collections.singleton(kiter.next());
        Function function = new TestFunction(true,TEST_FUNCTION2);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(region);
        ResultCollector rc1 = execute(dataSet, singleKeySet, Boolean.TRUE,  function, isByName);
        List l = null;
        l = ((List)rc1.getResult());
        assertEquals(1, l.size());

        ResultCollector rc2 =  execute(dataSet, singleKeySet, new HashSet(singleKeySet), 
            function, isByName);
        List l2 = null;
        l2 = ((List)rc2.getResult());
        
        assertEquals(1, l2.size());
        List subList = (List)l2.iterator().next();
        assertEquals(1, subList.size());
        assertEquals(region.get(singleKeySet.iterator().next()), subList
            .iterator().next());
      }
      catch (Exception expected) {
        getLogWriter().info("Exception : " + expected.getMessage());
        expected.printStackTrace();
        fail("Test failed after the put operation");
      }
    }
  }
  
  public static void serverMultiKeyExecution(Boolean isByName) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true,TEST_FUNCTION2);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      int j = 0;
      HashSet origVals = new HashSet();
      for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
        Integer val = new Integer(j++);
        origVals.add(val);
        region.put(i.next(), val);
      }
      List l = null;
      ResultCollector rc1 = execute(dataSet, testKeysSet, Boolean.TRUE,  function, isByName);
      l = ((List)rc1.getResult());
      getLogWriter().info("Result size : " + l.size());
      assertEquals(3, l.size());
      for (Iterator i = l.iterator(); i.hasNext();) {
        assertEquals(Boolean.TRUE, i.next());
      }

      List l2 = null;
      ResultCollector rc2 = execute(dataSet, testKeysSet, testKeysSet,
          function, isByName);
      l2 = ((List)rc2.getResult());
      assertEquals(3, l2.size());
      HashSet foundVals = new HashSet();
      for (Iterator i = l2.iterator(); i.hasNext();) {
        ArrayList subL = (ArrayList)i.next();
        assertTrue(subL.size() > 0);
        for (Iterator subI = subL.iterator(); subI.hasNext();) {
          assertTrue(foundVals.add(subI.next()));
        }
      }
      assertEquals(origVals, foundVals);
      
    }catch(Exception e){
      fail("Test failed after the put operation", e);
      
    }
  }
  
  
  
  public static void serverMultiKeyExecution_SendException(Boolean isByName) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    int j = 0;
    HashSet origVals = new HashSet();
    for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
      Integer val = new Integer(j++);
      origVals.add(val);
      region.put(i.next(), val);
    }
    try {
      List l = null;
      ResultCollector rc1 = execute(dataSet, testKeysSet, Boolean.TRUE,
          function, isByName);
      l = ((List)rc1.getResult());
      getLogWriter().info("Result size : " + l.size());
      assertEquals(3, l.size());
      for (Iterator i = l.iterator(); i.hasNext();) {
        assertTrue(i.next() instanceof MyFunctionExecutionException);
      }
    }
    catch(Exception ex){
      ex.printStackTrace();
      fail("No Exception Expected");
    }
    
    try {
      List l = null;
      ResultCollector rc1 = execute(dataSet, testKeysSet, testKeysSet,
          function, isByName);
      List resultList = (List)rc1.getResult();
      assertEquals(((testKeysSet.size() * 3) +3), resultList.size());
      Iterator resultIterator = resultList.iterator();
      int exceptionCount = 0;
      while(resultIterator.hasNext()){
        Object o = resultIterator.next();
        if(o instanceof MyFunctionExecutionException){
          exceptionCount++;
        }
      }
      assertEquals(3, exceptionCount);
    }
    catch(Exception ex){
      ex.printStackTrace();
      fail("No Exception Expected");
    }
  }
  
  public static void serverMultiKeyExecution_ThrowException(Boolean isByName) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_THROW_EXCEPTION);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    int j = 0;
    HashSet origVals = new HashSet();
    for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
      Integer val = new Integer(j++);
      origVals.add(val);
      region.put(i.next(), val);
    }
    try {
      List l = null;
      ResultCollector rc1 = execute(dataSet, testKeysSet, Boolean.TRUE,
          function, isByName);
      fail("Exception Expected");
    }
    catch(Exception ex){
      ex.printStackTrace();
    }
  }
  
  public static void serverMultiKeyExecutionSocketTimeOut(Boolean isByName) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      int j = 0;
      HashSet origVals = new HashSet();
      for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
        Integer val = new Integer(j++);
        origVals.add(val);
        region.put(i.next(), val);
      }
      List l = null;
      ResultCollector rc1 = execute(dataSet, testKeysSet, Boolean.TRUE,
          function, isByName);
      l = ((List)rc1.getResult());
      getLogWriter().info("Result size : " + l.size());
      assertEquals(3, l.size());
      for (Iterator i = l.iterator(); i.hasNext();) {
        assertEquals(Boolean.TRUE, i.next());
      }

    }catch(Exception e){
      fail("Test failed after the put operation", e);
      
    }
  }
  
  public static void serverSingleKeyExecutionSocketTimeOut(Boolean isByName) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_SOCKET_TIMEOUT);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    
    region.put(testKey, new Integer(1));
    try {
      ResultCollector rs = execute(dataSet, testKeysSet, Boolean.TRUE,
          function, isByName);
      assertEquals(Boolean.TRUE, ((List)rs.getResult()).get(0));

      ResultCollector rs2 = execute(dataSet, testKeysSet, testKey, function,
          isByName);
      assertEquals(testKey, ((List)rs2.getResult()).get(0));

    }catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : " , ex);
      fail("Test failed after the put operation",ex);
    }
  }
  
  public static void serverMultiKeyExecution_Inline() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      int j = 0;
      HashSet origVals = new HashSet();
      for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
        Integer val = new Integer(j++);
        origVals.add(val);
        region.put(i.next(), val);
      }
      List l = null;
      ResultCollector rc1 = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(new FunctionAdapter(){
        @Override
        public void execute(FunctionContext context) {
          if (context.getArguments() instanceof String) {
            context.getResultSender().lastResult( "Success");
          }else if(context.getArguments() instanceof Boolean){
            context.getResultSender().lastResult(Boolean.TRUE);
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
      l = ((List)rc1.getResult());
      getLogWriter().info("Result size : " + l.size());
      assertEquals(3, l.size());
      for (Iterator i = l.iterator(); i.hasNext();) {
        assertEquals(Boolean.TRUE, i.next());
      }
    }catch(Exception e){
      getLogWriter().info("Exception : " + e.getMessage());
      e.printStackTrace();
      fail("Test failed after the put operation");
      
    }
  }
  
  public static void serverMultiKeyExecution_FunctionInvocationTargetException() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Execution dataSet = FunctionService.onRegion(region);
    int j = 0;
    HashSet origVals = new HashSet();
    for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
      Integer val = new Integer(j++);
      origVals.add(val);
      region.put(i.next(), val);
    }
    ResultCollector rc1 = null;
    try {
      rc1 = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(
          new FunctionAdapter() {
            @Override
            public void execute(FunctionContext context) {
              if (((RegionFunctionContext)context).isPossibleDuplicate()) {
                context.getResultSender().lastResult(new Integer(retryCount));
                return;
              }
              if (context.getArguments() instanceof Boolean) {
                throw new FunctionInvocationTargetException(
                    "I have been thrown from TestFunction");
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

      List list = (ArrayList)rc1.getResult();
      assertEquals(list.get(0), 0);
    }
    catch (Throwable e) {
      e.printStackTrace();
      fail("This is not expected Exception", e);
    }

  }
  
  public static void serverMultiKeyExecutionNoResult(Boolean isByName) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final HashSet testKeysSet = new HashSet();
    for (int i = (totalNumBuckets.intValue() * 2); i > 0; i--) {
      testKeysSet.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(false,TEST_FUNCTION7);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);
    try {
      String msg = "<ExpectedException action=add>" +
      "FunctionException" + "</ExpectedException>";
      cache.getLogger().info(msg);
      int j = 0;
      HashSet origVals = new HashSet();
      for (Iterator i = testKeysSet.iterator(); i.hasNext();) {
        Integer val = new Integer(j++);
        origVals.add(val);
        region.put(i.next(), val);
      }
      ResultCollector rc1 = execute(dataSet, testKeysSet, Boolean.TRUE,  function, isByName);
      rc1.getResult();
      Thread.sleep(20000);
      fail("Test failed after the put operation");
    } catch(FunctionException expected) {
      assertTrue(expected.getMessage().startsWith((LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
          .toLocalizedString("return any"))));
    }
    catch (Exception notexpected) {
      fail("Test failed during execute or sleeping", notexpected);
    } finally {
      cache.getLogger().info("<ExpectedException action=remove>" +
          "FunctionException" +
          "</ExpectedException>");
    }
  }

  public static void serverSingleKeyExecutionOnRegion_SingleConnection() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    for (int i = 0; i < 13; i++) {
      region.put(new Integer(i), "KB_"+i);
    }
    Function function = new TestFunction(false,TEST_FUNCTION2);
    Execution dataSet = FunctionService.onRegion(region);
    dataSet.withArgs(Boolean.TRUE).execute(function);
    region.put(new Integer(2), "KB_2");
    assertEquals("KB_2", region.get(new Integer(2)));    
  }
  
  public static void serverSingleKeyExecutionOnServer_SingleConnection() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    Function function = new TestFunction(false,TEST_FUNCTION2);
    Execution dataSet = FunctionService.onServer(pool);
    dataSet.withArgs(Boolean.TRUE).execute(function);
    region.put(new Integer(1), "KB_1");
    assertEquals("KB_1", region.get(new Integer(1)));    
  }
  
  public static void serverSingleKeyExecution(Boolean isByName, Boolean toRegister) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function function = new TestFunction(true,TEST_FUNCTION2);
    
    if(toRegister.booleanValue()){
      FunctionService.registerFunction(function);
    }
    else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }
    Execution dataSet = FunctionService.onRegion(region);
    try {
      execute(dataSet, testKeysSet, Boolean.TRUE,  function, isByName);
    }
    catch (Exception expected) {
      assertTrue(expected.getMessage().contains(
          "No target node found for KEY = " + testKey)
          || expected.getMessage()
              .startsWith("Server could not send the reply")
          || expected.getMessage().startsWith("Unexpected exception during"));
    }
    region.put(testKey, new Integer(1));
    try {
      ResultCollector rs = execute(dataSet, testKeysSet, Boolean.TRUE,
          function, isByName);
      assertEquals(Boolean.TRUE, ((List)rs.getResult()).get(0));

      ResultCollector rs2 = execute(dataSet, testKeysSet, testKey, function,
          isByName);
      assertEquals(new Integer(1), ((List)rs2.getResult()).get(0));

      HashMap putData = new HashMap();
      putData.put(testKey + "1", new Integer(2));
      putData.put(testKey + "2", new Integer(3));

      ResultCollector rs1 = execute(dataSet, testKeysSet, putData, function,
          isByName);
      assertEquals(Boolean.TRUE, ((List)rs1.getResult()).get(0));

      assertEquals(new Integer(2), region.get(testKey + "1"));
      assertEquals(new Integer(3), region.get(testKey + "2"));

    }catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : " , ex);
      fail("Test failed after the put operation",ex);
    }
  }
  
  public static void executeRegisteredFunction() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
    testKeysSet.add(testKey);
    Execution dataSet = FunctionService.onRegion(region);
    region.put(testKey, new Integer(1));
      ((AbstractExecution)dataSet).removeFunctionAttributes(TestFunction.TEST_FUNCTION2);
      ResultCollector rs = dataSet.withFilter(testKeysSet).withArgs(
          Boolean.TRUE).execute(TestFunction.TEST_FUNCTION2);
      assertEquals(Boolean.TRUE, ((List)rs.getResult()).get(0));
      byte[] functionAttributes = ((AbstractExecution)dataSet).getFunctionAttributes(TestFunction.TEST_FUNCTION2);
      assertNotNull(functionAttributes);
      
      rs = dataSet.withFilter(testKeysSet).withArgs(
          Boolean.TRUE).execute(TestFunction.TEST_FUNCTION2);
      assertEquals(Boolean.TRUE, ((List)rs.getResult()).get(0));
      assertNotNull(functionAttributes);
  }
  
  public static void serverSingleKeyExecution_SendException(Boolean isByName,
      Boolean toRegister) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function function = new TestFunction(true,
        TestFunction.TEST_FUNCTION_SEND_EXCEPTION);

    if (toRegister.booleanValue()) {
      FunctionService.registerFunction(function);
    }
    else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }

    Execution dataSet = FunctionService.onRegion(region);
    region.put(testKey, new Integer(1));
    ResultCollector rs = null;
    try {
      rs = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      assertTrue(((List)rs.getResult()).get(0) instanceof MyFunctionExecutionException);
    }
    catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : ", ex);
      fail("Test failed after the put operation", ex);
    }
    
    try {
      rs = execute(dataSet, testKeysSet, (Serializable)testKeysSet, function, isByName);
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
    }
    catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : ", ex);
      fail("Test failed after the put operation", ex);
    }
  }
  
  public static void serverSingleKeyExecution_ThrowException(Boolean isByName,
      Boolean toRegister) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function function = new TestFunction(true,
        TestFunction.TEST_FUNCTION_THROW_EXCEPTION);

    if (toRegister.booleanValue()) {
      FunctionService.registerFunction(function);
    }
    else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }

    Execution dataSet = FunctionService.onRegion(region);
    region.put(testKey, new Integer(1));
    ResultCollector rs = null;
    try {
      rs = execute(dataSet, testKeysSet, Boolean.TRUE, function, isByName);
      fail("Exception Expected");
    }
    catch (Exception ex) {
      ex.printStackTrace();
      assertTrue(ex instanceof Exception);
    }
  }
  
  public static void serverSingleKeyExecutionWith2Regions(Boolean isByName, Boolean toRegister) {
    Region region1 = cache.getRegion(PartitionedRegionName+"1");
    assertNotNull(region1);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);
    Function function = new TestFunction(true,TEST_FUNCTION2);
    if(toRegister.booleanValue()){
      FunctionService.registerFunction(function);
    }
    else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }
    
    Execution dataSet1 = FunctionService.onRegion(region1);
    region1.put(testKey, new Integer(1));
    try {
      ResultCollector rs = dataSet1.execute(function.getId());
      assertEquals(Boolean.FALSE, ((List)rs.getResult()).get(0));

    }catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : " , ex);
      fail("Test failed after the put operation",ex);
    }
    
    Region region2 = cache.getRegion(PartitionedRegionName+"2");
    assertNotNull(region2);

    Execution dataSet2 = FunctionService.onRegion(region2);
    region2.put(testKey, new Integer(1));
    try {
      ResultCollector rs = dataSet2.execute(function.getId());
      assertEquals(Boolean.TRUE, ((List)rs.getResult()).get(0));
      fail("Expected FunctionException");
    }catch (Exception ex) {
      assertTrue(ex.getMessage().startsWith("No Replicated Region found for executing function"));
    }
  }
  
  public static void serverSingleKeyExecution_NoLastResult(Boolean isByName, Boolean toRegister) {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_NO_LASTRESULT);
    
    if(toRegister.booleanValue()){
      FunctionService.registerFunction(function);
    }
    else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }
    
    Execution dataSet = FunctionService.onRegion(region);
    region.put(testKey, new Integer(1));
    try {
      ResultCollector rs = execute(dataSet, testKeysSet, Boolean.TRUE,
          function, isByName);
      assertEquals(Boolean.TRUE, ((List)rs.getResult()).get(0));
      fail("Expected FunctionException : Function did not send last result");
    }catch (Exception ex) {
      assertTrue(ex.getMessage().contains(
          "did not send last result"));
      
    }
  }
  
  public static void serverSingleKeyExecution_FunctionInvocationTargetException() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Function function = new TestFunction(true,
        TestFunction.TEST_FUNCTION_REEXECUTE_EXCEPTION);
    FunctionService.registerFunction(function);
    Execution dataSet = FunctionService.onRegion(region);

    region.put(testKey, new Integer(1));
    try {
      ResultCollector rs = execute(dataSet, testKeysSet, Boolean.TRUE,
          function, false);
      ArrayList list = (ArrayList)rs.getResult();
      assertTrue(((Integer)list.get(0)) >= 5);
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("This is not expected Exception", ex);
    }
  }
  
  public static void serverSingleKeyExecution_Inline() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Execution dataSet = FunctionService.onRegion(region);
    try {
      cache.getLogger().info("<ExpectedException action=add>" +
          "No target node found for KEY = " + 
          "|Server could not send the reply" +
          "|Unexpected exception during" +
          "</ExpectedException>");
      dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(new FunctionAdapter(){
        @Override
        public void execute(FunctionContext context) {
          if (context.getArguments() instanceof String) {
            context.getResultSender().lastResult( "Success");
          }
          context.getResultSender().lastResult( "Failure");
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
    }
    catch (Exception expected) {
      getLogWriter().fine("Exception occured : " + expected.getMessage());
      assertTrue(expected.getMessage().contains(
          "No target node found for KEY = " + testKey)
          || expected.getMessage()
              .startsWith("Server could not send the reply")
          || expected.getMessage().startsWith("Unexpected exception during"));
    } finally {
      cache.getLogger().info("<ExpectedException action=remove>" +
          "No target node found for KEY = " + 
          "|Server could not send the reply" +
          "|Unexpected exception during" +
          "</ExpectedException>");
    }
    
    region.put(testKey, new Integer(1));
    try {
      ResultCollector rs = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(new FunctionAdapter(){
        @Override
        public void execute(FunctionContext context) {
          if (context.getArguments() instanceof String) {
            context.getResultSender().lastResult( "Success");
          }else{
            context.getResultSender().lastResult( "Failure");
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
      assertEquals("Failure", ((List)rs.getResult()).get(0));

      ResultCollector rs2 = dataSet.withFilter(testKeysSet).withArgs(testKey).execute(new FunctionAdapter(){
        @Override
        public void execute(FunctionContext context) {
          if (context.getArguments() instanceof String) {
            context.getResultSender().lastResult( "Success");
          }else{
            context.getResultSender().lastResult( "Failure");
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
      assertEquals("Success", ((List)rs2.getResult()).get(0));

    }catch (Exception ex) {
      ex.printStackTrace();
      getLogWriter().info("Exception : " , ex);
      fail("Test failed after the put operation",ex);
    }
  }
  
  

  /**
   * This class can be serialized but its deserialization will always fail
   * @author darrel
   *
   */
  private static class UnDeserializable implements DataSerializable {
    public void toData(DataOutput out) throws IOException {
    }
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      throw new RuntimeException("deserialization is not allowed on this class");
    }
    
  }
  public static void serverBug43430() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final String testKey = "execKey";
    final Set testKeysSet = new HashSet();
    testKeysSet.add(testKey);
    DistributedSystem.setThreadsSocketPolicy(false);

    Execution dataSet = FunctionService.onRegion(region);
    region.put(testKey, new Integer(1));
    try {
      cache.getLogger().info("<ExpectedException action=add>" +
          "Could not create an instance of  com.gemstone.gemfire.internal.cache.execute.PRClientServerRegionFunctionExecutionDUnitTest$UnDeserializable" +
          "</ExpectedException>");
      dataSet.withFilter(testKeysSet).withArgs(new UnDeserializable()).execute(new FunctionAdapter(){
        @Override
        public void execute(FunctionContext context) {
          if (context.getArguments() instanceof String) {
            context.getResultSender().lastResult( "Success");
          }
          context.getResultSender().lastResult( "Failure");
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
    }
    catch (Exception expected) {
      getLogWriter().fine("Exception occured : " + expected.getMessage());
      assertTrue(expected.getCause().getMessage().contains(
          "Could not create an instance of  com.gemstone.gemfire.internal.cache.execute.PRClientServerRegionFunctionExecutionDUnitTest$UnDeserializable"));
    } finally {
      cache.getLogger().info("<ExpectedException action=remove>" +
          "Could not create an instance of  com.gemstone.gemfire.internal.cache.execute.PRClientServerRegionFunctionExecutionDUnitTest$UnDeserializable" +
          "</ExpectedException>");
    }
  }

  private static ResultCollector execute(Execution dataSet, Set testKeysSet,
      Serializable args, Function function, Boolean isByName) throws Exception {
    if (isByName.booleanValue()) {// by name
      return dataSet.withFilter(testKeysSet).withArgs(args).execute(
          function.getId()); 
    }
    else { // By Instance
      return dataSet.withFilter(testKeysSet).withArgs(args).execute(function);
    }
  }
  
  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
  }

  /**
   * Attempt to do a client server function execution with an arg that fail deserialization
   * on the server. The client should see an exception instead of a hang if bug 43430 is fixed.
   */
  public void testBug43430() {
    createScenario();
    Function function = new TestFunction(true,TEST_FUNCTION2);
    registerFunctionAtServer(function);
    SerializableRunnable suspect = new SerializableRunnable() {
      public void run() {
        cache.getLogger().info("<ExpectedException action=add>" +
            "No target node found for KEY = " + 
            "|Server could not send the reply" +
            "|Unexpected exception during" +
            "</ExpectedException>");
      }
    };
    runOnAllServers(suspect);
    client.invoke(PRClientServerRegionFunctionExecutionDUnitTest.class,
        "serverBug43430", new Object[] {});
    SerializableRunnable endSuspect = new SerializableRunnable() {
      public void run() {
        cache.getLogger().info("<ExpectedException action=remove>" +
            "No target node found for KEY = " + 
            "|Server could not send the reply" +
            "|Unexpected exception during" +
            "</ExpectedException>");
      }
    };
    runOnAllServers(endSuspect);
  }

}
