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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.PartitionAttributesImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionTestHelper;
import com.gemstone.gemfire.internal.cache.functions.TestFunction;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/*
 * This is DUnite Test to test the Function Execution stats under various
 * scenarion like Cliet-Server with Region/without Region, P2P with partitioned
 * Region/Distributed Region,member Execution
 * 
 */

public class FunctionServiceStatsDUnitTest extends PRClientServerTestBase{
  
  static Boolean isByName = null;
  
  static InternalDistributedSystem ds = null;
  
  static int noOfExecutionCalls_Aggregate = 0; 
  static int noOfExecutionsCompleted_Aggregate  = 0 ; 
  static int resultReceived_Aggregate  = 0;
  static int noOfExecutionExceptions_Aggregate  = 0;
  
  static int noOfExecutionCalls_TESTFUNCTION1 = 0; 
  static int noOfExecutionsCompleted_TESTFUNCTION1 = 0 ; 
  static int resultReceived_TESTFUNCTION1 = 0;
  static int noOfExecutionExceptions_TESTFUNCTION1 = 0;
  
  static int noOfExecutionCalls_TESTFUNCTION2 = 0; 
  static int noOfExecutionsCompleted_TESTFUNCTION2 = 0 ; 
  static int resultReceived_TESTFUNCTION2 = 0;
  static int noOfExecutionExceptions_TESTFUNCTION2 = 0;
  
  static int noOfExecutionCalls_TESTFUNCTION3 = 0; 
  static int noOfExecutionsCompleted_TESTFUNCTION3 = 0 ;
  static int resultReceived_TESTFUNCTION3 = 0;
  static int noOfExecutionExceptions_TESTFUNCTION3 = 0;
  
  static int noOfExecutionCalls_TESTFUNCTION5 = 0; 
  static int noOfExecutionsCompleted_TESTFUNCTION5 = 0 ; 
  static int resultReceived_TESTFUNCTION5 = 0;
  static int noOfExecutionExceptions_TESTFUNCTION5 = 0;
  
  static int noOfExecutionCalls_Inline = 0; 
  static int noOfExecutionsCompleted_Inline = 0 ; 
  static int resultReceived_Inline = 0;
  static int noOfExecutionExceptions_Inline = 0;
  
  static int noOfExecutionCalls_TestFunctionException = 0; 
  static int noOfExecutionsCompleted_TestFunctionException  = 0 ; 
  static int resultReceived_TestFunctionException  = 0;
  static int noOfExecutionExceptions_TestFunctionException  = 0;
  
//  static Object[] VM0Stats;
//  static Object[] VM1Stats;
//  static Object[] VM2Stats;
//  static Object[] VM3tats;
  
  
  public FunctionServiceStatsDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    //Make sure stats to linger from a previous test
    disconnectAllFromDS();
  }
  
  final SerializableCallable initializeStats = new SerializableCallable(
      "initializeStats") {
    public Object call() throws Exception {

      noOfExecutionCalls_Aggregate = 0; 
      noOfExecutionsCompleted_Aggregate  = 0 ; 
      resultReceived_Aggregate  = 0;
      noOfExecutionExceptions_Aggregate  = 0;
      
      noOfExecutionCalls_TESTFUNCTION1 = 0; 
      noOfExecutionsCompleted_TESTFUNCTION1 = 0 ; 
      resultReceived_TESTFUNCTION1 = 0;
      noOfExecutionExceptions_TESTFUNCTION1 = 0;
      
      noOfExecutionCalls_TESTFUNCTION2 = 0; 
      noOfExecutionsCompleted_TESTFUNCTION2 = 0 ; 
      resultReceived_TESTFUNCTION2 = 0;
      noOfExecutionExceptions_TESTFUNCTION2 = 0;
      
      noOfExecutionCalls_TESTFUNCTION3 = 0; 
      noOfExecutionsCompleted_TESTFUNCTION3 = 0 ;
      resultReceived_TESTFUNCTION3 = 0;
      noOfExecutionExceptions_TESTFUNCTION3 = 0;
      
      noOfExecutionCalls_TESTFUNCTION5 = 0; 
      noOfExecutionsCompleted_TESTFUNCTION5 = 0 ; 
      resultReceived_TESTFUNCTION5 = 0;
      noOfExecutionExceptions_TESTFUNCTION5 = 0;
      
      noOfExecutionCalls_Inline = 0; 
      noOfExecutionsCompleted_Inline = 0 ; 
      resultReceived_Inline = 0;
      noOfExecutionExceptions_Inline = 0;
      
      noOfExecutionCalls_TestFunctionException = 0; 
      noOfExecutionsCompleted_TestFunctionException  = 0 ; 
      resultReceived_TestFunctionException  = 0;
      noOfExecutionExceptions_TestFunctionException  = 0;
      return Boolean.TRUE;
    }
  };

  /*
   * This helper method prevents race conditions in local functions. Typically, when
   * calling ResultCollector.getResult() one might expect the function to have completed.
   * For local functions this is true, however, at this point the function stats may
   * not have been updated yet thus any code which checks stats after calling getResult()
   * may get wrong data.
   */
  private void waitNoFunctionsRunning(FunctionServiceStats stats) {
    int count = 100;
    while (stats.getFunctionExecutionsRunning() > 0 && count > 0) {
      count--;
      try {
        Thread.sleep(50);
      } catch (InterruptedException ex) {
        // Ignored
      }
    }
  }

  /*
   * 1-client 3-Servers 
   * Function : TEST_FUNCTION2 
   * Function : TEST_FUNCTION3
   * Execution of the function on serverRegion with set multiple keys as the
   * routing object and using the name of the function
   * 
   * On server side, function execution calls should be equal to the no of
   * function executions completed.
   */
  public void testClientServerPartitonedRegionFunctionExecutionStats() {
    createScenario();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION2);
    registerFunctionAtServer(function);
    function = new TestFunction(true, TestFunction.TEST_FUNCTION3);
    registerFunctionAtServer(function);
    isByName = new Boolean(true);

    client.invoke(initializeStats);
    server1.invoke(initializeStats);
    server2.invoke(initializeStats);
    server3.invoke(initializeStats);
    
    SerializableCallable PopulateRegionAndExecuteFunctions = new SerializableCallable(
        "PopulateRegionAndExecuteFunctions") {
      public Object call() throws Exception {
        Region region = cache.getRegion(PartitionedRegionName);
        assertNotNull(region);
        final HashSet testKeysSet = new HashSet();
        for (int i = (totalNumBuckets.intValue() * 2); i > 0; i--) {
          testKeysSet.add("execKey-" + i);
        }
        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION2);
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
          ResultCollector rc = dataSet.withFilter(testKeysSet).withArgs(
              Boolean.TRUE).execute(function.getId());
          int resultSize = ((List)rc.getResult()).size();
          resultReceived_Aggregate += resultSize;
          resultReceived_TESTFUNCTION2 += resultSize;
          noOfExecutionCalls_Aggregate++;
          noOfExecutionCalls_TESTFUNCTION2++;
          noOfExecutionsCompleted_Aggregate++;
          noOfExecutionsCompleted_TESTFUNCTION2++;

          rc = dataSet.withFilter(testKeysSet).withArgs(testKeysSet).execute(
              function.getId());
          resultSize = ((List)rc.getResult()).size();
          resultReceived_Aggregate += resultSize;
          resultReceived_TESTFUNCTION2 += resultSize;
          noOfExecutionCalls_Aggregate++;
          noOfExecutionCalls_TESTFUNCTION2++;
          noOfExecutionsCompleted_Aggregate++;
          noOfExecutionsCompleted_TESTFUNCTION2++;

          function = new TestFunction(true, TestFunction.TEST_FUNCTION3);
          FunctionService.registerFunction(function);
          rc = dataSet.withFilter(testKeysSet).withArgs(Boolean.TRUE).execute(
              function.getId());
          resultSize = ((List)rc.getResult()).size();
          resultReceived_Aggregate += resultSize;
          resultReceived_TESTFUNCTION3 += resultSize;
          noOfExecutionCalls_Aggregate++;
          noOfExecutionCalls_TESTFUNCTION3++;
          noOfExecutionsCompleted_Aggregate++;
          noOfExecutionsCompleted_TESTFUNCTION3++;

        }
        catch (Exception e) {
          LogWriterUtils.getLogWriter().info("Exception : " + e.getMessage());
          e.printStackTrace();
          fail("Test failed after the put operation");
        }
        return Boolean.TRUE;
      }
    };
    client.invoke(PopulateRegionAndExecuteFunctions);

    SerializableCallable checkStatsOnClient = new SerializableCallable(
        "checkStatsOnClient") {
      public Object call() throws Exception {
        // checks for the aggregate stats
        InternalDistributedSystem iDS = (InternalDistributedSystem)cache.getDistributedSystem();
        FunctionServiceStats functionServiceStats = iDS
            .getFunctionServiceStats();
        waitNoFunctionsRunning(functionServiceStats);

        assertEquals(noOfExecutionCalls_Aggregate, functionServiceStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_Aggregate, functionServiceStats
            .getFunctionExecutionsCompleted());
        assertTrue(functionServiceStats.getResultsReceived() >= resultReceived_Aggregate);

        LogWriterUtils.getLogWriter().info("Calling FunctionStats for  TEST_FUNCTION2 :");
        FunctionStats functionStats = FunctionStats.getFunctionStats(TestFunction.TEST_FUNCTION2, iDS);
        LogWriterUtils.getLogWriter().info("Called FunctionStats for  TEST_FUNCTION2 :");
        assertEquals(noOfExecutionCalls_TESTFUNCTION2, functionStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_TESTFUNCTION2, functionStats
            .getFunctionExecutionsCompleted());
        assertTrue(functionStats.getResultsReceived() >= resultReceived_TESTFUNCTION2);

        functionStats = FunctionStats.getFunctionStats(TestFunction.TEST_FUNCTION3, iDS);
        assertEquals(noOfExecutionCalls_TESTFUNCTION3, functionStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_TESTFUNCTION3, functionStats
            .getFunctionExecutionsCompleted());
        assertTrue(functionStats.getResultsReceived() >= resultReceived_TESTFUNCTION3);

        return Boolean.TRUE;
      }
    };
    
    client.invoke(checkStatsOnClient);

    SerializableCallable checkStatsOnServer = new SerializableCallable(
        "checkStatsOnClient") {
      public Object call() throws Exception {
        // checks for the aggregate stats
        InternalDistributedSystem iDS = (InternalDistributedSystem)cache
            .getDistributedSystem();
        FunctionServiceStats functionServiceStats = iDS
            .getFunctionServiceStats();
        waitNoFunctionsRunning(functionServiceStats);

        //functions are executed 3 times
        noOfExecutionCalls_Aggregate +=3;
        assertTrue(functionServiceStats
            .getFunctionExecutionCalls() >= noOfExecutionCalls_Aggregate);
        noOfExecutionsCompleted_Aggregate +=3;
        assertTrue(functionServiceStats
            .getFunctionExecutionsCompleted() >= noOfExecutionsCompleted_Aggregate);
        
        FunctionStats functionStats = FunctionStats.getFunctionStats(TestFunction.TEST_FUNCTION2, iDS);
        //TEST_FUNCTION2 is executed twice
        noOfExecutionCalls_TESTFUNCTION2 +=2;
        assertTrue(functionStats
            .getFunctionExecutionCalls() >= noOfExecutionCalls_TESTFUNCTION2);
        noOfExecutionsCompleted_TESTFUNCTION2 += 2;
        assertTrue(functionStats
            .getFunctionExecutionsCompleted() >= noOfExecutionsCompleted_TESTFUNCTION2);

        functionStats = FunctionStats.getFunctionStats(TestFunction.TEST_FUNCTION3, iDS);
        //TEST_FUNCTION3 is executed once
        noOfExecutionCalls_TESTFUNCTION3 +=1;
        assertTrue(functionStats
            .getFunctionExecutionCalls() >= noOfExecutionCalls_TESTFUNCTION3);
        noOfExecutionsCompleted_TESTFUNCTION3 +=1;
        assertTrue(functionStats
            .getFunctionExecutionsCompleted() >= noOfExecutionsCompleted_TESTFUNCTION3);

        return Boolean.TRUE;
      }
    };
    
    server1.invoke(checkStatsOnServer);
    server2.invoke(checkStatsOnServer);
    server3.invoke(checkStatsOnServer);
  }

  /*
   * 1-client 3-Servers
   * server1 : Replicate
   * server2 : Replicate
   * server3 : Replicate
   * client : Empty  
   * Function : TEST_FUNCTION2 
   * Execution of the function on serverRegion with set multiple keys as the
   * routing object and using the name of the function
   * 
   * On server side, function execution calls should be equal to the no of
   * function executions completed.
   */
  public void testClientServerDistributedRegionFunctionExecutionStats() {
     
    final String regionName = "FunctionServiceStatsDUnitTest";
    SerializableCallable createCahenServer = new SerializableCallable(
        "createCahenServer") {
      public Object call() throws Exception {
        try {
          Properties props = new Properties();
          DistributedSystem ds = getSystem(props);
          assertNotNull(ds);
          ds.disconnect();
          ds = getSystem(props);
          cache = CacheFactory.create(ds);
          LogWriterUtils.getLogWriter().info("Created Cache on Server");
          assertNotNull(cache);
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);
          assertNotNull(cache);
          Region region = cache.createRegion(regionName, factory.create());
          LogWriterUtils.getLogWriter().info("Region Created :" + region);
          assertNotNull(region);
          for (int i = 1; i <= 200; i++) {
            region.put("execKey-" + i, new Integer(i));
          }
          CacheServer server = cache.addCacheServer();
          assertNotNull(server);
          int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
          server.setPort(port);
          try {
            server.start();
          }
          catch (IOException e) {
            Assert.fail("Failed to start the Server", e);
          }
          assertTrue(server.isRunning());
          return new Integer(server.getPort());
        }
        catch (Exception e) {
          Assert.fail(
              "FunctionServiceStatsDUnitTest#createCache() Failed while creating the cache",
              e);
          throw e;
        }
      }
    };
    final Integer port1 = (Integer)server1.invoke(createCahenServer);
    final Integer port2 = (Integer)server2.invoke(createCahenServer);
    final Integer port3 = (Integer)server3.invoke(createCahenServer);

    SerializableCallable createCaheInClient = new SerializableCallable(
        "createCaheInClient") {
      public Object call() throws Exception {
        try {
          Properties props = new Properties();
          props.put("mcast-port", "0");
          props.put("locators", "");
          DistributedSystem ds = getSystem(props);
          assertNotNull(ds);
          ds.disconnect();
          ds = getSystem(props);
          cache = CacheFactory.create(ds);
          LogWriterUtils.getLogWriter().info("Created Cache on Client");
          assertNotNull(cache);


          CacheServerTestUtil.disableShufflingOfEndpoints();
          Pool p;
          try {
            p = PoolManager.createFactory().addServer("localhost", port1.intValue())
                .addServer("localhost", port2.intValue()).addServer("localhost",
                    port3.intValue())
                .setPingInterval(250).setSubscriptionEnabled(false)
                .setSubscriptionRedundancy(-1).setReadTimeout(2000)
                .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
                .setRetryAttempts(3).create(
                    "FunctionServiceStatsDUnitTest_pool");
          }
          finally {
            CacheServerTestUtil.enableShufflingOfEndpoints();
          }
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          factory.setDataPolicy(DataPolicy.EMPTY);
          factory.setPoolName(p.getName());
          assertNotNull(cache);
          Region region = cache.createRegion(regionName, factory.create());
          LogWriterUtils.getLogWriter().info("Client Region Created :" + region);
          assertNotNull(region);
          for (int i = 1; i <= 200; i++) {
            region.put("execKey-" + i, new Integer(i));
          }
          return Boolean.TRUE;
        }
        catch (Exception e) {
          Assert.fail(
              "FunctionServiceStatsDUnitTest#createCache() Failed while creating the cache",
              e);
          throw e;
        }
      }
    };
    client.invoke(createCaheInClient);

    client.invoke(initializeStats);
    server1.invoke(initializeStats);
    server2.invoke(initializeStats);
    server3.invoke(initializeStats);
    
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION2);
    registerFunctionAtServer(function);
    function = new TestFunction(true, TestFunction.TEST_FUNCTION3);
    registerFunctionAtServer(function);

    SerializableCallable ExecuteFunctions = new SerializableCallable(
        "PopulateRegionAndExecuteFunctions") {
      public Object call() throws Exception {
        Function function2 = new TestFunction(true, TestFunction.TEST_FUNCTION2);
        FunctionService.registerFunction(function2);
        Function function3 = new TestFunction(true, TestFunction.TEST_FUNCTION3);
        FunctionService.registerFunction(function3); 
        Region region = cache.getRegion(regionName);
        Set filter = new HashSet();
        for (int i = 100; i < 120; i++) {
          filter.add("execKey-" + i);
        }
        
        try {
          noOfExecutionCalls_Aggregate++;
          noOfExecutionCalls_TESTFUNCTION2++;
          List list = (List)FunctionService.onRegion(region).withFilter(filter)
              .execute(function2).getResult();
          noOfExecutionsCompleted_Aggregate++;
          noOfExecutionsCompleted_TESTFUNCTION2++;
          int size = list.size();
          resultReceived_Aggregate += size;
          resultReceived_TESTFUNCTION2 += size;
          
          noOfExecutionCalls_Aggregate++;
          noOfExecutionCalls_TESTFUNCTION2++;
          list = (List)FunctionService.onRegion(region).withFilter(filter)
          .execute(function2).getResult();
          noOfExecutionsCompleted_Aggregate++;
          noOfExecutionsCompleted_TESTFUNCTION2++;
          size = list.size();
          resultReceived_Aggregate += size;
          resultReceived_TESTFUNCTION2 += size;

          return Boolean.TRUE;
        }
        catch (FunctionException e) {
          e.printStackTrace();
          Assert.fail("test failed due to", e);
          throw e;
        }
        catch (Exception e) {
          e.printStackTrace();
          Assert.fail("test failed due to", e);
          throw e;
        }
      
      }
    };
    client.invoke(ExecuteFunctions);

    SerializableCallable checkStatsOnClient = new SerializableCallable(
        "checkStatsOnClient") {
      public Object call() throws Exception {
        // checks for the aggregate stats
        InternalDistributedSystem iDS = (InternalDistributedSystem)cache
            .getDistributedSystem();
        FunctionServiceStats functionServiceStats = iDS
            .getFunctionServiceStats();
        waitNoFunctionsRunning(functionServiceStats);

        assertEquals(noOfExecutionCalls_Aggregate, functionServiceStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_Aggregate, functionServiceStats
            .getFunctionExecutionsCompleted());
        assertEquals(resultReceived_Aggregate, functionServiceStats
            .getResultsReceived());
        
        FunctionStats functionStats = FunctionStats.getFunctionStats(TestFunction.TEST_FUNCTION2, iDS);
        assertEquals(noOfExecutionCalls_TESTFUNCTION2, functionStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_TESTFUNCTION2, functionStats
            .getFunctionExecutionsCompleted());
        assertEquals(resultReceived_TESTFUNCTION2, functionStats
            .getResultsReceived());
        
        return Boolean.TRUE;
      }
    };
    client.invoke(checkStatsOnClient);
  }
  
  
  /*
   * Execution of the function on server using the name of the function
   * TEST_FUNCTION1
   * TEST_FUNCTION5
   * On client side, the no of result received should equal to the no of function execution calls.
   * On server side, function execution calls should be equal to the no of function executions completed. 
   */
  public void testClientServerwithoutRegion() {
    createClientServerScenarionWithoutRegion();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
    registerFunctionAtServer(function);
    function = new TestFunction(true, TestFunction.TEST_FUNCTION5);
    registerFunctionAtServer(function);
    isByName = new Boolean(true);

    client.invoke(initializeStats);
    server1.invoke(initializeStats);
    server2.invoke(initializeStats);
    server3.invoke(initializeStats);

    SerializableCallable ExecuteFunction = new SerializableCallable(
        "ExecuteFunction") {
      public Object call() throws Exception {
        DistributedSystem.setThreadsSocketPolicy(false);
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
        FunctionService.registerFunction(function);
        Execution member = FunctionService.onServers(pool);

        try {
          ResultCollector rs = member.withArgs(Boolean.TRUE).execute(
              function.getId());
          int size = ((List)rs.getResult()).size();
          resultReceived_Aggregate += size;
          noOfExecutionCalls_Aggregate++;
          noOfExecutionsCompleted_Aggregate++;
          resultReceived_TESTFUNCTION1 += size;
          noOfExecutionCalls_TESTFUNCTION1++;
          noOfExecutionsCompleted_TESTFUNCTION1++;
        }
        catch (Exception ex) {
          ex.printStackTrace();
          LogWriterUtils.getLogWriter().info("Exception : ", ex);
          fail("Test failed after the execute operation nn TRUE");
        }
        function = new TestFunction(true, TestFunction.TEST_FUNCTION5);
        FunctionService.registerFunction(function);
        try {
          final HashSet testKeysSet = new HashSet();
          for (int i = 0; i < 20; i++) {
            testKeysSet.add("execKey-" + i);
          }
          ResultCollector rs = member.withArgs("Success").execute(
              function.getId());
          int size = ((List)rs.getResult()).size();
          resultReceived_Aggregate += size;
          noOfExecutionCalls_Aggregate++;
          noOfExecutionsCompleted_Aggregate++;
          resultReceived_TESTFUNCTION5 += size;
          noOfExecutionCalls_TESTFUNCTION5++;
          noOfExecutionsCompleted_TESTFUNCTION5++;
        }
        catch (Exception ex) {
          ex.printStackTrace();
          LogWriterUtils.getLogWriter().info("Exception : ", ex);
          fail("Test failed after the execute operationssssss");
        }
        return Boolean.TRUE;
      }
    };
    client.invoke(ExecuteFunction);

    SerializableCallable checkStatsOnClient = new SerializableCallable(
        "checkStatsOnClient") {
      public Object call() throws Exception {
        // checks for the aggregate stats
        InternalDistributedSystem iDS = (InternalDistributedSystem)cache
            .getDistributedSystem();
        FunctionServiceStats functionServiceStats = iDS
            .getFunctionServiceStats();
        waitNoFunctionsRunning(functionServiceStats);

        assertEquals(noOfExecutionCalls_Aggregate, functionServiceStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_Aggregate, functionServiceStats
            .getFunctionExecutionsCompleted());
        assertEquals(resultReceived_Aggregate, functionServiceStats
                    .getResultsReceived());
        
        FunctionStats functionStats = FunctionStats.getFunctionStats(TestFunction.TEST_FUNCTION1, iDS);
        assertEquals(noOfExecutionCalls_TESTFUNCTION1, functionStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_TESTFUNCTION1, functionStats
            .getFunctionExecutionsCompleted());
        assertEquals(resultReceived_TESTFUNCTION1, functionStats
            .getResultsReceived());

        functionStats = FunctionStats.getFunctionStats(TestFunction.TEST_FUNCTION5, iDS);
        assertEquals(noOfExecutionCalls_TESTFUNCTION5, functionStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_TESTFUNCTION5, functionStats
            .getFunctionExecutionsCompleted());
        assertEquals(resultReceived_TESTFUNCTION5, functionStats
            .getResultsReceived());
        
        return Boolean.TRUE;
      }
    };

    client.invoke(checkStatsOnClient);

    SerializableCallable checkStatsOnServer = new SerializableCallable(
        "checkStatsOnClient") {
      public Object call() throws Exception {
        // checks for the aggregate stats
        InternalDistributedSystem iDS = (InternalDistributedSystem)cache
            .getDistributedSystem();
        FunctionServiceStats functionServiceStats = iDS
            .getFunctionServiceStats();
        waitNoFunctionsRunning(functionServiceStats);

        // functions are executed 2 times
        noOfExecutionCalls_Aggregate += 2;
        assertEquals(noOfExecutionCalls_Aggregate, functionServiceStats
            .getFunctionExecutionCalls());
        noOfExecutionsCompleted_Aggregate += 2;
        // this check is time sensitive, so allow it to fail a few times
        // before giving up
        for (int i=0; i<10; i++) {
          try {
            assertEquals(noOfExecutionsCompleted_Aggregate, functionServiceStats
              .getFunctionExecutionsCompleted());
          } catch (RuntimeException r) {
            if (i==9) {
              throw r;
            }
            try { Thread.sleep(1000); } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              throw r;
            }
          }
        }
        
        FunctionStats functionStats = FunctionStats.getFunctionStats(TestFunction.TEST_FUNCTION1, iDS);
        // TEST_FUNCTION1 is executed once
        noOfExecutionCalls_TESTFUNCTION1 += 1;
        assertEquals(noOfExecutionCalls_TESTFUNCTION1, functionStats
            .getFunctionExecutionCalls());
        noOfExecutionsCompleted_TESTFUNCTION1 += 1;
        assertEquals(noOfExecutionsCompleted_TESTFUNCTION1, functionStats
            .getFunctionExecutionsCompleted());

        functionStats = FunctionStats.getFunctionStats(TestFunction.TEST_FUNCTION5, iDS);
        // TEST_FUNCTION5 is executed once
        noOfExecutionCalls_TESTFUNCTION5 += 1;
        assertEquals(noOfExecutionCalls_TESTFUNCTION5, functionStats
            .getFunctionExecutionCalls());
        noOfExecutionsCompleted_TESTFUNCTION5 += 1;
        assertEquals(noOfExecutionsCompleted_TESTFUNCTION5, functionStats
            .getFunctionExecutionsCompleted());

        return Boolean.TRUE;
      }
    };

    server1.invoke(checkStatsOnServer);
    server2.invoke(checkStatsOnServer);
    server3.invoke(checkStatsOnServer);
  }
  
  public void testP2PDummyExecutionStats()
  throws Exception {
    Host host = Host.getHost(0);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    final VM accessor = host.getVM(3);
    SerializableCallable closeDistributedSystem = new SerializableCallable(
        "closeDistributedSystem") {
      public Object call() throws Exception {
        if (getCache() != null && !getCache().isClosed()) {
          getCache().close();
          getCache().getDistributedSystem().disconnect();
        }
        return Boolean.TRUE;
      }
    };
    accessor.invoke(closeDistributedSystem);
    datastore0.invoke(closeDistributedSystem);
    datastore1.invoke(closeDistributedSystem);
    datastore2.invoke(closeDistributedSystem);
}

  
  /**
   * Ensure that the execution is happening all the PR as a whole
   * 
   * Function Execution will not take place on accessor, accessor will onlu receive the resultsReceived.
   * On datastore, no of function execution calls should be equal to the no of function execution calls from the accessor.
   * @throws Exception
   */
  public void testP2PPartitionedRegionsFunctionExecutionStats()
      throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    final VM accessor = host.getVM(3);
    
    datastore0.invoke(initializeStats);
    datastore1.invoke(initializeStats);
    datastore2.invoke(initializeStats);
    accessor.invoke(initializeStats);
    
    accessor.invoke(new SerializableCallable("Create PR") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 0);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        pa.setTotalNumBuckets(17);
        raf.setPartitionAttributes(pa);
        
        PartitionedRegion pr = (PartitionedRegion)getCache().createRegion(
            rName, raf.create());
        return Boolean.TRUE;
      }
    });
    
    SerializableCallable dataStoreCreate = new SerializableCallable(
        "Create PR with Function Factory") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 10);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        pa.setTotalNumBuckets(17);
        raf.setPartitionAttributes(pa);
        PartitionedRegion pr = (PartitionedRegion)getCache().createRegion(
            rName, raf.create());
        Function function = new TestFunction(true,TestFunction.TEST_FUNCTION2);
        FunctionService.registerFunction(function);
        function = new TestFunction(true, TestFunction.TEST_FUNCTION3);
        FunctionService.registerFunction(function);
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(dataStoreCreate);
    datastore1.invoke(dataStoreCreate);
    datastore2.invoke(dataStoreCreate);

    accessor.invoke(new SerializableCallable(
        "Create data, invoke exectuable") {
      public Object call() throws Exception {
        
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);
        DistributedSystem.setThreadsSocketPolicy(false);
        final HashSet testKeys = new HashSet();
        for (int i = (pr.getTotalNumberOfBuckets() * 3); i > 0; i--) {
          testKeys.add("execKey-" + i);
        }
        int j = 0;
        for (Iterator i = testKeys.iterator(); i.hasNext();) {
          Integer val = new Integer(j++);
          pr.put(i.next(), val);
        }
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION2);
        FunctionService.registerFunction(function);
        Execution dataSet = FunctionService.onRegion(pr);
        ResultCollector rc1 = dataSet.withArgs(Boolean.TRUE)
            .execute(function);
        int size = ((List)rc1.getResult()).size();
        resultReceived_Aggregate += size;
        resultReceived_TESTFUNCTION2 += size;
        
        rc1 = dataSet.withArgs(testKeys)
        .execute(function);
        size = ((List)rc1.getResult()).size();
        resultReceived_Aggregate += size;
        resultReceived_TESTFUNCTION2 += size;
        
        function = new TestFunction(true, TestFunction.TEST_FUNCTION3);
        FunctionService.registerFunction(function);
        rc1 = dataSet.withArgs(Boolean.TRUE)
        .execute(function);
        size = ((List)rc1.getResult()).size();
        resultReceived_Aggregate += size;
        resultReceived_TESTFUNCTION3 += size;
        
        return Boolean.TRUE;
      }
    });
    
    accessor.invoke(new SerializableCallable(
        "checkFunctionExecutionStatsForAccessor") {
      public Object call() throws Exception {
        InternalDistributedSystem iDS = ((InternalDistributedSystem)getCache()
            .getDistributedSystem());
        FunctionServiceStats functionServiceStats = iDS.getFunctionServiceStats();
        waitNoFunctionsRunning(functionServiceStats);

        assertEquals(noOfExecutionCalls_Aggregate, functionServiceStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_Aggregate, functionServiceStats
            .getFunctionExecutionsCompleted());
        assertEquals(resultReceived_Aggregate, functionServiceStats.getResultsReceived());
        
        FunctionStats functionStats = FunctionStats.getFunctionStats(TestFunction.TEST_FUNCTION2, iDS);
        assertEquals(noOfExecutionCalls_TESTFUNCTION2, functionStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_TESTFUNCTION2, functionStats
            .getFunctionExecutionsCompleted());
        assertEquals(resultReceived_TESTFUNCTION2, functionStats
            .getResultsReceived());
        
        functionStats = FunctionStats.getFunctionStats(TestFunction.TEST_FUNCTION3, iDS);
        assertEquals(noOfExecutionCalls_TESTFUNCTION3, functionStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_TESTFUNCTION3, functionStats
            .getFunctionExecutionsCompleted());
        assertEquals(resultReceived_TESTFUNCTION3, functionStats
            .getResultsReceived());
        
        return Boolean.TRUE;
      }
    });

    SerializableCallable checkFunctionExecutionStatsForDataStore = new SerializableCallable(
        "checkFunctionExecutionStatsForDataStore") {
      public Object call() throws Exception {
        InternalDistributedSystem iDS = ((InternalDistributedSystem)getCache()
            .getDistributedSystem());
        //3 Function Executions took place 
        FunctionServiceStats functionServiceStats = iDS.getFunctionServiceStats();
        waitNoFunctionsRunning(functionServiceStats);

        noOfExecutionCalls_Aggregate += 3;
        noOfExecutionsCompleted_Aggregate += 3;
        assertEquals(noOfExecutionCalls_Aggregate, functionServiceStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_Aggregate, functionServiceStats
            .getFunctionExecutionsCompleted());
        
        FunctionStats functionStats = FunctionStats.getFunctionStats(TestFunction.TEST_FUNCTION2, iDS);
        //TEST_FUNCTION2 is executed twice
        noOfExecutionCalls_TESTFUNCTION2 +=2;
        assertEquals(noOfExecutionCalls_TESTFUNCTION2, functionStats
            .getFunctionExecutionCalls());
        noOfExecutionsCompleted_TESTFUNCTION2 += 2;
        assertEquals(noOfExecutionsCompleted_TESTFUNCTION2, functionStats
            .getFunctionExecutionsCompleted());

        functionStats = FunctionStats.getFunctionStats(TestFunction.TEST_FUNCTION3, iDS);
        //TEST_FUNCTION3 is executed once
        noOfExecutionCalls_TESTFUNCTION3 +=1;
        assertEquals(noOfExecutionCalls_TESTFUNCTION3, functionStats
            .getFunctionExecutionCalls());
        noOfExecutionsCompleted_TESTFUNCTION3 +=1;
        assertEquals(noOfExecutionsCompleted_TESTFUNCTION3, functionStats
            .getFunctionExecutionsCompleted());
        
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(checkFunctionExecutionStatsForDataStore);
    datastore1.invoke(checkFunctionExecutionStatsForDataStore);
    datastore2.invoke(checkFunctionExecutionStatsForDataStore);
    
    SerializableCallable closeDistributedSystem = new SerializableCallable(
        "closeDistributedSystem") {
      public Object call() throws Exception {
        if (getCache() != null && !getCache().isClosed()) {
          getCache().close();
          getCache().getDistributedSystem().disconnect();
        }
        return Boolean.TRUE;
      }
    };
     accessor.invoke(closeDistributedSystem);
     datastore0.invoke(closeDistributedSystem);
     datastore1.invoke(closeDistributedSystem);
     datastore2.invoke(closeDistributedSystem);
  }
  
  /**
   * Test the function execution statistics in case of the distributed Region P2P
   * DataStore0 is with Empty datapolicy 
   */
  
  public void testP2PDistributedRegionFunctionExecutionStats() {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    final VM datastore3 = host.getVM(3);

    datastore0.invoke(initializeStats);
    datastore1.invoke(initializeStats);
    datastore2.invoke(initializeStats);
    datastore3.invoke(initializeStats);
    
    SerializableCallable createAndPopulateRegionWithEmpty = new SerializableCallable(
    "Create PR with Function Factory") {
      public Object call() throws Exception {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.EMPTY);
        Region region = getCache().createRegion(rName, factory.create());
        LogWriterUtils.getLogWriter().info("Region Created :" + region);
        assertNotNull(region);
        FunctionService.registerFunction(new TestFunction(true, TestFunction.TEST_FUNCTION2));
        for (int i = 1; i <= 200; i++) {
          region.put("execKey-" + i, new Integer(i));
        }
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(createAndPopulateRegionWithEmpty);
    
    SerializableCallable createAndPopulateRegionWithReplicate = new SerializableCallable(
    "Create PR with Function Factory") {
      public Object call() throws Exception {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.REPLICATE);
        Region region = getCache().createRegion(rName, factory.create());
        LogWriterUtils.getLogWriter().info("Region Created :" + region);
        assertNotNull(region);
        FunctionService.registerFunction(new TestFunction(true, TestFunction.TEST_FUNCTION2));
        for (int i = 1; i <= 200; i++) {
          region.put("execKey-" + i, new Integer(i));
        }
        return Boolean.TRUE;
      }
    };
    
    datastore1.invoke(createAndPopulateRegionWithReplicate);
    datastore2.invoke(createAndPopulateRegionWithReplicate);
    datastore3.invoke(createAndPopulateRegionWithReplicate);
    
    SerializableCallable executeFunction = new SerializableCallable(
    "ExecuteFunction from Normal Region") {
      public Object call() throws Exception {
        Region region = getCache().getRegion(rName);
        try {
          List list = (List)FunctionService.onRegion(region).withArgs(
              Boolean.TRUE).execute(TestFunction.TEST_FUNCTION2).getResult();
          // this is the Distributed Region with Empty Data policy.
          // therefore no function execution takes place here. it only receives the results.
          resultReceived_Aggregate += list.size();
          assertEquals(resultReceived_Aggregate, ((InternalDistributedSystem)getCache()
              .getDistributedSystem()).getFunctionServiceStats().getResultsReceived());
          
          resultReceived_TESTFUNCTION2 += list.size();
          assertEquals(resultReceived_TESTFUNCTION2, ((InternalDistributedSystem)getCache()
              .getDistributedSystem()).getFunctionServiceStats().getResultsReceived());
          
          return Boolean.TRUE;
        }
        catch (FunctionException e) {
          e.printStackTrace();
          Assert.fail("test failed due to", e);
          return Boolean.FALSE;
        }
        catch (Exception e) {
          e.printStackTrace();
          Assert.fail("test failed due to", e);
          return Boolean.FALSE;
        }
        
      }
    };
    datastore0.invoke(executeFunction);
    // there is a replicated region on 3 nodes so we cannot predict on which
    // node the function execution will take place
    // so i have avoided that check.
    
    SerializableCallable closeDistributedSystem = new SerializableCallable(
        "closeDistributedSystem") {
      public Object call() throws Exception {
        if (getCache() != null && !getCache().isClosed()) {
          getCache().close();
          getCache().getDistributedSystem().disconnect();
        }
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(closeDistributedSystem);
    datastore1.invoke(closeDistributedSystem);
    datastore2.invoke(closeDistributedSystem);
    datastore3.invoke(closeDistributedSystem);
  }
  
  
  /**
   * Test the execution of function on all memebers haveResults = true
   * 
   * member1 calls for the function executions sp the results received on
   * memeber 1 should be equal to the no of function execution calls. Function
   * Execution should happen on all other members too. so the no of function
   * execution calls and no of function executions completed should be equal tio
   * the no of functions from member 1
   * 
   * @throws Exception
   */
  
  public void testP2PMembersFunctionExecutionStats()
      throws Exception {
    Host host = Host.getHost(0);
    VM member1 = host.getVM(0);
    VM member2 = host.getVM(1);
    VM member3 = host.getVM(2);
    VM member4 = host.getVM(3);
    
    SerializableCallable connectToDistributedSystem = new SerializableCallable(
    "connectToDistributedSystem") {
      public Object call() throws Exception {
        Properties props = new Properties();
        try {
          ds = getSystem(props);
          assertNotNull(ds);
        }
        catch (Exception e) {
          Assert.fail("Failed while creating the Distribued System", e);
        }
        return Boolean.TRUE;
      }
    };
    member1.invoke(connectToDistributedSystem);
    member2.invoke(connectToDistributedSystem);
    member3.invoke(connectToDistributedSystem);
    member4.invoke(connectToDistributedSystem);
    
    member1.invoke(initializeStats);
    member2.invoke(initializeStats);
    member3.invoke(initializeStats);
    member4.invoke(initializeStats);
    
    final int noOfMembers = 1;
    final Function inlineFunction = new FunctionAdapter() {
      public void execute (FunctionContext context) {
        if (context.getArguments() instanceof String) {
          context.getResultSender().lastResult("Success");
        }
        else {
          context.getResultSender().lastResult("Failure");
        }
      }

      public String getId() {
        return getClass().getName();
      }

      public boolean hasResult() {
        return true;
      }
    };
    
    member1.invoke(new SerializableCallable(
        "excuteOnMembers_InlineFunction") {
      public Object call() throws Exception {
      
        assertNotNull(ds);
        Execution memberExecution = null;
        DistributedMember localmember = ds.getDistributedMember();
        memberExecution = FunctionService.onMember(ds, localmember);

        memberExecution.withArgs("Key");
        try {
          ResultCollector rc = memberExecution.execute(inlineFunction);
          int size = ((List)rc.getResult()).size();
          resultReceived_Aggregate += size;
          noOfExecutionCalls_Aggregate++;
          noOfExecutionsCompleted_Aggregate++;
          resultReceived_Inline += size;
          noOfExecutionCalls_Inline++;
          noOfExecutionsCompleted_Inline++;
          
        }
        catch (Exception e) {
          LogWriterUtils.getLogWriter().info("Exception Occured : " + e.getMessage());
          e.printStackTrace();
          Assert.fail("Test failed", e);
        }
        return Boolean.TRUE;
      }
    });
    
    member1.invoke(new SerializableCallable(
        "checkFunctionExecutionStatsForMember1") {
      public Object call() throws Exception {
        FunctionServiceStats functionServiceStats = ds.getFunctionServiceStats();
        waitNoFunctionsRunning(functionServiceStats);

        assertEquals(noOfExecutionCalls_Aggregate, functionServiceStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_Aggregate, functionServiceStats
            .getFunctionExecutionsCompleted());
        assertEquals(resultReceived_Aggregate, functionServiceStats.getResultsReceived());
        
        FunctionStats functionStats = FunctionStats.getFunctionStats(inlineFunction.getId(), ds);
        assertEquals(noOfExecutionCalls_Inline, functionStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_Inline, functionStats
            .getFunctionExecutionsCompleted());
        assertEquals(resultReceived_Inline, functionStats.getResultsReceived());
        return Boolean.TRUE;
      }
    });

    SerializableCallable checkFunctionExecutionStatsForOtherMember = new SerializableCallable(
        "checkFunctionExecutionStatsForOtherMember") {
      public Object call() throws Exception {
        FunctionServiceStats functionServiceStats = ds.getFunctionServiceStats();
        waitNoFunctionsRunning(functionServiceStats);

        // One function Execution took place on there members
        //noOfExecutionCalls_Aggregate++;
        //noOfExecutionsCompleted_Aggregate++;
        assertEquals(noOfExecutionCalls_Aggregate, functionServiceStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_Aggregate, functionServiceStats
            .getFunctionExecutionsCompleted());
        
        FunctionStats functionStats = FunctionStats.getFunctionStats(inlineFunction.getId(), ds);
        //noOfExecutionCalls_Inline++;
        //noOfExecutionsCompleted_Inline++;
        assertEquals(noOfExecutionCalls_Inline, functionStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_Inline, functionStats
            .getFunctionExecutionsCompleted());
        return Boolean.TRUE;
      }
    };
    member2.invoke(checkFunctionExecutionStatsForOtherMember);
    member3.invoke(checkFunctionExecutionStatsForOtherMember);
    member4.invoke(checkFunctionExecutionStatsForOtherMember);
    
    SerializableCallable closeDistributedSystem = new SerializableCallable(
        "closeDistributedSystem") {
      public Object call() throws Exception {
        if (getCache() != null && !getCache().isClosed()) {
          getCache().close();
          getCache().getDistributedSystem().disconnect();
        }
        return Boolean.TRUE;
      }
    };
    member1.invoke(closeDistributedSystem);
    member2.invoke(closeDistributedSystem);
    member3.invoke(closeDistributedSystem);
    member4.invoke(closeDistributedSystem);
  }
  
  /**
   * Test the exception occured while invoking the function execution on all members of DS
   * 
   * Function throws the Exception,
   * The check is added to for the no of function execution execption in datatostore1
   *  
   * @throws Exception
   */
  public void testFunctionExecutionExceptionStatsOnAllNodesPRegion()
      throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore0 = host.getVM(0);
    final VM datastore1 = host.getVM(1);
    final VM datastore2 = host.getVM(2);
    final VM datastore3 = host.getVM(3);

    datastore0.invoke(initializeStats);
    datastore1.invoke(initializeStats);
    datastore2.invoke(initializeStats);
    datastore3.invoke(initializeStats);

    SerializableCallable dataStoreCreate = new SerializableCallable(
        "Create PR with Function Factory") {
      public Object call() throws Exception {
        RegionAttributes ra = PartitionedRegionTestHelper
            .createRegionAttrsForPR(0, 10);
        AttributesFactory raf = new AttributesFactory(ra);
        PartitionAttributesImpl pa = new PartitionAttributesImpl();
        pa.setAll(ra.getPartitionAttributes());
        pa.setTotalNumBuckets(17);
        raf.setPartitionAttributes(pa);
        getCache().createRegion(rName, raf.create());
        Function function = new TestFunction(true, "TestFunctionException");
        FunctionService.registerFunction(function);
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(dataStoreCreate);
    datastore1.invoke(dataStoreCreate);
    datastore2.invoke(dataStoreCreate);
    datastore3.invoke(dataStoreCreate);

    Object o = datastore3.invoke(new SerializableCallable(
        "Create data, invoke exectuable") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);
        DistributedSystem.setThreadsSocketPolicy(false);
        final HashSet testKeys = new HashSet();
        for (int i = (pr.getTotalNumberOfBuckets() * 3); i > 0; i--) {
          testKeys.add("execKey-" + i);
        }
        int j = 0;
        for (Iterator i = testKeys.iterator(); i.hasNext();) {
          Integer key = new Integer(j++);
          pr.put(key, i.next());
        }
        try {
          Function function = new TestFunction(true, "TestFunctionException");
          FunctionService.registerFunction(function);
          Execution dataSet = FunctionService.onRegion(pr);
          ResultCollector rc = dataSet.withArgs(Boolean.TRUE).execute(function.getId());
          // Wait Criterion is added to make sure that the function execution
          // happens on all nodes and all nodes get the FunctionException so that the stats will get incremented,
          WaitCriterion wc = new WaitCriterion() {
            String excuse;

            public boolean done() {
              return false;
            }

            public String description() {
              return excuse;
            }
          };
          Wait.waitForCriterion(wc, 20000, 1000, false);
          rc.getResult();
        }
        catch (Exception expected) {
          return Boolean.TRUE;
        }
        fail("No exception Occured");
        return Boolean.FALSE;
      }
    });
    assertEquals(Boolean.TRUE, o);
    
    SerializableCallable checkFunctionExecutionStatsForDataStore = new SerializableCallable(
        "checkFunctionExecutionStatsForDataStore") {
      public Object call() throws Exception {
        FunctionStats functionStats = FunctionStats
            .getFunctionStats("TestFunctionException", getSystem());
        noOfExecutionCalls_TestFunctionException++;
        noOfExecutionExceptions_TestFunctionException++;
        assertEquals(noOfExecutionCalls_TestFunctionException, functionStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_TestFunctionException,
            functionStats.getFunctionExecutionsCompleted());
        assertEquals(noOfExecutionExceptions_TestFunctionException,
            functionStats.getFunctionExecutionExceptions());

        noOfExecutionCalls_Aggregate++;
        noOfExecutionExceptions_Aggregate++;
        FunctionServiceStats functionServiceStats = ((InternalDistributedSystem)getCache()
            .getDistributedSystem()).getFunctionServiceStats();
        assertEquals(noOfExecutionCalls_Aggregate, functionServiceStats
            .getFunctionExecutionCalls());
        assertEquals(noOfExecutionsCompleted_Aggregate, functionServiceStats
            .getFunctionExecutionsCompleted());
        assertEquals(noOfExecutionExceptions_Aggregate, functionServiceStats
            .getFunctionExecutionExceptions());
        return Boolean.TRUE;
      }
    };
    
    /*datastore0.invoke(checkFunctionExecutionStatsForDataStore);
    datastore1.invoke(checkFunctionExecutionStatsForDataStore);
    datastore2.invoke(checkFunctionExecutionStatsForDataStore);
    datastore3.invoke(checkFunctionExecutionStatsForDataStore);*/

    SerializableCallable closeDistributedSystem = new SerializableCallable(
        "closeDistributedSystem") {
      public Object call() throws Exception {
        if (getCache() != null && !getCache().isClosed()) {
          getCache().close();
          getCache().getDistributedSystem().disconnect();
        }
        return Boolean.TRUE;
      }
    };
    datastore0.invoke(closeDistributedSystem);
    datastore1.invoke(closeDistributedSystem);
    datastore2.invoke(closeDistributedSystem);
    datastore3.invoke(closeDistributedSystem);
  }
  
  private void createScenario() {
    ArrayList commonAttributes = createCommonServerAttributes("TestPartitionedRegion", null, 0, 13, null);
    createClientServerScenarion(commonAttributes, 20, 20, 20);
  }
}
