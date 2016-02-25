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
package com.gemstone.gemfire.rest.internal.web.controllers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.PartitionAttributesImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionTestHelper;
import com.gemstone.gemfire.internal.cache.functions.DistributedRegionFunction;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Dunit Test to validate OnRegion function execution with REST APIs
 * 
 * @author Nilkanth Patel
 * @since 8.0
 */

public class RestAPIOnRegionFunctionExecutionDUnitTest extends RestAPITestBase {

  private static final long serialVersionUID = 1L;

  public static final String REGION_NAME = "DistributedRegionFunctionExecutionDUnitTest";

  public static final String PR_REGION_NAME = "samplePRRegion";

  public static Region region = null;

  public static List<String> restURLs = new ArrayList<String>();

  public static String restEndPoint = null;

  public static String getRestEndPoint() {
    return restEndPoint;
  }

  public static void setRestEndPoint(String restEndPoint) {
    RestAPIOnRegionFunctionExecutionDUnitTest.restEndPoint = restEndPoint;
  }

  public static final Function function = new DistributedRegionFunction();

  public static final Function functionWithNoResultThrowsException = new MyFunctionException();

  public RestAPIOnRegionFunctionExecutionDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    final Host host = Host.getHost(0);
  }

  static class FunctionWithNoLastResult implements Function {
    private static final long serialVersionUID = -1032915440862585532L;
    public static final String Id = "FunctionWithNoLastResult";
    public static int invocationCount;

    @Override
    public void execute(FunctionContext context) {
      invocationCount++;
      InternalDistributedSystem
          .getConnectedInstance()
          .getLogWriter()
          .info(
              "<ExpectedException action=add>did not send last result"
                  + "</ExpectedException>");
      context.getResultSender().sendResult(
          (Serializable) context.getArguments());
    }

    @Override
    public String getId() {
      return Id;
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean optimizeForWrite() {
      return false;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

  static class SampleFunction implements Function {
    private static final long serialVersionUID = -1032915440862585534L;
    public static final String Id = "SampleFunction";
    public static int invocationCount;

    @Override
    public void execute(FunctionContext context) {
      invocationCount++;
      if (context instanceof RegionFunctionContext) {
        RegionFunctionContext rfContext = (RegionFunctionContext) context;
        rfContext.getDataSet().getCache().getLogger()
            .info("Executing function :  TestFunction2.execute " + rfContext);
        if (rfContext.getArguments() instanceof Boolean) {
          /* return rfContext.getArguments(); */
          if (hasResult()) {
            rfContext.getResultSender().lastResult(
                (Serializable) rfContext.getArguments());
          } else {
            rfContext
                .getDataSet()
                .getCache()
                .getLogger()
                .info(
                    "Executing function :  TestFunction2.execute " + rfContext);
            while (true && !rfContext.getDataSet().isDestroyed()) {
              rfContext.getDataSet().getCache().getLogger()
                  .info("For Bug43513 ");
              try {
                Thread.sleep(100);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
              }
            }
          }
        } else if (rfContext.getArguments() instanceof String) {
          String key = (String) rfContext.getArguments();
          if (key.equals("TestingTimeOut")) { // for test
                                              // PRFunctionExecutionDUnitTest#testRemoteMultiKeyExecution_timeout
            try {
              Thread.sleep(2000);
            } catch (InterruptedException e) {
              rfContext.getDataSet().getCache().getLogger()
                  .warning("Got Exception : Thread Interrupted" + e);
            }
          }
          if (PartitionRegionHelper.isPartitionedRegion(rfContext.getDataSet())) {
            /*
             * return
             * (Serializable)PartitionRegionHelper.getLocalDataForContext(
             * rfContext).get(key);
             */
            rfContext.getResultSender().lastResult(
                (Serializable) PartitionRegionHelper.getLocalDataForContext(
                    rfContext).get(key));
          } else {
            rfContext.getResultSender().lastResult(
                (Serializable) rfContext.getDataSet().get(key));
          }
          /* return (Serializable)rfContext.getDataSet().get(key); */
        } else if (rfContext.getArguments() instanceof Set) {
          Set origKeys = (Set) rfContext.getArguments();
          ArrayList vals = new ArrayList();
          for (Object key : origKeys) {
            Object val = PartitionRegionHelper
                .getLocalDataForContext(rfContext).get(key);
            if (val != null) {
              vals.add(val);
            }
          }
          rfContext.getResultSender().lastResult(vals);
          /* return vals; */
        } else if (rfContext.getArguments() instanceof HashMap) {
          HashMap putData = (HashMap) rfContext.getArguments();
          for (Iterator i = putData.entrySet().iterator(); i.hasNext();) {
            Map.Entry me = (Map.Entry) i.next();
            rfContext.getDataSet().put(me.getKey(), me.getValue());
          }
          rfContext.getResultSender().lastResult(Boolean.TRUE);
        } else {
          rfContext.getResultSender().lastResult(Boolean.FALSE);
        }
      } else {
        if (hasResult()) {
          context.getResultSender().lastResult(Boolean.FALSE);
        } else {
          DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
          LogWriter logger = ds.getLogWriter();
          logger.info("Executing in TestFunction on Server : "
              + ds.getDistributedMember() + "with Context : " + context);
          while (ds.isConnected()) {
            logger
                .fine("Just executing function in infinite loop for Bug43513");
            try {
              Thread.sleep(250);
            } catch (InterruptedException e) {
              return;
            }
          }
        }
      }
    }

    @Override
    public String getId() {
      return Id;
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean optimizeForWrite() {
      return false;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

  private int getInvocationCount(VM vm) {
    return (Integer) vm.invoke(new SerializableCallable() {
      /**
       * 
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Object call() throws Exception {
        SampleFunction f = (SampleFunction) FunctionService
            .getFunction(SampleFunction.Id);
        int count = f.invocationCount;
        f.invocationCount = 0;
        return count;
      }
    });
  }

  private void verifyAndResetInvocationCount(VM vm, final int count) {
    vm.invoke(new SerializableCallable() {
      /**
       * 
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Object call() throws Exception {
        SampleFunction f = (SampleFunction) FunctionService
            .getFunction(SampleFunction.Id);
        assertEquals(count, f.invocationCount);
        // assert succeeded, reset count
        f.invocationCount = 0;
        return null;
      }
    });
  }

  public static void createPeer(DataPolicy policy) {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(policy);
    assertNotNull(cache);
    region = cache.createRegion(REGION_NAME, factory.create());
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Region Created :" + region);
    assertNotNull(region);
  }

  public static boolean createPeerWithPR() {
    RegionAttributes ra = PartitionedRegionTestHelper.createRegionAttrsForPR(0,
        10);
    AttributesFactory raf = new AttributesFactory(ra);
    PartitionAttributesImpl pa = new PartitionAttributesImpl();
    pa.setAll(ra.getPartitionAttributes());
    pa.setTotalNumBuckets(17);
    raf.setPartitionAttributes(pa);

    if (cache == null || cache.isClosed()) {
      // Cache not available
    }
    assertNotNull(cache);

    region = cache.createRegion(PR_REGION_NAME, raf.create());
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Region Created :" + region);
    assertNotNull(region);
    return Boolean.TRUE;
  }

  public static void populateRegion() {
    assertNotNull(cache);
    region = cache.getRegion(REGION_NAME);
    assertNotNull(region);
    for (int i = 1; i <= 200; i++) {
      region.put("execKey-" + i, new Integer(i));
    }
  }

  public static void populatePRRegion() {
    assertNotNull(cache);
    region = cache.getRegion(REGION_NAME);

    PartitionedRegion pr = (PartitionedRegion) cache.getRegion(PR_REGION_NAME);
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
    // Assert there is data in each bucket
    for (int bid = 0; bid < pr.getTotalNumberOfBuckets(); bid++) {
      assertTrue(pr.getBucketKeys(bid).size() > 0);
    }
  }

  public static void populateRRRegion() {
    assertNotNull(cache);
    region = cache.getRegion(REGION_NAME);
    assertNotNull(region);

    final HashSet testKeys = new HashSet();
    for (int i = 17 * 3; i > 0; i--) {
      testKeys.add("execKey-" + i);
    }
    int j = 0;
    for (Iterator i = testKeys.iterator(); i.hasNext();) {
      Integer val = new Integer(j++);
      region.put(i.next(), val);
    }

  }

  public static void executeFunction_NoLastResult(String regionName) {

    try {
      CloseableHttpClient httpclient = HttpClients.createDefault();
      CloseableHttpResponse response = null;
      Random randomGenerator = new Random();
      int index = randomGenerator.nextInt(restURLs.size());
      HttpPost post = new HttpPost(restURLs.get(index) + "/functions/"
          + "FunctionWithNoLastResult" + "?onRegion=" + regionName);
      post.addHeader("Content-Type", "application/json");
      post.addHeader("Accept", "application/json");
      response = httpclient.execute(post);
    } catch (Exception e) {
      throw new RuntimeException("unexpected exception", e);
    }

  }

  public static void executeFunctionThroughRestCall(String regionName) {
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Entering executeFunctionThroughRestCall");
    try {
      CloseableHttpClient httpclient = HttpClients.createDefault();
      CloseableHttpResponse response = null;
      Random randomGenerator = new Random();
      int index = randomGenerator.nextInt(restURLs.size());
      
      HttpPost post = new HttpPost(restURLs.get(index) + "/functions/"
          + "SampleFunction" + "?onRegion=" + regionName);
      post.addHeader("Content-Type", "application/json");
      post.addHeader("Accept", "application/json");
      
      com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Request: POST " + post.toString());
      response = httpclient.execute(post);
      com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Response: POST " + response.toString());
      
      assertEquals(response.getStatusLine().getStatusCode(), 200);
      assertNotNull(response.getEntity());
    } catch (Exception e) {
      throw new RuntimeException("unexpected exception", e);
    }
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Exiting executeFunctionThroughRestCall");

  }

  private void registerFunction(VM vm) {
    vm.invoke(new SerializableCallable() {
      private static final long serialVersionUID = 1L;
      @Override
      public Object call() throws Exception {
        FunctionService.registerFunction(new FunctionWithNoLastResult());
        return null;
      }
    });
  }

  private void registerSampleFunction(VM vm) {
    vm.invoke(new SerializableCallable() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object call() throws Exception {
        FunctionService.registerFunction(new SampleFunction());
        return null;
      }
    });
  }

  public void __testOnRegionExecutionOnDataPolicyEmpty_NoLastResult() {
    // Step-1 : create cache on each VM, this will start HTTP service in
    // embedded mode and deploy REST APIs web app on it.

    fail("This test is trying to invoke non existent methods");
//    String url1 = (String) vm3.invoke(() -> createCacheInVm( vm3 ));
//    restURLs.add(url1);
//
//    String url2 = (String) vm0.invoke(() -> createCacheInVm( vm0 ));
//    restURLs.add(url2);
//
//    String url3 = (String) vm1.invoke(() -> createCacheInVm( vm1 ));
//    restURLs.add(url3);
//
//    String url4 = (String) vm2.invoke(() -> createCacheInVm( vm2 ));
//    restURLs.add(url4);

    // Step-2: Register function in all VMs
    registerFunction(vm3);
    registerFunction(vm0);
    registerFunction(vm1);
    registerFunction(vm2);

    // Step-3: Create and configure Region on all VMs
    vm3.invoke(() -> createPeer( DataPolicy.EMPTY ));
    vm0.invoke(() -> createPeer( DataPolicy.REPLICATE ));
    vm1.invoke(() -> createPeer( DataPolicy.REPLICATE ));
    vm2.invoke(() -> createPeer( DataPolicy.REPLICATE ));

    // Step-4 : Do some puts on region created earlier
    vm3.invoke(() -> populateRegion());

    // add expected exception to avoid suspect strings
    final IgnoredException ex = IgnoredException.addIgnoredException("did not send last result");

    // Step-5 : Execute function randomly (in iteration) on all available (per
    // VM) REST end-points and verify its result
    for (int i = 0; i < 10; i++) {
      executeFunction_NoLastResult(REGION_NAME);
    }
    ex.remove();

    restURLs.clear();
  }

  public void testOnRegionExecutionWithRR() {
    // Step-1 : create cache on each VM, this will start HTTP service in
    // embedded mode and deploy REST APIs web app on it.
    //
    String url1 = (String) vm3.invoke(() -> RestAPITestBase.createCache( vm3 ));
    restURLs.add(url1);

    String url2 = (String) vm0.invoke(() -> RestAPITestBase.createCache( vm0 ));
    restURLs.add(url2);

    String url3 = (String) vm1.invoke(() -> RestAPITestBase.createCache( vm1 ));
    restURLs.add(url3);

    String url4 = (String) vm2.invoke(() -> RestAPITestBase.createCache( vm2 ));
    restURLs.add(url4);

    // Step-2: Register function in all VMs
    registerSampleFunction(vm3);
    registerSampleFunction(vm0);
    registerSampleFunction(vm1);
    registerSampleFunction(vm2);

    // Step-3: Create and configure PR on all VMs
    vm3.invoke(() -> createPeer( DataPolicy.EMPTY ));
    vm0.invoke(() -> createPeer( DataPolicy.REPLICATE ));
    vm1.invoke(() -> createPeer( DataPolicy.REPLICATE ));
    vm2.invoke(() -> createPeer( DataPolicy.REPLICATE ));

    // Step-4 : Do some puts in Replicated region on vm3
    vm3.invoke(() -> populateRRRegion());

    // Step-5 : Execute function randomly (in iteration) on all available (per
    // VM) REST end-points and verify its result
    executeFunctionThroughRestCall(REGION_NAME);
    int c0 = getInvocationCount(vm0);
    int c1 = getInvocationCount(vm1);
    int c2 = getInvocationCount(vm2);
    int c3 = getInvocationCount(vm3);

    assertEquals(1, c0 + c1 + c2 + c3);

    // remove the expected exception
    restURLs.clear();
  }

  public void testOnRegionExecutionWithPR() throws Exception {
    final String rName = getUniqueName();

    // Step-1 : create cache on each VM, this will start HTTP service in
    // embedded mode and deploy REST APIs web app on it.
    String url1 = (String) vm3.invoke(() -> RestAPITestBase.createCache( vm3 ));
    restURLs.add(url1);

    String url2 = (String) vm0.invoke(() -> RestAPITestBase.createCache( vm0 ));
    restURLs.add(url2);

    String url3 = (String) vm1.invoke(() -> RestAPITestBase.createCache( vm1 ));
    restURLs.add(url3);

    String url4 = (String) vm2.invoke(() -> RestAPITestBase.createCache( vm2 ));
    restURLs.add(url4);

    // Step-2: Register function in all VMs
    registerSampleFunction(vm3);
    registerSampleFunction(vm0);
    registerSampleFunction(vm1);
    registerSampleFunction(vm2);

    // Step-3: Create and configure PR on all VMs
    vm3.invoke(() -> createPeerWithPR());
    vm0.invoke(() -> createPeerWithPR());
    vm1.invoke(() -> createPeerWithPR());
    vm2.invoke(() -> createPeerWithPR());

    // Step-4: Do some puts such that data exist in each bucket
    vm3.invoke(() -> populatePRRegion());

    // Step-5 : Execute function randomly (in iteration) on all available (per
    // VM) REST end-points and verify its result
    executeFunctionThroughRestCall(PR_REGION_NAME);

    // Assert that each node has executed the function once.
    verifyAndResetInvocationCount(vm0, 1);
    verifyAndResetInvocationCount(vm1, 1);
    verifyAndResetInvocationCount(vm2, 1);
    verifyAndResetInvocationCount(vm3, 1);

    int c0 = getInvocationCount(vm0);
    int c1 = getInvocationCount(vm1);
    int c2 = getInvocationCount(vm2);
    int c3 = getInvocationCount(vm3);

    restURLs.clear();
  }

}

class MyFunctionException implements Function {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    throw new RuntimeException("failure");
  }

  @Override
  public String getId() {
    return this.getClass().getName();
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

}
