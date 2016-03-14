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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

public class RestAPIsOnGroupsFunctionExecutionDUnitTest extends RestAPITestBase {

  public RestAPIsOnGroupsFunctionExecutionDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
    final Host host = Host.getHost(0);
  }
  
  private void registerFunction(VM vm) {
    vm.invoke(new SerializableCallable() {
      private static final long serialVersionUID = 1L;
      
      @Override
      public Object call() throws Exception {
        FunctionService.registerFunction(new OnGroupsFunction());
        return null;
      }
    });
  }
  
  static class OnGroupsFunction implements Function {
    private static final long serialVersionUID = -1032915440862585532L;
    public static final String Id = "OnGroupsFunction";
    public static int invocationCount;

    @Override
    public void execute(FunctionContext context) {
      LogWriterUtils.getLogWriter().fine("SWAP:1:executing OnGroupsFunction:"+invocationCount);
      InternalDistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
      invocationCount++;
      ArrayList<String> l = (ArrayList<String>) context.getArguments();
      if (l != null) {
        assertFalse(Collections.disjoint(l, ds.getDistributedMember().getGroups()));
      }
      context.getResultSender().lastResult(Boolean.TRUE);
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
  
  
  public static void executeFunctionThroughRestCall(List<String> restURLs) {
    Random randomGenerator = new Random();
    int index = randomGenerator.nextInt(restURLs.size());
    
    try {
      
      CloseableHttpClient httpclient = HttpClients.createDefault();
      CloseableHttpResponse response = null;
      HttpPost post = new HttpPost(restURLs.get(index) + "/functions/OnGroupsFunction?onGroups=g0,g1");
      post.addHeader("Content-Type", "application/json");
      post.addHeader("Accept", "application/json");
      LogWriterUtils.getLogWriter().info("Request POST : " + post.toString());
      response = httpclient.execute(post);
      
      HttpEntity entity = response.getEntity();
      InputStream content = entity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(
          content));
      String line;
      StringBuffer sb = new StringBuffer();
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }      
      LogWriterUtils.getLogWriter().info("Response : " + sb.toString());
    
      //verify response status code. expected status code is 200 OK.
      assertEquals(response.getStatusLine().getStatusCode(), 200);
      
      
      //verify response hasbody flag, expected is true.
      assertNotNull(response.getEntity());
      
      
      response.close();
      
      //verify function execution result
      JSONArray resultArray = new JSONArray(sb.toString());
      assertEquals(resultArray.length(), 3);
      
    } catch (Exception e) {
      throw new RuntimeException("unexpected exception", e);
    }
    
  }
  
  public static void executeFunctionOnMemberThroughRestCall(List<String> restURLs) {
    Random randomGenerator = new Random();
    int index = randomGenerator.nextInt(restURLs.size());
    
    //Testcase-1: Executing function on non-existing group. 
    final IgnoredException ex = IgnoredException.addIgnoredException("com.gemstone.gemfire.cache.execute.FunctionException");
    try {
      
      CloseableHttpClient httpclient = HttpClients.createDefault();
      CloseableHttpResponse response = null;
      HttpPost post = new HttpPost(restURLs.get(index) + "/functions/OnGroupsFunction?onGroups=no%20such%20group");
      post.addHeader("Content-Type", "application/json");
      post.addHeader("Accept", "application/json");
      response = httpclient.execute(post);
      
      if ( response.getStatusLine().getStatusCode() == 200 ) {
        fail("FunctionException expected : no member(s) are found belonging to the provided group(s)");
      }
    } catch (Exception e) {
      throw new RuntimeException("unexpected exception", e);
      
    } finally {
      ex.remove();
    }
    
    //Testcase-2: Executing function on group with args.
    
    final String FUNCTION_ARGS1 =  "{"
        +        "\"@type\": \"string\","
        +        "\"@value\": \"gm\""
        +    "}";
    
    try {
     
      CloseableHttpClient httpclient = HttpClients.createDefault();
      CloseableHttpResponse response = null;
      HttpPost post = new HttpPost(restURLs.get(index) + "/functions/OnGroupsFunction?onGroups=gm");
      post.addHeader("Content-Type", "application/json");
      post.addHeader("Accept", "application/json");
      response = httpclient.execute(post);
      
      //verify response status code
      assertEquals(response.getStatusLine().getStatusCode(), 200);
      
      //verify response hasbody flag
      assertNotNull(response.getEntity());
      
      HttpEntity entity = response.getEntity();
      InputStream content = entity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(
          content));
      String line;
      StringBuffer sb = new StringBuffer();
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      response.close();
      
      //verify function execution result
      JSONArray resultArray = new JSONArray(sb.toString());
      assertEquals(resultArray.length(), 1);
      
    } catch (Exception e) {
      throw new RuntimeException("unexpected exception", e);
    }   
  }
  
  private void verifyAndResetInvocationCount(VM vm, final int count) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        OnGroupsFunction f = (OnGroupsFunction) FunctionService.getFunction(OnGroupsFunction.Id);
        assertEquals(count, f.invocationCount);
        // assert succeeded, reset count
        f.invocationCount = 0;
        return null;
      }
    });
  }
  
  private void resetInvocationCount(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        OnGroupsFunction f = (OnGroupsFunction) FunctionService.getFunction(OnGroupsFunction.Id);
        f.invocationCount = 0;
        return null;
      }
    });
  }
  
  public void testonGroupsExecutionOnAllMembers() {
  
    List<String> restURLs = new ArrayList<String>(); 
    //Step-1 : create cache on each VM, this will start HTTP service in embedded mode and deploy REST APIs web app on it.
    //         Create and configure Region on all VMs. Add Rest end-point into the restURLs list.
    
    String url1 = (String)vm0.invoke(() -> RestAPITestBase.createCacheWithGroups(vm0, "g0,gm", "TEST_REGION"));
    restURLs.add(url1);
    
    String url2 = (String)vm1.invoke(() -> RestAPITestBase.createCacheWithGroups(vm1, "g1", "TEST_REGION" ));
    restURLs.add(url2);
    
    String url3 = (String)vm2.invoke(() -> RestAPITestBase.createCacheWithGroups(vm2, "g0,g1", "TEST_REGION"));
    restURLs.add(url3);
    
    //Step-2: Register function in all VMs
    registerFunction(vm0);
    registerFunction(vm1);
    registerFunction(vm2);
    
    //Step-3 : Execute function randomly (in iteration) on all available (per VM) REST end-points and verify its result
    for (int i=0; i< 10; i++)
      executeFunctionThroughRestCall(restURLs);
    
    //Verify that each node belonging to specified group has run the function
    verifyAndResetInvocationCount(vm0, 10);
    verifyAndResetInvocationCount(vm1, 10);
    verifyAndResetInvocationCount(vm2, 10);
   
    restURLs.clear();
  }
  
  
  public void testBasicP2PFunctionSelectedGroup() {
  
    List<String> restURLs = new ArrayList<String>(); 
    
    //Step-1 : create cache on each VM, this will start HTTP service in embedded mode and deploy REST APIs web app on it.
    //         Create and configure Region on all VMs. Add Rest end-point into the restURLs list.
    String url1 = (String)vm0.invoke(() -> RestAPITestBase.createCacheWithGroups(vm0, "g0,gm", "null" ));
    restURLs.add(url1);
    
    String url2 = (String)vm1.invoke(() -> RestAPITestBase.createCacheWithGroups(vm1, "g1", "null"  ));
    restURLs.add(url2);
    
    String url3 = (String)vm2.invoke(() -> RestAPITestBase.createCacheWithGroups(vm2, "g0,g1", "null" ));
    restURLs.add(url3);
    
    //Step-2: Register function in all VMs
    registerFunction(vm0);
    registerFunction(vm1);
    registerFunction(vm2);
    
    //Step-3 : Execute function randomly (in iteration) on all available (per VM) REST end-points and verify its result
    for (int i=0; i< 5; i++)
      executeFunctionOnMemberThroughRestCall(restURLs);
    
    resetInvocationCount(vm0);
    resetInvocationCount(vm1);
    resetInvocationCount(vm2);
    
    restURLs.clear();
  }
}

