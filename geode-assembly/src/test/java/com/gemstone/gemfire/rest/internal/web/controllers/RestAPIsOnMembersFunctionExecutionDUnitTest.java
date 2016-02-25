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
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * 
 * @author Nilkanth Patel
 */

public class RestAPIsOnMembersFunctionExecutionDUnitTest extends CacheTestCase { 
  
  private static final long serialVersionUID = 1L;
  
  VM member1 = null;
  VM member2 = null;
  VM member3 = null;
  VM member4 = null;
  
  static InternalDistributedSystem ds = null;

  public RestAPIsOnMembersFunctionExecutionDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    member1 = host.getVM(0);
    member2 = host.getVM(1);
    member3 = host.getVM(2);
    member4 = host.getVM(3);
  }
  
  static class OnMembersFunction implements Function {
    private static final long serialVersionUID = -1032915440862585532L;
    public static final String Id = "OnMembersFunction";
    public static int invocationCount;

    @Override
    public void execute(FunctionContext context) {
      
      LogWriterUtils.getLogWriter().fine("SWAP:1:executing OnMembersFunction:"+invocationCount);
      InternalDistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
      invocationCount++;
      
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
  
  private void verifyAndResetInvocationCount(VM vm, final int count) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        OnMembersFunction f = (OnMembersFunction) FunctionService.getFunction(OnMembersFunction.Id);
        assertEquals(count, f.invocationCount);
        // assert succeeded, reset count
        f.invocationCount = 0;
        return null;
      }
    });
  }
  
  private InternalDistributedSystem createSystem(Properties props){
    try {
      ds = getSystem(props);
      assertNotNull(ds);
      FunctionService.registerFunction(new OnMembersFunction());
      
    }
    catch (Exception e) {
      Assert.fail("Failed while creating the Distribued System", e);
    }
    return ds;
  }
  
  public static String createCacheAndRegisterFunction(VM vm, String memberName) {
    final String hostName = vm.getHost().getHostName(); 
    final int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    
    Properties props = new Properties();
    props.setProperty(DistributionConfig.NAME_NAME, memberName);
    props.setProperty(DistributionConfig.START_DEV_REST_API_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_BIND_ADDRESS_NAME, hostName);
    props.setProperty(DistributionConfig.HTTP_SERVICE_PORT_NAME, String.valueOf(serverPort));
    
    Cache c = null;
    try {
      c = CacheFactory.getInstance( new RestAPIsOnMembersFunctionExecutionDUnitTest("temp").getSystem(props));
      c.close();
    } catch (CacheClosedException cce) {
    }
    
    c = CacheFactory.create(new RestAPIsOnMembersFunctionExecutionDUnitTest("temp").getSystem(props));
    FunctionService.registerFunction(new OnMembersFunction());
    
    String restEndPoint =  "http://" + hostName + ":" + serverPort + "/gemfire-api/v1";
    return restEndPoint;
   
  }
  
  public static void executeFunctionOnAllMembersThroughRestCall(List<String> restURLs) {
    Random randomGenerator = new Random();
    int index = randomGenerator.nextInt(restURLs.size());
    
    //Testcase: onMembers Function execution with No groups specified
    try {
      
      CloseableHttpClient httpclient = HttpClients.createDefault();
      CloseableHttpResponse response = null;
      HttpPost post = new HttpPost(restURLs.get(index) + "/functions/OnMembersFunction");
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
            
      
      //verify response status code
      assertEquals(200, response.getStatusLine().getStatusCode());
      
      //verify response hasbody flag
      assertNotNull(response.getEntity());
      
      
      response.close();      
     
      JSONArray resultArray = new JSONArray(sb.toString());
      assertEquals(resultArray.length(), 4);
      
      //fail("Expected exception while executing function onMembers without any members ");
      
    } catch (Exception e) {
      throw new RuntimeException("unexpected exception", e);
    }
  }
 
  public static void executeFunctionOnGivenMembersThroughRestCall(List<String> restURLs) {
    Random randomGenerator = new Random();
    int index = randomGenerator.nextInt(restURLs.size());
    
    //Testcase: onMembers Function execution with valid groups
    try {
      
      CloseableHttpClient httpclient = HttpClients.createDefault();
      CloseableHttpResponse response = null;
      HttpPost post = new HttpPost(restURLs.get(index) + "/functions/OnMembersFunction?onMembers=m1,m2,m3");
      post.addHeader("Content-Type", "application/json");
      post.addHeader("Accept", "application/json");
      response = httpclient.execute(post);
    
      //verify response status code. expected status code is 200 OK.
      assertEquals(response.getStatusLine().getStatusCode(), 200);
      
      //verify response hasbody flag, expected is true.
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
      assertEquals(resultArray.length(), 3);
      
    } catch (Exception e) {
      throw new RuntimeException("unexpected exception", e);
    }
  }
  
  public void testFunctionExecutionOnAllMembers()  {
    
    List<String> restURLs = new ArrayList<String>(); 
    
    //Step-1 : create cache on each VM, this will start HTTP service in embedded mode and deploy REST APIs web app on it.
    //         Connect to DS and Register function. Add Rest end-point into the restURLs list.
    
    String url1 = (String)member1.invoke(() -> RestAPIsOnMembersFunctionExecutionDUnitTest.createCacheAndRegisterFunction(member1, "m1"));
    restURLs.add(url1);
    
    String url2 = (String)member2.invoke(() -> RestAPIsOnMembersFunctionExecutionDUnitTest.createCacheAndRegisterFunction(member2, "m2"));
    restURLs.add(url2);
    
    String url3 = (String)member3.invoke(() -> RestAPIsOnMembersFunctionExecutionDUnitTest.createCacheAndRegisterFunction(member3, "m3"));
    restURLs.add(url3);
    
    String url4 = (String)member4.invoke(() -> RestAPIsOnMembersFunctionExecutionDUnitTest.createCacheAndRegisterFunction(member4, "m4"));
    restURLs.add(url4);
    
    //default case, execute function on all members, register the function in controller VM
    //FunctionService.registerFunction(new OnMembersFunction());
    
    //Step-2 : Execute function randomly (in iteration) on all available (per VM) REST end-points and verify its result
    for (int i=0; i< 10; i++) {
      executeFunctionOnAllMembersThroughRestCall(restURLs);
    }
    
    //Verify that each node (m1, m2, m3) has run the function
    verifyAndResetInvocationCount(member1, 10);
    verifyAndResetInvocationCount(member2, 10);
    verifyAndResetInvocationCount(member3, 10);
    verifyAndResetInvocationCount(member4, 10);

    restURLs.clear();
  }
  
  public void testFunctionExecutionEOnSelectedMembers()  {
    
    List<String> restURLs = new ArrayList<String>(); 
    
    //Step-1 : create cache on each VM, this will start HTTP service in embedded mode and deploy REST APIs web app on it.
    //         Connect to DS and Register function. Add Rest end-point into the restURLs list.
    
    String url1 = (String)member1.invoke(() -> RestAPIsOnMembersFunctionExecutionDUnitTest.createCacheAndRegisterFunction(member1, "m1"));
    restURLs.add(url1);
    
    String url2 = (String)member2.invoke(() -> RestAPIsOnMembersFunctionExecutionDUnitTest.createCacheAndRegisterFunction(member2, "m2"));
    restURLs.add(url2);
    
    String url3 = (String)member3.invoke(() -> RestAPIsOnMembersFunctionExecutionDUnitTest.createCacheAndRegisterFunction(member3, "m3"));
    restURLs.add(url3);
    
    String url4 = (String)member4.invoke(() -> RestAPIsOnMembersFunctionExecutionDUnitTest.createCacheAndRegisterFunction(member4, "m4"));
    restURLs.add(url4);
    
    //default case, execute function on all members, register the function in controller VM
    //FunctionService.registerFunction(new OnMembersFunction());
    
    //Step-2 : Execute function randomly (in iteration) on all available (per VM) REST end-points and verify its result
    for (int i=0; i< 10; i++) {
      executeFunctionOnGivenMembersThroughRestCall(restURLs);
    }
    
    //Verify that each node (m1, m2, m3) has run the function
    verifyAndResetInvocationCount(member1, 10);
    verifyAndResetInvocationCount(member2, 10);
    verifyAndResetInvocationCount(member3, 10);
    

    restURLs.clear();
  }
  
}
