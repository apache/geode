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

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.rest.internal.web.RestFunctionTemplate;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.VM;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.util.Properties;

public class RestAPIsOnMembersFunctionExecutionDUnitTest extends RestAPITestBase {

  private static final long serialVersionUID = 1L;

  public RestAPIsOnMembersFunctionExecutionDUnitTest(String name) {
    super(name);
  }

  private class OnMembersFunction extends RestFunctionTemplate {
    public static final String Id = "OnMembersFunction";

    @Override
    public void execute(FunctionContext context) {

      System.out.println("SWAP:1:executing OnMembersFunction:" + invocationCount);
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

  private String createCacheAndRegisterFunction(String hostName, String memberName) {
    final int servicePort = AvailablePortHelper.getRandomAvailableTCPPort();

    Properties props = new Properties();
    props.setProperty(DistributionConfig.NAME_NAME, memberName);
    props.setProperty(DistributionConfig.START_DEV_REST_API_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_BIND_ADDRESS_NAME, hostName);
    props.setProperty(DistributionConfig.HTTP_SERVICE_PORT_NAME, String.valueOf(servicePort));

    Cache c = null;
    try {
      c = CacheFactory.getInstance(new RestAPIsOnMembersFunctionExecutionDUnitTest("temp").getSystem(props));
      c.close();
    } catch (CacheClosedException cce) {
    }

    c = CacheFactory.create(new RestAPIsOnMembersFunctionExecutionDUnitTest("temp").getSystem(props));
    FunctionService.registerFunction(new OnMembersFunction());

    String restEndPoint = "http://" + hostName + ":" + servicePort + "/gemfire-api/v1";
    return restEndPoint;

  }

  @Override
  protected String getFunctionID() {
    return OnMembersFunction.Id;
  }

  public void testFunctionExecutionOnAllMembers() {
    createCacheForVMs();

    for (int i = 0; i < 10; i++) {
      CloseableHttpResponse response = executeFunctionThroughRestCall("OnMembersFunction",null,null,null,null,null);
      assertHttpResponse(response, 200, 4);
    }

    assertCorrectInvocationCount(40,vm0,vm1,vm2,vm3);

    restURLs.clear();
  }

  private void createCacheForVMs() {
    restURLs.add(vm0.invoke("createCacheAndRegisterFunction",() -> createCacheAndRegisterFunction(vm0.getHost().getHostName(), "m1")));
    restURLs.add(vm1.invoke("createCacheAndRegisterFunction",() -> createCacheAndRegisterFunction(vm1.getHost().getHostName(), "m2")));
    restURLs.add(vm2.invoke("createCacheAndRegisterFunction",() -> createCacheAndRegisterFunction(vm2.getHost().getHostName(), "m3")));
    restURLs.add(vm3.invoke("createCacheAndRegisterFunction",() -> createCacheAndRegisterFunction(vm3.getHost().getHostName(), "m4")));
  }

  public void testFunctionExecutionEOnSelectedMembers() {
    createCacheForVMs();

    for (int i = 0; i < 10; i++) {
      CloseableHttpResponse response = executeFunctionThroughRestCall("OnMembersFunction",null,null,null,null,"m1,m2,m3");
      assertHttpResponse(response, 200, 3);
    }

    assertCorrectInvocationCount(30,vm0,vm1,vm2,vm3);

    restURLs.clear();
  }

  public void testFunctionExecutionOnMembersWithFilter() {
    createCacheForVMs();

    for (int i = 0; i < 10; i++) {
      CloseableHttpResponse response = executeFunctionThroughRestCall("OnMembersFunction",null,"key2",null,null,"m1,m2,m3");
      assertHttpResponse(response, 500, 0);
    }

    assertCorrectInvocationCount(0,vm0,vm1,vm2,vm3);

    restURLs.clear();
  }

}
