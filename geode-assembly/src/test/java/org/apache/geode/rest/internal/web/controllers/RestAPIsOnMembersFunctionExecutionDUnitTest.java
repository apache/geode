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

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.rest.internal.web.RestFunctionTemplate;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.FlakyTest;
import com.gemstone.gemfire.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category(DistributedTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class RestAPIsOnMembersFunctionExecutionDUnitTest extends RestAPITestBase {

  @Parameterized.Parameter
  public String urlContext;

  @Parameterized.Parameters
  public static Collection<String> data() {
    return Arrays.asList("/geode", "/gemfire-api");
  }

  private String createCacheAndRegisterFunction(String hostName, String memberName) {
    final int servicePort = AvailablePortHelper.getRandomAvailableTCPPort();

    Properties props = new Properties();
    props.setProperty(NAME, memberName);
    props.setProperty(START_DEV_REST_API, "true");
    props.setProperty(HTTP_SERVICE_BIND_ADDRESS, hostName);
    props.setProperty(HTTP_SERVICE_PORT, String.valueOf(servicePort));

    Cache c = null;
    try {
      c = CacheFactory.getInstance(new RestAPIsOnMembersFunctionExecutionDUnitTest().getSystem(props));
      c.close();
    } catch (CacheClosedException ignore) {
    }

    c = CacheFactory.create(new RestAPIsOnMembersFunctionExecutionDUnitTest().getSystem(props));
    FunctionService.registerFunction(new OnMembersFunction());

    return "http://" + hostName + ":" + servicePort + urlContext + "/v1";

  }

  @Override
  protected String getFunctionID() {
    return OnMembersFunction.Id;
  }

  @Test
  @Category(FlakyTest.class) // GEODE-1594 HTTP server fails to respond
  public void testFunctionExecutionOnAllMembers() {
    createCacheForVMs();

    for (int i = 0; i < 10; i++) {
      CloseableHttpResponse response = executeFunctionThroughRestCall("OnMembersFunction", null, null, null, null, null);
      assertHttpResponse(response, 200, 4);
    }

    assertCorrectInvocationCount(40, vm0, vm1, vm2, vm3);

    restURLs.clear();
  }

  private void createCacheForVMs() {
    restURLs.add(vm0.invoke("createCacheAndRegisterFunction", () -> createCacheAndRegisterFunction(vm0.getHost()
                                                                                                      .getHostName(), "m1")));
    restURLs.add(vm1.invoke("createCacheAndRegisterFunction", () -> createCacheAndRegisterFunction(vm1.getHost()
                                                                                                      .getHostName(), "m2")));
    restURLs.add(vm2.invoke("createCacheAndRegisterFunction", () -> createCacheAndRegisterFunction(vm2.getHost()
                                                                                                      .getHostName(), "m3")));
    restURLs.add(vm3.invoke("createCacheAndRegisterFunction", () -> createCacheAndRegisterFunction(vm3.getHost()
                                                                                                      .getHostName(), "m4")));
  }

  @Test
  public void testFunctionExecutionEOnSelectedMembers() {
    createCacheForVMs();

    for (int i = 0; i < 10; i++) {
      CloseableHttpResponse response = executeFunctionThroughRestCall("OnMembersFunction", null, null, null, null, "m1,m2,m3");
      assertHttpResponse(response, 200, 3);
    }

    assertCorrectInvocationCount(30, vm0, vm1, vm2, vm3);

    restURLs.clear();
  }

  @Test
  public void testFunctionExecutionOnMembersWithFilter() {
    createCacheForVMs();

    for (int i = 0; i < 10; i++) {
      CloseableHttpResponse response = executeFunctionThroughRestCall("OnMembersFunction", null, "key2", null, null, "m1,m2,m3");
      assertHttpResponse(response, 500, 0);
    }

    assertCorrectInvocationCount(0, vm0, vm1, vm2, vm3);

    restURLs.clear();
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

}
