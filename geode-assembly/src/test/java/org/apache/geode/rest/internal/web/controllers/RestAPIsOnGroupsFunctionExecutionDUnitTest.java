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
package org.apache.geode.rest.internal.web.controllers;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.rest.internal.web.RestFunctionTemplate;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category(DistributedTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class RestAPIsOnGroupsFunctionExecutionDUnitTest extends RestAPITestBase {

  @Parameterized.Parameter
  public String urlContext;

  @Parameterized.Parameters
  public static Collection<String> data() {
    return Arrays.asList("/geode", "/gemfire-api");
  }


  @Override
  protected String getFunctionID() {
    return OnGroupsFunction.Id;
  }

  private void setupCacheWithGroupsAndFunction() {
    restURLs.add(vm0.invoke("createCacheWithGroups",
        () -> createCacheWithGroups(vm0.getHost().getHostName(), "g0,gm", urlContext)));
    restURLs.add(vm1.invoke("createCacheWithGroups",
        () -> createCacheWithGroups(vm1.getHost().getHostName(), "g1", urlContext)));
    restURLs.add(vm2.invoke("createCacheWithGroups",
        () -> createCacheWithGroups(vm2.getHost().getHostName(), "g0,g1", urlContext)));

    vm0.invoke("registerFunction(new OnGroupsFunction())",
        () -> FunctionService.registerFunction(new OnGroupsFunction()));
    vm1.invoke("registerFunction(new OnGroupsFunction())",
        () -> FunctionService.registerFunction(new OnGroupsFunction()));
    vm2.invoke("registerFunction(new OnGroupsFunction())",
        () -> FunctionService.registerFunction(new OnGroupsFunction()));
  }

  @Test
  public void testonGroupsExecutionOnAllMembers() {
    setupCacheWithGroupsAndFunction();

    for (int i = 0; i < 10; i++) {
      CloseableHttpResponse response =
          executeFunctionThroughRestCall("OnGroupsFunction", null, null, null, "g0,g1", null);
      assertHttpResponse(response, 200, 3);
    }

    assertCorrectInvocationCount(30, vm0, vm1, vm2);

    restURLs.clear();
  }

  @Category(FlakyTest.class) // GEODE-1924
  @Test
  public void testonGroupsExecutionOnAllMembersWithFilter() {
    setupCacheWithGroupsAndFunction();

    // Execute function randomly (in iteration) on all available (per VM) REST end-points and verify
    // its result
    for (int i = 0; i < 10; i++) {
      CloseableHttpResponse response =
          executeFunctionThroughRestCall("OnGroupsFunction", null, "someKey", null, "g1", null);
      assertHttpResponse(response, 500, 0);
    }

    assertCorrectInvocationCount(0, vm0, vm1, vm2);
    restURLs.clear();
  }

  @Test
  public void testBasicP2PFunctionSelectedGroup() {
    setupCacheWithGroupsAndFunction();

    // Step-3 : Execute function randomly (in iteration) on all available (per VM) REST end-points
    // and verify its result
    for (int i = 0; i < 5; i++) {
      CloseableHttpResponse response = executeFunctionThroughRestCall("OnGroupsFunction", null,
          null, null, "no%20such%20group", null);
      assertHttpResponse(response, 500, 0);
    }
    assertCorrectInvocationCount(0, vm0, vm1, vm2);

    for (int i = 0; i < 5; i++) {

      CloseableHttpResponse response =
          executeFunctionThroughRestCall("OnGroupsFunction", null, null, null, "gm", null);
      assertHttpResponse(response, 200, 1);
    }

    assertCorrectInvocationCount(5, vm0, vm1, vm2);

    resetInvocationCounts(vm0, vm1, vm2);

    restURLs.clear();
  }

  private class OnGroupsFunction extends RestFunctionTemplate {
    static final String Id = "OnGroupsFunction";

    @Override
    public void execute(FunctionContext context) {
      LogWriterUtils.getLogWriter().fine("SWAP:1:executing OnGroupsFunction:" + invocationCount);
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
}

