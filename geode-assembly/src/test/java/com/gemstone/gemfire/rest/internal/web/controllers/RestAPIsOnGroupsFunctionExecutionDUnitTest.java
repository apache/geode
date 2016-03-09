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

import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.rest.internal.web.RestFunctionTemplate;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.util.ArrayList;
import java.util.Collections;

public class RestAPIsOnGroupsFunctionExecutionDUnitTest extends RestAPITestBase {

  public RestAPIsOnGroupsFunctionExecutionDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected String getFunctionID() {
    return OnGroupsFunction.Id;
  }

  private void resetInvocationCount() {
    OnGroupsFunction f = (OnGroupsFunction) FunctionService.getFunction(OnGroupsFunction.Id);
    f.invocationCount = 0;
  }

  public void testonGroupsExecutionOnAllMembers() {
    setupCacheWithGroupsAndFunction();

    for (int i = 0; i < 10; i++) {
      CloseableHttpResponse response = executeFunctionThroughRestCall("OnGroupsFunction", null, null, null, "g0,g1", null);
      assertHttpResponse(response, 200, 3);
    }

    int c0 = vm0.invoke(() -> getInvocationCount());
    int c1 = vm1.invoke(() -> getInvocationCount());
    int c2 = vm2.invoke(() -> getInvocationCount());

    assertEquals(30, (c0 + c1 + c2));

    restURLs.clear();
  }

  private void setupCacheWithGroupsAndFunction() {
    restURLs.add(vm0.invoke(() -> createCacheWithGroups(vm0, "g0,gm")));
    restURLs.add(vm1.invoke(() -> createCacheWithGroups(vm1, "g1")));
    restURLs.add(vm2.invoke(() -> createCacheWithGroups(vm2, "g0,g1")));

    vm0.invoke(() -> FunctionService.registerFunction(new OnGroupsFunction()));
    vm1.invoke(() -> FunctionService.registerFunction(new OnGroupsFunction()));
    vm2.invoke(() -> FunctionService.registerFunction(new OnGroupsFunction()));
  }

  public void testonGroupsExecutionOnAllMembersWithFilter() {
    setupCacheWithGroupsAndFunction();

    //Execute function randomly (in iteration) on all available (per VM) REST end-points and verify its result
    for (int i = 0; i < 10; i++) {
      CloseableHttpResponse response = executeFunctionThroughRestCall("OnGroupsFunction", null, "someKey", null, "g1", null);
      assertHttpResponse(response, 500, 0);
    }

    int c0 = vm0.invoke(() -> getInvocationCount());
    int c1 = vm1.invoke(() -> getInvocationCount());
    int c2 = vm2.invoke(() -> getInvocationCount());

    assertEquals(0, (c0 + c1 + c2));
    restURLs.clear();
  }

  public void testBasicP2PFunctionSelectedGroup() {
    setupCacheWithGroupsAndFunction();

    //Step-3 : Execute function randomly (in iteration) on all available (per VM) REST end-points and verify its result
    for (int i = 0; i < 5; i++) {
      CloseableHttpResponse response = executeFunctionThroughRestCall("OnGroupsFunction", null, null, null, "no%20such%20group", null);
      assertHttpResponse(response, 500, 0);
    }
    int c0 = vm0.invoke(() -> getInvocationCount());
    int c1 = vm1.invoke(() -> getInvocationCount());
    int c2 = vm2.invoke(() -> getInvocationCount());

    assertEquals(0, (c0 + c1 + c2));

    for (int i = 0; i < 5; i++) {

      CloseableHttpResponse response = executeFunctionThroughRestCall("OnGroupsFunction", null, null, null, "gm", null);
      assertHttpResponse(response, 200, 1);
    }

    c0 = vm0.invoke(() -> getInvocationCount());
    c1 = vm1.invoke(() -> getInvocationCount());
    c2 = vm2.invoke(() -> getInvocationCount());

    assertEquals(5, (c0 + c1 + c2));

    vm0.invoke(() -> resetInvocationCount());
    vm1.invoke(() -> resetInvocationCount());
    vm2.invoke(() -> resetInvocationCount());

    restURLs.clear();
  }

  private class OnGroupsFunction extends RestFunctionTemplate {
    public static final String Id = "OnGroupsFunction";

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

