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

package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;
import org.assertj.core.util.Strings;
import org.json.JSONArray;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Category(DistributedTest.class)
public class FunctionCommandsDUnitTest {
  private MemberVM server1;
  private MemberVM server2;

  private static final String REGION_ONE = "RegionOne";
  private static final String REGION_TWO = "RegionTwo";

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Before
  public void before() throws Exception {
    MemberVM locator = lsRule.startLocatorVM(0);

    Properties props = new Properties();
    props.setProperty("groups", "group-1");
    server1 = lsRule.startServerVM(1, props, locator.getPort());

    server2 = lsRule.startServerVM(2, locator.getPort());

    server1.invoke(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();

      RegionFactory<Integer, Integer> dataRegionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      Region region = dataRegionFactory.create(REGION_ONE);
      for (int i = 0; i < 10; i++) {
        region.put("key" + (i + 200), "value" + (i + 200));
      }
      region = dataRegionFactory.create(REGION_TWO);
      for (int i = 0; i < 1000; i++) {
        region.put("key" + (i + 200), "value" + (i + 200));
      }
    });

    server2.invoke(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();
      RegionFactory<Integer, Integer> dataRegionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      Region region = dataRegionFactory.create(REGION_ONE);
      for (int i = 0; i < 10000; i++) {
        region.put("key" + (i + 400), "value" + (i + 400));
      }
      region = dataRegionFactory.create(REGION_TWO);
      for (int i = 0; i < 10; i++) {
        region.put("key" + (i + 200), "value" + (i + 200));
      }
    });

    registerFunction(new TestFunction(true, TestFunction.TEST_FUNCTION1), locator, server1,
        server2);
    registerFunction(new TestFunction(true, TestFunction.TEST_FUNCTION_RETURN_ARGS), locator,
        server1, server2);

    locator.invoke(() -> {
      Cache cache = LocatorServerStartupRule.locatorStarter.getLocator().getCache();
      ManagementService managementService = ManagementService.getManagementService(cache);
      DistributedSystemMXBean dsMXBean = managementService.getDistributedSystemMXBean();

      await().atMost(120, TimeUnit.SECONDS).until(() -> dsMXBean.getMemberCount() == 3);
    });

    connectGfsh(locator);
  }

  private void registerFunction(Function function, MemberVM... vms) {
    for (MemberVM vm : vms) {
      vm.invoke(() -> FunctionService.registerFunction(function));
    }
  }

  public void connectGfsh(MemberVM vm) throws Exception {
    gfsh.connectAndVerify(vm.getJmxPort(), GfshShellConnectionRule.PortType.jmxManager);
  }

  @Test
  public void testExecuteFunctionOnRegion() throws Exception {
    CommandResult result = gfsh.executeAndVerifyCommand(
        "execute function --id=" + TestFunction.TEST_FUNCTION1 + " --region=/" + REGION_ONE);
    assertThat(((JSONArray) result.getContent().get("Member ID/Name")).length()).isEqualTo(2);
  }

  @Test
  public void testExecuteUnknownFunction() throws Exception {
    CommandResult result = gfsh.executeAndVerifyCommand("execute function --id=UNKNOWN_FUNCTION");
    assertThat(result.getContent().toString())
        .contains("UNKNOWN_FUNCTION is not registered on member");
  }

  @Test
  public void testExecuteFunctionOnRegionWithCustomResultCollector() {
    CommandResult result = gfsh
        .executeAndVerifyCommand("execute function --id=" + TestFunction.TEST_FUNCTION_RETURN_ARGS
            + " --region=" + REGION_ONE + " --arguments=arg1,arg2" + " --result-collector="
            + ToUpperResultCollector.class.getName());
    assertThat(result.getContent().toString()).contains("ARG1");
  }

  @Test
  public void testExecuteFunctionOnMember() {
    CommandResult result = gfsh.executeAndVerifyCommand("execute function --id="
        + TestFunction.TEST_FUNCTION1 + " --member=" + server1.getMember().getName());
    assertThat(((JSONArray) result.getContent().get("Member ID/Name")).length()).isEqualTo(1);
  }

  @Test
  public void testExecuteFunctionOnInvalidMember() {
    CommandResult result = gfsh.executeCommand(
        "execute function --id=" + TestFunction.TEST_FUNCTION1 + " --member=INVALID_MEMBER");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
  }

  @Test
  public void testExecuteFunctionOnAllMembers() {
    CommandResult result =
        gfsh.executeAndVerifyCommand("execute function --id=" + TestFunction.TEST_FUNCTION1);
    assertThat(((JSONArray) result.getContent().get("Member ID/Name")).length()).isEqualTo(2);
  }

  @Test
  public void testExecuteFunctionOnMultipleMembers() {
    CommandResult result =
        gfsh.executeAndVerifyCommand("execute function --id=" + TestFunction.TEST_FUNCTION1
            + " --member=" + Strings.join(server1.getName(), server2.getName()).with(","));
    assertThat(((JSONArray) result.getContent().get("Member ID/Name")).length()).isEqualTo(2);
  }

  @Test
  public void testExecuteFunctionOnMultipleMembersWithArgsAndResultCollector() {
    CommandResult result = gfsh.executeAndVerifyCommand(
        "execute function --id=" + TestFunction.TEST_FUNCTION_RETURN_ARGS + " --arguments=arg1,arg2"
            + " --result-collector=" + ToUpperResultCollector.class.getName());
    assertThat(((JSONArray) result.getContent().get("Member ID/Name")).length()).isEqualTo(2);
    assertThat(((JSONArray) result.getContent().get("Function Execution Result")).getString(0))
        .contains("ARG1");
    assertThat(((JSONArray) result.getContent().get("Function Execution Result")).getString(1))
        .contains("ARG1");
  }

  @Test
  public void testExecuteFunctionOnGroup() {
    CommandResult result = gfsh.executeAndVerifyCommand(
        "execute function --id=" + TestFunction.TEST_FUNCTION1 + " --groups=group-1");
    assertThat(((JSONArray) result.getContent().get("Member ID/Name")).length()).isEqualTo(1);
  }

  @Test
  public void testDestroyFunctionOnMember() {
    gfsh.executeAndVerifyCommand(
        "destroy function --id=" + TestFunction.TEST_FUNCTION1 + " --member=" + server1.getName());
    CommandResult result = gfsh.executeAndVerifyCommand("list functions");
    assertThat(((JSONArray) result.getContent().get("Function")).length()).isEqualTo(3);
    gfsh.executeAndVerifyCommand(
        "destroy function --id=" + TestFunction.TEST_FUNCTION1 + " --member=" + server2.getName());
    result = gfsh.executeAndVerifyCommand("list functions");
    assertThat(((JSONArray) result.getContent().get("Function")).length()).isEqualTo(2);
  }

  @Test
  public void testDestroyFunctionOnGroup() {
    gfsh.executeAndVerifyCommand(
        "destroy function --id=" + TestFunction.TEST_FUNCTION1 + " --groups=group-1");
    CommandResult result = gfsh.executeAndVerifyCommand("list functions");
    assertThat(((JSONArray) result.getContent().get("Function")).length()).isEqualTo(3);
  }

  @Test
  public void testListFunctions() {
    CommandResult result = gfsh.executeAndVerifyCommand("list functions");
    assertThat(((JSONArray) result.getContent().get("Function")).length()).isEqualTo(4);

    result = gfsh.executeAndVerifyCommand("list functions --matches=Test.*");
    assertThat(((JSONArray) result.getContent().get("Function")).length()).isEqualTo(2);

    result = gfsh.executeAndVerifyCommand("list functions --matches=Test.* --groups=group-1");
    assertThat(((JSONArray) result.getContent().get("Function")).length()).isEqualTo(1);

    result = gfsh
        .executeAndVerifyCommand("list functions --matches=Test.* --members=" + server1.getName());
    assertThat(((JSONArray) result.getContent().get("Function")).length()).isEqualTo(1);
  }
}
