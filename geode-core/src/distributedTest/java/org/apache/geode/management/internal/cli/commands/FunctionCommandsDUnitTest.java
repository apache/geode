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

import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION1;
import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION_ALWAYS_THROWS_EXCEPTION;
import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION_ON_ONE_MEMBER_RETURN_ARGS;
import static org.apache.geode.internal.cache.functions.TestFunction.TEST_FUNCTION_RETURN_ARGS;
import static org.awaitility.Awaitility.await;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.assertj.core.util.Strings;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({DistributedTest.class})
public class FunctionCommandsDUnitTest {
  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;

  private static final String REGION_ONE = "RegionOne";
  private static final String REGION_TWO = "RegionTwo";
  private static final String RESULT_HEADER = "Message";

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void before() throws Exception {
    locator = lsRule.startLocatorVM(0);

    Properties props = new Properties();
    props.setProperty("groups", "group-1");
    server1 = lsRule.startServerVM(1, props, locator.getPort());

    server2 = lsRule.startServerVM(2, locator.getPort());

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();

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
      Cache cache = ClusterStartupRule.getCache();
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

    locator.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      ManagementService managementService = ManagementService.getManagementService(cache);
      DistributedSystemMXBean dsMXBean = managementService.getDistributedSystemMXBean();

      await().atMost(120, TimeUnit.SECONDS).until(() -> dsMXBean.getMemberCount() == 3);
    });
  }

  @Before
  public void setup() throws Exception {
    registerFunction(new TestFunction(true, TEST_FUNCTION1), locator, server1, server2);
    registerFunction(new TestFunction(true, TEST_FUNCTION_RETURN_ARGS), locator, server1, server2);
    registerFunction(new TestFunction(true, TEST_FUNCTION_ALWAYS_THROWS_EXCEPTION), locator,
        server1, server2);
    registerFunction(new TestFunction(true, TEST_FUNCTION_ON_ONE_MEMBER_RETURN_ARGS), locator,
        server1);

    connectGfsh();
  }

  public void connectGfsh() throws Exception {
    gfsh.connectAndVerify(getLocator().getJmxPort(), GfshCommandRule.PortType.jmxManager);
  }

  public MemberVM getLocator() {
    return locator;
  }

  private static void registerFunction(Function function, MemberVM... vms) {
    for (MemberVM vm : vms) {
      vm.invoke(() -> FunctionService.registerFunction(function));
    }
  }

  @Test
  public void testExecuteFunctionOnRegion() throws Exception {
    gfsh.executeAndAssertThat(
        "execute function --id=" + TEST_FUNCTION1 + " --region=/" + REGION_ONE).statusIsSuccess()
        .tableHasColumnWithValuesContaining("Member", server1.getName(), server2.getName());
  }

  @Test
  public void testExecuteFunctionOnUnknownRegion() throws Exception {
    gfsh.executeAndAssertThat("execute function --id=" + TEST_FUNCTION1 + " --region=/UNKNOWN")
        .statusIsError().containsOutput("No members found");
  }

  @Test
  public void testExecuteUnknownFunction() throws Exception {
    gfsh.executeAndAssertThat("execute function --id=UNKNOWN_FUNCTION").statusIsError()
        .containsOutput("UNKNOWN_FUNCTION is not registered on member");
  }

  @Test
  public void testExecuteFunctionOnRegionWithCustomResultCollector() {
    gfsh.executeAndAssertThat(
        "execute function --id=" + TEST_FUNCTION_RETURN_ARGS + " --region=" + REGION_ONE
            + " --arguments=arg1" + " --result-collector=" + ToUpperResultCollector.class.getName())
        .statusIsSuccess()
        .tableHasColumnOnlyWithValues(RESULT_HEADER, "[ARG1, ARG1]", "[ARG1, ARG1]");
  }

  @Test
  public void testExecuteFunctionOnMember() {
    gfsh.executeAndAssertThat(
        "execute function --id=" + TEST_FUNCTION1 + " --member=" + server1.getMember().getName())
        .statusIsSuccess().tableHasColumnWithValuesContaining("Member", server1.getName());
  }

  @Test
  public void testExecuteFunctionOnInvalidMember() {
    gfsh.executeAndAssertThat(
        "execute function --id=" + TEST_FUNCTION1 + " --member=INVALID_MEMBER").statusIsError();
  }

  @Test
  public void testExecuteFunctionOnAllMembers() {
    gfsh.executeAndAssertThat("execute function --id=" + TEST_FUNCTION1).statusIsSuccess()
        .tableHasColumnWithValuesContaining("Member", server1.getName(), server2.getName())
        .tableHasColumnWithExactValuesInAnyOrder(RESULT_HEADER, "[false]", "[false]");
  }

  @Test
  public void testExecuteFunctionOnMultipleMembers() {
    gfsh.executeAndAssertThat("execute function --id=" + TEST_FUNCTION1 + " --member="
        + Strings.join(server1.getName(), server2.getName()).with(",")).statusIsSuccess()
        .tableHasColumnWithValuesContaining("Member", server1.getName(), server2.getName())
        .tableHasColumnWithExactValuesInAnyOrder(RESULT_HEADER, "[false]", "[false]");
  }

  @Test
  public void testExecuteFunctionOnMultipleMembersWithArgsAndResultCollector() {
    gfsh.executeAndAssertThat("execute function --id=" + TEST_FUNCTION_RETURN_ARGS
        + " --arguments=arg1" + " --result-collector=" + ToUpperResultCollector.class.getName())
        .statusIsSuccess()
        .tableHasColumnWithValuesContaining("Member", server1.getName(), server2.getName())
        .tableHasColumnWithExactValuesInAnyOrder(RESULT_HEADER, "[ARG1]", "[ARG1]");
  }

  @Test
  public void testFunctionOnlyRegisteredOnOneMember() {
    gfsh.executeAndAssertThat("execute function --id=" + TEST_FUNCTION_ON_ONE_MEMBER_RETURN_ARGS)
        .tableHasColumnWithValuesContaining(RESULT_HEADER, "[false]",
            "Function : executeFunctionOnOneMemberToReturnArgs is not registered on member.")
        .statusIsError();
  }

  @Test
  public void testExecuteFunctionOnGroup() {
    gfsh.executeAndAssertThat("execute function --id=" + TEST_FUNCTION1 + " --groups=group-1")
        .statusIsSuccess().tableHasColumnWithValuesContaining("Member", server1.getName())
        .tableHasColumnWithExactValuesInAnyOrder(RESULT_HEADER, "[false]");
  }

  @Test
  public void testDestroyFunctionOnMember() {
    gfsh.executeAndAssertThat(
        "destroy function --id=" + TEST_FUNCTION1 + " --member=" + server1.getName())
        .statusIsSuccess();

    gfsh.executeAndAssertThat("list functions").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Function", TEST_FUNCTION_RETURN_ARGS,
            TEST_FUNCTION1, TEST_FUNCTION_RETURN_ARGS, TEST_FUNCTION_ALWAYS_THROWS_EXCEPTION,
            TEST_FUNCTION_ALWAYS_THROWS_EXCEPTION, TEST_FUNCTION_ON_ONE_MEMBER_RETURN_ARGS);
    gfsh.executeAndAssertThat(
        "destroy function --id=" + TEST_FUNCTION1 + " --member=" + server2.getName())
        .statusIsSuccess();
    gfsh.executeAndAssertThat("list functions").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Function", TEST_FUNCTION_RETURN_ARGS,
            TEST_FUNCTION_RETURN_ARGS, TEST_FUNCTION_ALWAYS_THROWS_EXCEPTION,
            TEST_FUNCTION_ALWAYS_THROWS_EXCEPTION, TEST_FUNCTION_ON_ONE_MEMBER_RETURN_ARGS);
  }

  @Test
  public void testDestroyFunctionOnGroup() {
    gfsh.executeAndAssertThat("destroy function --id=" + TEST_FUNCTION1 + " --groups=group-1")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("list functions").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Function", TEST_FUNCTION_RETURN_ARGS,
            TEST_FUNCTION1, TEST_FUNCTION_RETURN_ARGS, TEST_FUNCTION_ALWAYS_THROWS_EXCEPTION,
            TEST_FUNCTION_ALWAYS_THROWS_EXCEPTION, TEST_FUNCTION_ON_ONE_MEMBER_RETURN_ARGS);
  }

  @Test
  public void testListFunctions() {
    gfsh.executeAndAssertThat("list functions").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Function", TEST_FUNCTION1, TEST_FUNCTION1,
            TEST_FUNCTION_RETURN_ARGS, TEST_FUNCTION_RETURN_ARGS,
            TEST_FUNCTION_ALWAYS_THROWS_EXCEPTION, TEST_FUNCTION_ALWAYS_THROWS_EXCEPTION,
            TEST_FUNCTION_ON_ONE_MEMBER_RETURN_ARGS);

    gfsh.executeAndAssertThat("list functions --matches=Test.*").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Function", TEST_FUNCTION1, TEST_FUNCTION1,
            TEST_FUNCTION_ALWAYS_THROWS_EXCEPTION, TEST_FUNCTION_ALWAYS_THROWS_EXCEPTION);

    gfsh.executeAndAssertThat("list functions --matches=Test.* --groups=group-1").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Function", TEST_FUNCTION1,
            TEST_FUNCTION_ALWAYS_THROWS_EXCEPTION);

    gfsh.executeAndAssertThat("list functions --matches=Test.* --members=" + server1.getName())
        .statusIsSuccess().tableHasColumnWithExactValuesInAnyOrder("Function", TEST_FUNCTION1,
            TEST_FUNCTION_ALWAYS_THROWS_EXCEPTION);
  }

  @Test
  public void testFunctionException() {
    gfsh.executeAndAssertThat("execute function --id=" + TEST_FUNCTION_ALWAYS_THROWS_EXCEPTION)
        .tableHasColumnWithValuesContaining(RESULT_HEADER, "I have been thrown from TestFunction",
            "I have been thrown from TestFunction")
        .statusIsError();
  }
}
