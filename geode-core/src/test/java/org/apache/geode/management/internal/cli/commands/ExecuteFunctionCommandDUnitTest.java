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

import java.util.Set;

import org.apache.logging.log4j.util.Strings;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.FixedPartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.management.internal.cli.result.ModelCommandResult;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({DistributedTest.class, GfshTest.class})
public class ExecuteFunctionCommandDUnitTest {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator, server1, server2, server3;
  private static final String functionId = "genericFunctionId";

  private static String command = "execute function --id=" + functionId + " ";

  @BeforeClass
  public static void setUpClass() throws Exception {
    locator = cluster.startLocatorVM(0);
    gfsh.connectAndVerify(locator);

    server1 = cluster.startServerVM(1, "group1", locator.getPort());
    server2 = cluster.startServerVM(2, "group1", locator.getPort());
    server3 = cluster.startServerVM(3, "group2", locator.getPort());
    MemberVM.invokeInEveryMember(() -> {
      FunctionService.registerFunction(new GenericFunctionOp(functionId));
    }, server1, server2, server3);

    // create a partitioned region on only group1
    gfsh.executeAndAssertThat(
        "create region --name=regionA --type=PARTITION --group=group1 --redundant-copies=0 --total-num-buckets=2 --partition-resolver=org.apache.geode.management.internal.cli.commands.ExecuteFunctionCommandDUnitTest$MyPartitionResolver");

    locator.waitTillRegionsAreReadyOnServers("/regionA", 2);

    server1.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion("/regionA");
      region.put("a", "a");
      region.put("b", "b");
    });

    // this makes sure entry a and entry b are on different member
    CommandResultAssert locate1 =
        gfsh.executeAndAssertThat("locate entry --key=a --region=/regionA").statusIsSuccess();
    CommandResultAssert locate2 =
        gfsh.executeAndAssertThat("locate entry --key=b --region=/regionA").statusIsSuccess();

    TabularResultModel section1 =
        ((ModelCommandResult) locate1.getCommandResult()).getResultData()
            .getTableSection("location");
    String member1 = section1.getContent().get("MemberName").get(0);
    TabularResultModel section2 =
        ((ModelCommandResult) locate2.getCommandResult()).getResultData()
            .getTableSection("location");
    String member2 = section2.getContent().get("MemberName").get(0);
    assertThat(member1).isNotEqualToIgnoringCase(member2);
  }

  @Test
  public void noExtraArgument() {
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-1", "server-2", "server-3")
        .tableHasColumnWithExactValuesInAnyOrder("Message", "[genericFunctionId]",
            "[genericFunctionId]", "[genericFunctionId]");
  }

  @Test
  public void withMemberOnly() {
    gfsh.executeAndAssertThat(command + "--member=server-1").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-1")
        .tableHasColumnWithExactValuesInAnyOrder("Message", "[genericFunctionId]");
  }

  @Test
  public void withGroupOnly() {
    gfsh.executeAndAssertThat(command + "--group=group1").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-1", "server-2")
        .tableHasColumnWithExactValuesInAnyOrder("Message", "[genericFunctionId]",
            "[genericFunctionId]");
  }

  @Test
  public void withArgumentsOnly() {
    gfsh.executeAndAssertThat(command + "--arguments=arguments").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-1", "server-2", "server-3")
        .tableHasColumnWithExactValuesInAnyOrder("Message", "[genericFunctionId-arguments]",
            "[genericFunctionId-arguments]", "[genericFunctionId-arguments]");
  }


  @Test
  public void withRegionOnly() {
    // function is only executed on one member, but the returned message is repeated twice
    // i.e. that member will execute the function on other members
    gfsh.executeAndAssertThat(command + "--region=/regionA").statusIsSuccess()
        .tableHasRowCount("Member", 1)
        .tableHasColumnWithExactValuesInAnyOrder("Message",
            "[genericFunctionId, genericFunctionId]");
  }

  @Test
  public void withRegionAndFilterMatchingMultipleMembers() {
    // the function is executed on multiple members, the expected result is either
    // "[genericFunctionId-a, genericFunctionId-b]"
    // or "[genericFunctionId-b, genericFunctionId-a]" depending which server's function gets
    // executed first
    gfsh.executeAndAssertThat(command + "--region=/regionA --filter=a,b").statusIsSuccess()
        .tableHasRowCount("Member", 1)
        .tableHasColumnWithValuesContaining("Message", "[genericFunctionId-a, genericFunctionId-b]",
            "[genericFunctionId-b, genericFunctionId-a]");
  }

  @Test
  public void withRegionAndFilterMatchingOnlyOneMember() {
    gfsh.executeAndAssertThat(command + "--region=/regionA --filter=a").statusIsSuccess()
        .tableHasRowCount("Member", 1)
        .tableHasColumnWithExactValuesInAnyOrder("Message", "[genericFunctionId-a]");
  }

  @Test
  public void withRegionAndArguments() {
    gfsh.executeAndAssertThat(command + "--region=/regionA --arguments=arguments").statusIsSuccess()
        .tableHasRowCount("Member", 1)
        .tableHasColumnWithExactValuesInAnyOrder("Message",
            "[genericFunctionId-arguments, genericFunctionId-arguments]");
  }

  @Test
  public void withRegionAndFilterAndArgument() {
    gfsh.executeAndAssertThat(command + "--region=/regionA --filter=b --arguments=arguments")
        .tableHasRowCount("Member", 1)
        .tableHasColumnWithExactValuesInAnyOrder("Message", "[genericFunctionId-b-arguments]");
  }

  @Test
  public void withRegionAndFilterAndArgumentAndResultCollector() {
    gfsh.executeAndAssertThat(
        command + "--region=/regionA --filter=a --arguments=arguments --result-collector="
            + ToUpperResultCollector.class.getName())
        .tableHasRowCount("Member", 1)
        .tableHasColumnWithExactValuesInAnyOrder("Message", "[GENERICFUNCTIONID-A-ARGUMENTS]");
  }

  @Test
  public void withRegionAndArgumentAndResultCollector() {
    gfsh.executeAndAssertThat(
        command + "--region=/regionA --arguments=arguments --result-collector="
            + ToUpperResultCollector.class.getName())
        .tableHasRowCount("Member", 1)
        .tableHasColumnWithExactValuesInAnyOrder("Message",
            "[GENERICFUNCTIONID-ARGUMENTS, GENERICFUNCTIONID-ARGUMENTS]");
  }

  @Test
  public void withMemberAndArgument() {
    gfsh.executeAndAssertThat(command + "--member=server-3 --arguments=arguments").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3")
        .tableHasColumnWithExactValuesInAnyOrder("Message", "[genericFunctionId-arguments]");
  }

  @Test
  public void withMemberAndArgumentAndResultCollector() {
    gfsh.executeAndAssertThat(
        command + "--member=server-1 --arguments=arguments  --result-collector="
            + ToUpperResultCollector.class.getName())
        .statusIsSuccess().tableHasColumnWithExactValuesInAnyOrder("Member", "server-1")
        .tableHasColumnWithExactValuesInAnyOrder("Message", "[GENERICFUNCTIONID-ARGUMENTS]");
  }

  @Test
  public void withArgumentAndResultCollector() {
    gfsh.executeAndAssertThat(command + "--arguments=arguments  --result-collector="
        + ToUpperResultCollector.class.getName()).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-1", "server-2", "server-3")
        .tableHasColumnWithExactValuesInAnyOrder("Message", "[GENERICFUNCTIONID-ARGUMENTS]",
            "[GENERICFUNCTIONID-ARGUMENTS]", "[GENERICFUNCTIONID-ARGUMENTS]");
  }


  public static class MyPartitionResolver implements FixedPartitionResolver {
    @Override
    public String getPartitionName(final EntryOperation opDetails,
        @Deprecated final Set targetPartitions) {
      return (String) opDetails.getKey();
    }

    @Override
    public Object getRoutingObject(final EntryOperation opDetails) {
      return opDetails.getKey();
    }

    @Override
    public String getName() {
      return getClass().getName();
    }

    @Override
    public void close() {

    }
  }


  public static class GenericFunctionOp implements Function {
    private String functionId;

    GenericFunctionOp(String functionId) {
      this.functionId = functionId;
    }

    @Override
    public void execute(FunctionContext context) {
      String filter = null;
      if (context instanceof RegionFunctionContext) {
        RegionFunctionContext rContext = (RegionFunctionContext) context;
        Set filters = rContext.getFilter();
        filter = Strings.join(filters, ',');
      }

      String argument = null;
      Object arguments = (context.getArguments());
      if (arguments instanceof String[]) {
        argument = String.join(",", (String[]) arguments);
      }

      if (filter != null && argument != null) {
        context.getResultSender().lastResult(functionId + "-" + filter + "-" + argument);
      } else if (filter != null) {
        context.getResultSender().lastResult(functionId + "-" + filter);
      } else if (argument != null) {
        context.getResultSender().lastResult(functionId + "-" + argument);
      } else {
        context.getResultSender().lastResult(functionId);
      }
    }

    public String getId() {
      return functionId;
    }
  }
}
