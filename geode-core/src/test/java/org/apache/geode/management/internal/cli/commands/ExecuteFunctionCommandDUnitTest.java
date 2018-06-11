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
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.assertions.TabularResultModelAssert;
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
        "create region --name=regionA --type=PARTITION --group=group1 --redundant-copies=0 --total-num-buckets=2 --partition-resolver=org.apache.geode.management.internal.cli.commands.ExecuteFunctionCommandDUnitTest$MyPartitionResolver")
        .statusIsSuccess()
        .tableHasColumnOnlyWithValues("Member", "server-1", "server-2");

    locator.waitTillRegionsAreReadyOnServers("/regionA", 2);

    server1.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion("/regionA");
      region.put("a", "a");
      region.put("b", "b");
    });

    // this makes sure entry a and entry b are on different member
    CommandResultAssert locateACommand =
        gfsh.executeAndAssertThat("locate entry --key=a --region=/regionA").statusIsSuccess()
            .hasSection("location", "data-info");
    locateACommand.hasDataSection().hasContent().containsEntry("Locations Found", "1");
    locateACommand.hasTableSection().hasColumnSize(4).hasColumn("MemberName").hasSize(1)
        .isSubsetOf("server-1", "server-2");

    CommandResultAssert locateBCommand =
        gfsh.executeAndAssertThat("locate entry --key=b --region=/regionA").statusIsSuccess()
            .hasSection("location", "data-info");
    locateBCommand.hasDataSection().hasContent().containsEntry("Locations Found", "1");
    locateBCommand.hasTableSection().hasColumnSize(4).hasColumn("MemberName").hasSize(1)
        .isSubsetOf("server-1", "server-2");

    String member1 =
        locateACommand.getResultModel().getTableSection("location").getValue("MemberName", 0);
    String member2 =
        locateBCommand.getResultModel().getTableSection("location").getValue("MemberName", 0);

    assertThat(member1).isNotEqualToIgnoringCase(member2);
  }

  @Test
  public void noExtraArgument() {
    TabularResultModelAssert tableAssert =
        gfsh.executeAndAssertThat(command).statusIsSuccess()
            .hasTableSection()
            .hasRowSize(3)
            .hasColumnSize(3);
    tableAssert.hasColumn("Member").containsExactlyInAnyOrder("server-1", "server-2", "server-3");
    tableAssert.hasColumn("Message").containsExactlyInAnyOrder("[genericFunctionId]",
        "[genericFunctionId]", "[genericFunctionId]");
  }

  @Test
  public void withMemberOnly() {
    gfsh.executeAndAssertThat(command + "--member=server-1").statusIsSuccess()
        .hasTableSection()
        .hasRowSize(1)
        .hasColumnSize(3)
        .hasRow(0).containsExactly("server-1", "OK", "[genericFunctionId]");
  }

  @Test
  public void withGroupOnly() {
    TabularResultModelAssert tableAssert =
        gfsh.executeAndAssertThat(command + "--group=group1").statusIsSuccess()
            .hasTableSection()
            .hasRowSize(2)
            .hasColumnSize(3);
    tableAssert.hasColumn("Member").containsExactlyInAnyOrder("server-1", "server-2");
    tableAssert.hasColumn("Message").containsExactly("[genericFunctionId]", "[genericFunctionId]");
  }

  @Test
  public void withArgumentsOnly() {
    TabularResultModelAssert tableAssert =
        gfsh.executeAndAssertThat(command + "--arguments=arguments").statusIsSuccess()
            .hasTableSection()
            .hasRowSize(3)
            .hasColumnSize(3);
    tableAssert.hasColumn("Member").containsExactlyInAnyOrder("server-1", "server-2", "server-3");
    tableAssert.hasColumn("Message").containsExactlyInAnyOrder("[genericFunctionId-arguments]",
        "[genericFunctionId-arguments]", "[genericFunctionId-arguments]");
  }


  @Test
  public void withRegionOnly() {
    // function is only executed on one member, but the returned message is repeated twice
    // i.e. that member will execute the function on other members
    gfsh.executeAndAssertThat(command + "--region=/regionA").statusIsSuccess()
        .hasTableSection()
        .hasRowSize(1)
        .hasColumnSize(3)
        .hasRow(0)
        .isSubsetOf("server-1", "server-2", "OK", "[genericFunctionId, genericFunctionId]");
  }

  @Test
  public void withRegionAndFilterMatchingMultipleMembers() {
    // the function is executed on multiple members, the expected result is either
    // "[genericFunctionId-a, genericFunctionId-b]"
    // or "[genericFunctionId-b, genericFunctionId-a]" depending which server's function gets
    // executed first
    gfsh.executeAndAssertThat(command + "--region=/regionA --filter=a,b").statusIsSuccess()
        .hasTableSection()
        .hasRowSize(1)
        .hasColumnSize(3)
        .hasRow(0)
        .isSubsetOf("server-1", "server-2", "OK", "[genericFunctionId-b, genericFunctionId-a]",
            "[genericFunctionId-a, genericFunctionId-b]");
  }

  @Test
  public void withRegionAndFilterMatchingOnlyOneMember() {
    gfsh.executeAndAssertThat(command + "--region=/regionA --filter=a").statusIsSuccess()
        .hasTableSection()
        .hasRowSize(1)
        .hasColumnSize(3)
        .hasRow(0)
        .isSubsetOf("server-1", "server-2", "OK", "[genericFunctionId-a]");
  }

  @Test
  public void withRegionAndArguments() {
    gfsh.executeAndAssertThat(command + "--region=/regionA --arguments=arguments").statusIsSuccess()
        .hasTableSection()
        .hasRowSize(1)
        .hasColumnSize(3)
        .hasRow(0)
        .isSubsetOf("server-1", "server-2", "OK",
            "[genericFunctionId-arguments, genericFunctionId-arguments]");
  }

  @Test
  public void withRegionAndFilterAndArgument() {
    gfsh.executeAndAssertThat(command + "--region=/regionA --filter=b --arguments=arguments")
        .hasTableSection()
        .hasRowSize(1)
        .hasColumnSize(3)
        .hasRow(0)
        .isSubsetOf("server-1", "server-2", "OK", "[genericFunctionId-b-arguments]");
  }

  @Test
  public void withRegionAndFilterAndArgumentAndResultCollector() {
    gfsh.executeAndAssertThat(
        command + "--region=/regionA --filter=a --arguments=arguments --result-collector="
            + ToUpperResultCollector.class.getName())
        .hasTableSection()
        .hasRowSize(1)
        .hasColumnSize(3)
        .hasRow(0)
        .isSubsetOf("server-1", "server-2", "OK", "[GENERICFUNCTIONID-A-ARGUMENTS]");
  }

  @Test
  public void withRegionAndArgumentAndResultCollector() {
    gfsh.executeAndAssertThat(
        command + "--region=/regionA --arguments=arguments --result-collector="
            + ToUpperResultCollector.class.getName())
        .hasTableSection()
        .hasRowSize(1)
        .hasColumnSize(3)
        .hasRow(0)
        .isSubsetOf("server-1", "server-2", "OK",
            "[GENERICFUNCTIONID-ARGUMENTS, GENERICFUNCTIONID-ARGUMENTS]");
  }

  @Test
  public void withMemberAndArgument() {
    gfsh.executeAndAssertThat(command + "--member=server-3 --arguments=arguments").statusIsSuccess()
        .hasTableSection()
        .hasRowSize(1)
        .hasColumnSize(3)
        .hasRow(0)
        .containsExactly("server-3", "OK", "[genericFunctionId-arguments]");
  }

  @Test
  public void withMemberAndArgumentAndResultCollector() {
    gfsh.executeAndAssertThat(
        command + "--member=server-1 --arguments=arguments  --result-collector="
            + ToUpperResultCollector.class.getName())
        .statusIsSuccess()
        .hasTableSection()
        .hasRowSize(1)
        .hasColumnSize(3)
        .hasRow(0)
        .containsExactly("server-1", "OK", "[GENERICFUNCTIONID-ARGUMENTS]");
  }

  @Test
  public void withArgumentAndResultCollector() {

    TabularResultModelAssert tableAssert =
        gfsh.executeAndAssertThat(command + "--arguments=arguments  --result-collector="
            + ToUpperResultCollector.class.getName()).statusIsSuccess()
            .hasTableSection()
            .hasRowSize(3)
            .hasColumnSize(3);

    tableAssert.hasColumn("Member").containsExactlyInAnyOrder("server-1", "server-2", "server-3");
    tableAssert.hasColumn("Message").containsExactlyInAnyOrder("[GENERICFUNCTIONID-ARGUMENTS]",
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
