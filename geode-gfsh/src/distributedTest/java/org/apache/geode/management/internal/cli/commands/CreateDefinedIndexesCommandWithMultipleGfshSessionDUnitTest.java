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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({OQLIndexTest.class})
public class CreateDefinedIndexesCommandWithMultipleGfshSessionDUnitTest {
  private static MemberVM locator0, locator1, server2, server3;

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator0 = clusterStartupRule.startLocatorVM(0);
    locator1 = clusterStartupRule.startLocatorVM(1, locator0.getPort());
    server2 = clusterStartupRule.startServerVM(2, locator0.getPort());
    server3 = clusterStartupRule.startServerVM(3, locator0.getPort());
  }

  @Test
  public void defineAndCreateInSeparateGfshSessions() throws Exception {
    gfsh.connectAndVerify(locator0);

    gfsh.executeAndAssertThat("create region --name=regionA --type=REPLICATE")
        .statusIsSuccess();
    locator0.waitUntilRegionIsReadyOnExactlyThisManyServers("/regionA", 2);

    gfsh.executeAndAssertThat("define index --name=index1 --expression=id --region=regionA")
        .statusIsSuccess().containsOutput("Index successfully defined");

    MemberVM.invokeInEveryMember(() -> {
      assertThat(IndexDefinition.indexDefinitions)
          .extracting(RegionConfig.Index::getName).containsExactly("index1");
    }, locator0, locator1);

    gfsh.disconnect();
    gfsh.connectAndVerify(locator1);

    gfsh.executeAndAssertThat("define index --name=index2 --expression=name --region=regionA")
        .statusIsSuccess().containsOutput("Index successfully defined");

    MemberVM.invokeInEveryMember(() -> {
      assertThat(IndexDefinition.indexDefinitions)
          .extracting(RegionConfig.Index::getName)
          .containsExactlyInAnyOrder("index1", "index2");
    }, locator0, locator1);

    // 2 indexes created on 2 servers, thus 4 rows
    gfsh.executeAndAssertThat("create defined indexes")
        .statusIsSuccess()
        .hasTableSection(CreateDefinedIndexesCommand.CREATE_DEFINED_INDEXES_SECTION)
        .hasRowSize(4)
        .hasColumn("Message")
        .asList()
        .allMatch(s -> s.startsWith("Created index"));

    assertThat(gfsh.getGfshOutput())
        .contains("Cluster configuration for group 'cluster' is updated");

    // verify at this point, indexes are still present in each locator's memory
    MemberVM.invokeInEveryMember(() -> {
      assertThat(IndexDefinition.indexDefinitions)
          .extracting(RegionConfig.Index::getName)
          .containsExactlyInAnyOrder("index1", "index2");
    }, locator0, locator1);

    gfsh.disconnect();
    gfsh.connectAndVerify(locator0);

    // issue create defined index again will result in error.
    gfsh.executeAndAssertThat("create defined indexes")
        .statusIsError()
        .hasTableSection(CreateDefinedIndexesCommand.CREATE_DEFINED_INDEXES_SECTION)
        .hasRowSize(4)
        .hasColumn("Message")
        .asList()
        .allMatch(s -> s.startsWith("Failed to create index") && s.endsWith("already exists."));

    gfsh.executeAndAssertThat("clear defined indexes")
        .statusIsSuccess()
        .containsOutput("Index definitions successfully cleared");

    MemberVM.invokeInEveryMember(() -> {
      assertThat(IndexDefinition.indexDefinitions).isEmpty();
    }, locator0, locator1);
  }


}
