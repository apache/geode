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

import static org.apache.geode.cache.Region.SEPARATOR;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({RegionsTest.class})
public class CreateRegionCommandWithNoClusterConfigDUnitTest {

  private static MemberVM locator, server1, server2;

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TestName testName = new SerializableTestName();

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder();

  @BeforeClass
  public static void before() throws Exception {
    locator = lsRule.startLocatorVM(0, LocatorStarterRule::withoutClusterConfigurationService);
    server1 = lsRule.startServerVM(1, "group1", locator.getPort());
    server2 = lsRule.startServerVM(2, "group2", locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void multipleTemplateRegionTypes() throws Exception {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --type=REPLICATE --group=group1")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --type=REPLICATE_PROXY --group=group2")
        .statusIsSuccess();

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + regionName, 2);

    gfsh.executeAndAssertThat("create region --name=failed --template-region=" + regionName)
        .statusIsError()
        .hasInfoSection().hasOutput()
        .contains("Multiple types of template region " + SEPARATOR
            + "multipleTemplateRegionTypes exist.");
  }

  @Test
  public void multipleTemplateRegionWithSameType() throws Exception {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + regionName, 2);
    gfsh.executeAndAssertThat("create region --name=success --template-region=" + regionName)
        .statusIsSuccess();
  }
}
