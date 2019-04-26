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


import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.dto.Key;
import org.apache.geode.management.internal.cli.dto.Value;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;



public class LocateEntryDUnitTest {
  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator, server1, server2;

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = lsRule.startLocatorVM(0);

    Properties props = new Properties();
    props.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.management.internal.cli.dto.*");

    server1 = lsRule.startServerVM(1, props, locator.getPort());
    server2 = lsRule.startServerVM(2, props, locator.getPort());

    gfsh.connectAndVerify(locator);

    // create a partitioned region and put an entry
    gfsh.executeAndAssertThat("create region --name=regionA --type=PARTITION").statusIsSuccess();
    gfsh.executeAndAssertThat("put --region=regionA --key=key --value=value").statusIsSuccess();

    // create a replicate region and put an entry
    gfsh.executeAndAssertThat("create region --name=regionB --type=REPLICATE").statusIsSuccess();
    gfsh.executeAndAssertThat("put --region=regionB --key=key --value=value").statusIsSuccess();

    // create a child replicate region
    gfsh.executeAndAssertThat("create region --name=regionB/regionBB --type=REPLICATE")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("put --region=regionB/regionBB --key=key --value=value")
        .statusIsSuccess();

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/regionA", 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/regionB", 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/regionB/regionBB", 2);
  }

  @Test
  public void locateEntryForPartitionedRegion() throws Exception {
    gfsh.executeAndAssertThat("locate entry --region=regionA --key=key")
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION).hasContent()
        .containsEntry("Locations Found", "1");
  }

  @Test
  public void locateEntryForReplicateRegion() throws Exception {
    gfsh.executeAndAssertThat("locate entry --region=regionB --key=key")
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION).hasContent()
        .containsEntry("Locations Found", "2");
  }

  @Test
  public void recursiveLocate() throws Exception {
    gfsh.executeAndAssertThat("locate entry --region=regionB --key=key --recursive=true")
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION).hasContent()
        .containsEntry("Locations Found", "4");
  }

  @Test
  public void jsonKeyValue() throws Exception {
    gfsh.executeAndAssertThat(
        "put --region=regionA --key=('key':'1') --value=('value':'1') " + "--key-class="
            + Key.class.getCanonicalName() + " --value-class=" + Value.class.getCanonicalName())
        .statusIsSuccess();
    gfsh.executeAndAssertThat("locate entry --region=regionA --key=('key':'1') " + "--key-class="
        + Key.class.getCanonicalName() + " --value-class=" + Value.class.getCanonicalName())
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION).hasContent()
        .containsEntry("Locations Found", "1");
  }
}
