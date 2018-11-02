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
package org.apache.geode.connectors.jdbc.internal.cli;

import static org.apache.geode.connectors.jdbc.internal.cli.ListMappingCommand.LIST_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.cli.ListMappingCommand.LIST_OF_MAPPINGS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;


public class ListMappingCommandDUnitTest implements Serializable {

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public transient ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private MemberVM locator;
  private MemberVM server;

  private String regionName = "testRegion";

  @Test
  public void listsRegionMappingFromClusterConfiguration() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();

    String mapping = "create jdbc-mapping --region=testRegion --connection=connection "
        + "--table=myTable --pdx-name=myPdxClass";
    gfsh.executeAndAssertThat(mapping).statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(LIST_MAPPING);
    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();
    commandResultAssert.tableHasRowCount(LIST_OF_MAPPINGS, 1);
    commandResultAssert.tableHasColumnOnlyWithValues(LIST_OF_MAPPINGS, regionName);
  }

  @Test
  public void listsRegionMappingsFromMember() throws Exception {
    Properties properties = new Properties();
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");

    locator = startupRule.startLocatorVM(0, properties);
    server = startupRule.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();

    server.invoke(() -> createNRegionMappings(3));

    CommandResultAssert commandResultAssert =
        gfsh.executeAndAssertThat(LIST_MAPPING).statusIsSuccess();

    commandResultAssert.statusIsSuccess();
    commandResultAssert.tableHasRowCount(LIST_OF_MAPPINGS, 3);
    commandResultAssert.tableHasColumnOnlyWithValues(LIST_OF_MAPPINGS, regionName + "-1",
        regionName + "-2", regionName + "-3");
  }

  @Test
  public void reportsNoRegionMappingsFound() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(LIST_MAPPING);

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsOutput("No mappings found");
  }

  private void createNRegionMappings(int N) throws RegionMappingExistsException {
    InternalCache cache = ClusterStartupRule.getCache();
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    for (int i = 1; i <= N; i++) {
      String name = regionName + "-" + i;
      service.createRegionMapping(
          new RegionMapping(name, "x.y.MyPdxClass", "table", "connection"));
      assertThat(service.getMappingForRegion(name)).isNotNull();
    }
  }
}
