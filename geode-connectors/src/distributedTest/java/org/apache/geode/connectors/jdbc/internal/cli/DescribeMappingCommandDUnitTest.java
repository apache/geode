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

import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__CONNECTION_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__PDX_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__REGION_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__TABLE_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.DescribeMappingCommand.DESCRIBE_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.cli.DescribeMappingCommand.DESCRIBE_MAPPING__REGION_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.JDBCConnectorTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({JDBCConnectorTest.class})
public class DescribeMappingCommandDUnitTest implements Serializable {

  private static final String REGION_NAME = "testRegion";

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public transient ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private MemberVM locator, server;

  @Test
  public void describesExistingMapping() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + REGION_NAME + " --type=REPLICATE")
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, REGION_NAME);
    csb.addOption(CREATE_MAPPING__CONNECTION_NAME, "connection");
    csb.addOption(CREATE_MAPPING__TABLE_NAME, "testTable");
    csb.addOption(CREATE_MAPPING__PDX_NAME, "myPdxClass");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(DESCRIBE_MAPPING__REGION_NAME,
        REGION_NAME);

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsKeyValuePair(CREATE_MAPPING__REGION_NAME, REGION_NAME);
    commandResultAssert.containsKeyValuePair(CREATE_MAPPING__CONNECTION_NAME, "connection");
    commandResultAssert.containsKeyValuePair(CREATE_MAPPING__TABLE_NAME, "testTable");
    commandResultAssert.containsKeyValuePair(CREATE_MAPPING__PDX_NAME, "myPdxClass");
  }

  @Test
  public void reportsNoMappingFound() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + REGION_NAME + " --type=REPLICATE")
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_MAPPING)
        .addOption(DESCRIBE_MAPPING__REGION_NAME, "nonExisting");

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsError();
    commandResultAssert.containsOutput(
        String.format("(Experimental) \n" + "mapping for region 'nonExisting' not found"));
  }

  @Test
  public void reportConfigurationFoundOnMember() throws Exception {
    Properties properties = new Properties();
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");

    locator = startupRule.startLocatorVM(0, properties);
    server = startupRule.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + REGION_NAME + " --type=REPLICATE")
        .statusIsSuccess();

    server.invoke(() -> createRegionMapping());

    CommandResultAssert commandResultAssert =
        gfsh.executeAndAssertThat(DESCRIBE_MAPPING + " --region=" + REGION_NAME).statusIsSuccess();

    commandResultAssert.containsKeyValuePair(CREATE_MAPPING__REGION_NAME, REGION_NAME);
    commandResultAssert.containsKeyValuePair(CREATE_MAPPING__CONNECTION_NAME, "connection");
    commandResultAssert.containsKeyValuePair(CREATE_MAPPING__TABLE_NAME, "testTable");
    commandResultAssert.containsKeyValuePair(CREATE_MAPPING__PDX_NAME, "myPdxClass");
  }

  private void createRegionMapping() throws RegionMappingExistsException {
    InternalCache cache = ClusterStartupRule.getCache();
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    service.createRegionMapping(new RegionMapping(REGION_NAME, "myPdxClass",
        "testTable", "connection"));
    assertThat(service.getMappingForRegion(REGION_NAME)).isNotNull();
  }
}
