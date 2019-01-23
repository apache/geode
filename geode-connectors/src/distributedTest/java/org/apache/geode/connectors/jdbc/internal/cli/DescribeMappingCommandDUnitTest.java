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
import static org.apache.geode.connectors.jdbc.internal.cli.DescribeMappingCommand.DESCRIBE_MAPPING;
import static org.apache.geode.connectors.util.internal.MappingConstants.CATALOG_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.DATA_SOURCE_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.ID_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.PDX_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.REGION_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.SCHEMA_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.SYNCHRONOUS_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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
@RunWith(JUnitParamsRunner.class)
public class DescribeMappingCommandDUnitTest implements Serializable {

  private static final String TEST_REGION = "testRegion";

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public transient ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private MemberVM locator, server;

  private static String convertRegionPathToName(String regionPath) {
    if (regionPath.startsWith("/")) {
      return regionPath.substring(1);
    }
    return regionPath;
  }

  // TODO: need to add test for server group
  @After
  public void cleanUp() throws Exception {
    startupRule.stop(0);
    startupRule.stop(1);
    gfsh.disconnect();
  }

  @Test
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void describesExistingSynchronousMapping(String regionName) throws Exception {
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "testTable");
    csb.addOption(PDX_NAME, "myPdxClass");
    csb.addOption(SYNCHRONOUS_NAME, "true");
    csb.addOption(ID_NAME, "myId");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        regionName);

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsKeyValuePair(REGION_NAME,
        convertRegionPathToName(regionName));
    commandResultAssert.containsKeyValuePair(DATA_SOURCE_NAME, "connection");
    commandResultAssert.containsKeyValuePair(TABLE_NAME, "testTable");
    commandResultAssert.containsKeyValuePair(PDX_NAME, "myPdxClass");
    commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "true");
    commandResultAssert.containsKeyValuePair(ID_NAME, "myId");
  }

  @Test
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void describesExistingAsyncMapping(String regionName) throws Exception {
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);

    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "testTable");
    csb.addOption(PDX_NAME, "myPdxClass");
    csb.addOption(SYNCHRONOUS_NAME, "false");
    csb.addOption(ID_NAME, "myId");
    csb.addOption(CATALOG_NAME, "myCatalog");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        regionName);

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsKeyValuePair(REGION_NAME,
        convertRegionPathToName(regionName));

    commandResultAssert.containsKeyValuePair(DATA_SOURCE_NAME, "connection");
    commandResultAssert.containsKeyValuePair(TABLE_NAME, "testTable");
    commandResultAssert.containsKeyValuePair(PDX_NAME, "myPdxClass");
    commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "false");
    commandResultAssert.containsKeyValuePair(ID_NAME, "myId");
    commandResultAssert.containsKeyValuePair(CATALOG_NAME, "myCatalog");
    commandResultAssert.containsKeyValuePair(SCHEMA_NAME, "mySchema");
  }

  @Test
  public void reportsNoMappingFound() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + TEST_REGION + " --type=REPLICATE")
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_MAPPING)
        .addOption(REGION_NAME, "nonExisting");

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsError();
    commandResultAssert.containsOutput(
        String.format("(Experimental) \n" + "JDBC mapping for region 'nonExisting' not found"));
  }

  @Test
  public void reportConfigurationFoundOnMember() throws Exception {
    Properties properties = new Properties();
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");

    locator = startupRule.startLocatorVM(0, properties);
    server = startupRule.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + TEST_REGION + " --type=REPLICATE")
        .statusIsSuccess();

    server.invoke(() -> createRegionMapping());

    CommandResultAssert commandResultAssert =
        gfsh.executeAndAssertThat(DESCRIBE_MAPPING + " --region=" + TEST_REGION).statusIsSuccess();

    commandResultAssert.containsKeyValuePair(REGION_NAME, TEST_REGION);
    commandResultAssert.containsKeyValuePair(DATA_SOURCE_NAME, "connection");
    commandResultAssert.containsKeyValuePair(TABLE_NAME, "testTable");
    commandResultAssert.containsKeyValuePair(PDX_NAME, "myPdxClass");
    commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "false");
    commandResultAssert.containsKeyValuePair(ID_NAME, "myId");

  }

  private void createRegionMapping() throws RegionMappingExistsException {
    InternalCache cache = ClusterStartupRule.getCache();
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    service.createRegionMapping(new RegionMapping(TEST_REGION, "myPdxClass",
        "testTable", "connection", "myId", "myCatalog", "mySchema"));
    assertThat(service.getMappingForRegion(TEST_REGION)).isNotNull();
  }
}
