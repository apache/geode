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
import static org.apache.geode.connectors.util.internal.MappingConstants.GROUP_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.ID_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.PDX_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.REGION_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.SCHEMA_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.SYNCHRONOUS_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.TABLE_NAME;

import java.io.Serializable;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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

  private MemberVM locator;

  private static String convertRegionPathToName(String regionPath) {
    if (regionPath.startsWith("/")) {
      return regionPath.substring(1);
    }
    return regionPath;
  }

  @Test
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void describesExistingSynchronousMapping(String regionName) throws Exception {
    locator = startupRule.startLocatorVM(0);
    startupRule.startServerVM(1, locator.getPort());

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
    commandResultAssert.doesNotContainOutput("Mapping for group");
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
  public void describesExistingSynchronousMappingWithGroups(String regionName) throws Exception {
    String groupName = "group1";
    locator = startupRule.startLocatorVM(0);
    startupRule.startServerVM(1, groupName, locator.getPort());

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --type=REPLICATE --group=" + groupName)
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(GROUP_NAME, groupName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "testTable");
    csb.addOption(PDX_NAME, "myPdxClass");
    csb.addOption(SYNCHRONOUS_NAME, "true");
    csb.addOption(ID_NAME, "myId");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        regionName).addOption(GROUP_NAME, groupName);

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsKeyValuePair("Mapping for group", "group1");
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
    startupRule.startServerVM(1, locator.getPort());

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
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void describesExistingAsyncMappingWithGroup(String regionName) throws Exception {
    String groupName = "group1";
    locator = startupRule.startLocatorVM(0);
    startupRule.startServerVM(1, groupName, locator.getPort());

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --type=REPLICATE --group=" + groupName)
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);

    csb.addOption(REGION_NAME, regionName);
    csb.addOption(GROUP_NAME, groupName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "testTable");
    csb.addOption(PDX_NAME, "myPdxClass");
    csb.addOption(SYNCHRONOUS_NAME, "false");
    csb.addOption(ID_NAME, "myId");
    csb.addOption(CATALOG_NAME, "myCatalog");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        regionName).addOption(GROUP_NAME, groupName);

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsKeyValuePair(REGION_NAME,
        convertRegionPathToName(regionName));

    commandResultAssert.containsKeyValuePair("Mapping for group", "group1");
    commandResultAssert.containsKeyValuePair(DATA_SOURCE_NAME, "connection");
    commandResultAssert.containsKeyValuePair(TABLE_NAME, "testTable");
    commandResultAssert.containsKeyValuePair(PDX_NAME, "myPdxClass");
    commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "false");
    commandResultAssert.containsKeyValuePair(ID_NAME, "myId");
    commandResultAssert.containsKeyValuePair(CATALOG_NAME, "myCatalog");
    commandResultAssert.containsKeyValuePair(SCHEMA_NAME, "mySchema");
  }

  @Test
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void describesExistingAsyncMappingsWithSameRegionOnDifferentGroups(String regionName)
      throws Exception {
    String groupName1 = "group1";
    String groupName2 = "group2";
    locator = startupRule.startLocatorVM(0);
    startupRule.startServerVM(1, groupName1, locator.getPort());
    startupRule.startServerVM(2, groupName2, locator.getPort());

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE --group="
        + groupName1 + "," + groupName2)
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);

    csb.addOption(REGION_NAME, regionName);
    csb.addOption(GROUP_NAME, groupName1 + "," + groupName2);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "testTable");
    csb.addOption(PDX_NAME, "myPdxClass");
    csb.addOption(SYNCHRONOUS_NAME, "false");
    csb.addOption(ID_NAME, "myId");
    csb.addOption(CATALOG_NAME, "myCatalog");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        regionName).addOption(GROUP_NAME, groupName1 + "," + groupName2);

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsKeyValuePair(REGION_NAME,
        convertRegionPathToName(regionName));

    commandResultAssert.containsKeyValuePair("Mapping for group", "group1");
    commandResultAssert.containsKeyValuePair("Mapping for group", "group2");
    commandResultAssert.containsKeyValuePair(DATA_SOURCE_NAME, "connection");
    commandResultAssert.containsKeyValuePair(TABLE_NAME, "testTable");
    commandResultAssert.containsKeyValuePair(PDX_NAME, "myPdxClass");
    commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "false");
    commandResultAssert.containsKeyValuePair(ID_NAME, "myId");
    commandResultAssert.containsKeyValuePair(CATALOG_NAME, "myCatalog");
    commandResultAssert.containsKeyValuePair(SCHEMA_NAME, "mySchema");
  }

  @Test
  @Parameters({TEST_REGION, "/" + TEST_REGION})
  public void describesExistingAsyncMappingsWithSameRegionOnDifferentGroupsWithDifferentMappings(
      String regionName)
      throws Exception {
    String groupName1 = "group1";
    String groupName2 = "group2";
    locator = startupRule.startLocatorVM(0);
    startupRule.startServerVM(1, groupName1, locator.getPort());
    startupRule.startServerVM(2, groupName2, locator.getPort());

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE --group="
        + groupName1 + "," + groupName2)
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);

    csb.addOption(REGION_NAME, regionName);
    csb.addOption(GROUP_NAME, groupName1);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "testTable");
    csb.addOption(PDX_NAME, "myPdxClass");
    csb.addOption(SYNCHRONOUS_NAME, "false");
    csb.addOption(ID_NAME, "myId");
    csb.addOption(CATALOG_NAME, "myCatalog");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CREATE_MAPPING);

    csb.addOption(REGION_NAME, regionName);
    csb.addOption(GROUP_NAME, groupName2);
    csb.addOption(DATA_SOURCE_NAME, "connection2");
    csb.addOption(TABLE_NAME, "testTable2");
    csb.addOption(PDX_NAME, "myPdxClass2");
    csb.addOption(SYNCHRONOUS_NAME, "false");
    csb.addOption(ID_NAME, "myId2");
    csb.addOption(CATALOG_NAME, "myCatalog2");
    csb.addOption(SCHEMA_NAME, "mySchema2");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        regionName).addOption(GROUP_NAME, groupName1);

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsKeyValuePair(REGION_NAME,
        convertRegionPathToName(regionName));

    commandResultAssert.containsKeyValuePair("Mapping for group", "group1");
    commandResultAssert.containsKeyValuePair(DATA_SOURCE_NAME, "connection");
    commandResultAssert.containsKeyValuePair(TABLE_NAME, "testTable");
    commandResultAssert.containsKeyValuePair(PDX_NAME, "myPdxClass");
    commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "false");
    commandResultAssert.containsKeyValuePair(ID_NAME, "myId");
    commandResultAssert.containsKeyValuePair(CATALOG_NAME, "myCatalog");
    commandResultAssert.containsKeyValuePair(SCHEMA_NAME, "mySchema");

    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        regionName).addOption(GROUP_NAME, groupName2);

    commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsKeyValuePair(REGION_NAME,
        convertRegionPathToName(regionName));

    commandResultAssert.containsKeyValuePair("Mapping for group", "group2");
    commandResultAssert.containsKeyValuePair(DATA_SOURCE_NAME, "connection2");
    commandResultAssert.containsKeyValuePair(TABLE_NAME, "testTable2");
    commandResultAssert.containsKeyValuePair(PDX_NAME, "myPdxClass2");
    commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "false");
    commandResultAssert.containsKeyValuePair(ID_NAME, "myId2");
    commandResultAssert.containsKeyValuePair(CATALOG_NAME, "myCatalog2");
    commandResultAssert.containsKeyValuePair(SCHEMA_NAME, "mySchema2");
  }

  @Test
  public void reportsNoRegionFound() throws Exception {
    locator = startupRule.startLocatorVM(0);
    startupRule.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + TEST_REGION + " --type=REPLICATE")
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_MAPPING)
        .addOption(REGION_NAME, "nonExisting");

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsError();
    commandResultAssert.containsOutput(
        String.format("A region named nonExisting must already exist."));
  }

  @Test
  public void reportsRegionButNoMappingFound() throws Exception {
    locator = startupRule.startLocatorVM(0);
    startupRule.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + TEST_REGION + " --type=REPLICATE")
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_MAPPING)
        .addOption(REGION_NAME, TEST_REGION);

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsError();
    commandResultAssert.containsOutput(
        String.format("JDBC mapping for region '" + TEST_REGION + "' not found"));
  }
}
