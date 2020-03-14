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
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.DATA_SOURCE_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.GROUP_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.ID_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.PDX_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.REGION_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.SCHEMA_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.SYNCHRONOUS_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.TABLE_NAME;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.JDBCConnectorTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({JDBCConnectorTest.class})
public class DescribeMappingCommandDUnitTest implements Serializable {

  private static final String TEST_REGION = "testRegion";

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public transient ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private MemberVM locator;
  private MemberVM server;
  private MemberVM server2;

  private static String convertRegionPathToName(String regionPath) {
    if (regionPath.startsWith("/")) {
      return regionPath.substring(1);
    }
    return regionPath;
  }

  private boolean setupDatabase;

  private void setupDatabase() {
    setupDatabase = true;
    gfsh.executeAndAssertThat(
        "create data-source --name=connection"
            + " --pooled=false"
            + " --url=\"jdbc:derby:memory:newDB;create=true\"")
        .statusIsSuccess();
    executeSql(server,
        "create table mySchema.testTable (myId varchar(10) primary key, name varchar(10))");
  }

  @After
  public void after() {
    teardownDatabase();
  }

  private void teardownDatabase() {
    if (setupDatabase) {
      setupDatabase = false;
      executeSql(server, "drop table mySchema.testTable");
    }
  }

  private void executeSql(MemberVM targetMember, String sql) {
    targetMember.invoke(() -> {
      try {
        DataSource ds = JNDIInvoker.getDataSource("connection");
        Connection conn = ds.getConnection();
        Statement sm = conn.createStatement();
        sm.execute(sql);
        sm.close();
        conn.close();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public static class IdAndName implements PdxSerializable {
    private String id;
    private String name;

    public IdAndName() {
      // nothing
    }

    IdAndName(String id, String name) {
      this.id = id;
      this.name = name;
    }

    String getId() {
      return id;
    }

    String getName() {
      return name;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("myid", id);
      writer.writeString("name", name);
    }

    @Override
    public void fromData(PdxReader reader) {
      id = reader.readString("myid");
      name = reader.readString("name");
    }
  }

  public static class IdAndName2 implements PdxSerializable {
    private String id;
    private String name;

    public IdAndName2() {
      // nothing
    }

    IdAndName2(String id, String name) {
      this.id = id;
      this.name = name;
    }

    String getId() {
      return id;
    }

    String getName() {
      return name;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("myid2", id);
      writer.writeString("name", name);
    }

    @Override
    public void fromData(PdxReader reader) {
      id = reader.readString("myid2");
      name = reader.readString("name");
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void describesExistingSynchronousMapping() throws Exception {
    String regionName = "/" + TEST_REGION;
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);
    setupDatabase();
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(TABLE_NAME, "testTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
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
    commandResultAssert.containsKeyValuePair(PDX_NAME, IdAndName.class.getName());
    commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "true");
    commandResultAssert.containsKeyValuePair(ID_NAME, "myId");
  }

  @SuppressWarnings("deprecation")
  @Test
  public void describesExistingSynchronousMappingWithGroups() throws Exception {
    String regionName = TEST_REGION;
    String groupName = "group1";
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, groupName, locator.getPort());

    gfsh.connectAndVerify(locator);
    setupDatabase();
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --type=REPLICATE --group=" + groupName)
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(GROUP_NAME, groupName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(SCHEMA_NAME, "mySchema");
    csb.addOption(TABLE_NAME, "testTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
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
    commandResultAssert.containsKeyValuePair(PDX_NAME, IdAndName.class.getName());
    commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "true");
    commandResultAssert.containsKeyValuePair(ID_NAME, "myId");
  }

  @SuppressWarnings("deprecation")
  @Test
  public void describesExistingAsyncMapping() throws Exception {
    String regionName = "/" + TEST_REGION;
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);
    setupDatabase();
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);

    csb.addOption(REGION_NAME, regionName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "testTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(SYNCHRONOUS_NAME, "false");
    csb.addOption(ID_NAME, "myId");
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
    commandResultAssert.containsKeyValuePair(PDX_NAME, IdAndName.class.getName());
    commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "false");
    commandResultAssert.containsKeyValuePair(ID_NAME, "myId");
    commandResultAssert.containsKeyValuePair(SCHEMA_NAME, "mySchema");
  }

  @SuppressWarnings("deprecation")
  @Test
  public void describesExistingAsyncMappingWithGroup() throws Exception {
    String regionName = TEST_REGION;
    String groupName = "group1";
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, groupName, locator.getPort());

    gfsh.connectAndVerify(locator);
    setupDatabase();
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --type=REPLICATE --group=" + groupName)
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);

    csb.addOption(REGION_NAME, regionName);
    csb.addOption(GROUP_NAME, groupName);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "testTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(SYNCHRONOUS_NAME, "false");
    csb.addOption(ID_NAME, "myId");
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
    commandResultAssert.containsKeyValuePair(PDX_NAME, IdAndName.class.getName());
    commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "false");
    commandResultAssert.containsKeyValuePair(ID_NAME, "myId");
    commandResultAssert.containsKeyValuePair(SCHEMA_NAME, "mySchema");
  }

  @SuppressWarnings({"DuplicatedCode", "deprecation"})
  @Test
  public void describesExistingAsyncMappingsWithSameRegionOnDifferentGroups()
      throws Exception {
    String regionName = "/" + TEST_REGION;
    String groupName1 = "group1";
    String groupName2 = "group2";
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, groupName1, locator.getPort());
    server2 = startupRule.startServerVM(2, groupName2, locator.getPort());

    gfsh.connectAndVerify(locator);
    setupDatabase();
    executeSql(server2,
        "create table mySchema.testTable (myId varchar(10) primary key, name varchar(10))");
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE --group="
        + groupName1 + "," + groupName2)
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);

    csb.addOption(REGION_NAME, regionName);
    csb.addOption(GROUP_NAME, groupName1 + "," + groupName2);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "testTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(SYNCHRONOUS_NAME, "false");
    csb.addOption(ID_NAME, "myId");
    csb.addOption(SCHEMA_NAME, "mySchema");

    try {
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
      commandResultAssert.containsKeyValuePair(PDX_NAME, IdAndName.class.getName());
      commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "false");
      commandResultAssert.containsKeyValuePair(ID_NAME, "myId");
      commandResultAssert.containsKeyValuePair(SCHEMA_NAME, "mySchema");
    } finally {
      executeSql(server2, "drop table mySchema.testTable");
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void describesExistingAsyncMappingsWithSameRegionOnDifferentGroupsWithDifferentMappings()
      throws Exception {
    String regionName = TEST_REGION;
    String groupName1 = "group1";
    String groupName2 = "group2";
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, groupName1, locator.getPort());
    server2 = startupRule.startServerVM(2, groupName2, locator.getPort());

    gfsh.connectAndVerify(locator);
    setupDatabase();
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE --group="
        + groupName1 + "," + groupName2)
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);

    csb.addOption(REGION_NAME, regionName);
    csb.addOption(GROUP_NAME, groupName1);
    csb.addOption(DATA_SOURCE_NAME, "connection");
    csb.addOption(TABLE_NAME, "testTable");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(SYNCHRONOUS_NAME, "false");
    csb.addOption(ID_NAME, "myId");
    csb.addOption(SCHEMA_NAME, "mySchema");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    gfsh.executeAndAssertThat(
        "create data-source --name=connection2"
            + " --pooled=false"
            + " --url=\"jdbc:derby:memory:newDB;create=true\"")
        .statusIsSuccess();
    executeSql(server2,
        "create table mySchema2.testTable2 (myId2 varchar(10) primary key, name varchar(10))");
    try {
      csb = new CommandStringBuilder(CREATE_MAPPING);

      csb.addOption(REGION_NAME, regionName);
      csb.addOption(GROUP_NAME, groupName2);
      csb.addOption(DATA_SOURCE_NAME, "connection2");
      csb.addOption(TABLE_NAME, "testTable2");
      csb.addOption(PDX_NAME, IdAndName2.class.getName());
      csb.addOption(SYNCHRONOUS_NAME, "false");
      csb.addOption(ID_NAME, "myId2");
      csb.addOption(SCHEMA_NAME, "mySchema2");

      gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
    } finally {
      executeSql(server2, "drop table mySchema2.testTable2");
    }

    csb = new CommandStringBuilder(DESCRIBE_MAPPING).addOption(REGION_NAME,
        regionName).addOption(GROUP_NAME, groupName1);

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsKeyValuePair(REGION_NAME,
        convertRegionPathToName(regionName));

    commandResultAssert.containsKeyValuePair("Mapping for group", "group1");
    commandResultAssert.containsKeyValuePair(DATA_SOURCE_NAME, "connection");
    commandResultAssert.containsKeyValuePair(TABLE_NAME, "testTable");
    commandResultAssert.containsKeyValuePair(PDX_NAME, IdAndName.class.getName());
    commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "false");
    commandResultAssert.containsKeyValuePair(ID_NAME, "myId");
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
    commandResultAssert.containsKeyValuePair(PDX_NAME, IdAndName2.class.getName());
    commandResultAssert.containsKeyValuePair(SYNCHRONOUS_NAME, "false");
    commandResultAssert.containsKeyValuePair(ID_NAME, "myId2");
    commandResultAssert.containsKeyValuePair(SCHEMA_NAME, "mySchema2");
  }

  @Test
  public void reportsNoRegionFound() throws Exception {
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
        "A region named nonExisting must already exist.");
  }

  @Test
  public void reportsRegionButNoMappingFound() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + TEST_REGION + " --type=REPLICATE")
        .statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_MAPPING)
        .addOption(REGION_NAME, TEST_REGION);

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsError();
    commandResultAssert.containsOutput(
        "JDBC mapping for region '" + TEST_REGION + "' not found");
  }
}
