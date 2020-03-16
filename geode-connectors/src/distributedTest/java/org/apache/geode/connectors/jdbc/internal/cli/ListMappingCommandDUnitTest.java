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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import javax.sql.DataSource;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
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
  private MemberVM server1;
  private MemberVM server2;
  private MemberVM server3;
  private MemberVM server4;

  private String regionName = "testRegion";
  private static final String GROUP1_REGION = "group1Region";
  private static final String GROUP2_REGION = "group2Region";
  private static final String GROUP1_GROUP2_REGION = "group1Group2Region";
  private static final String TEST_GROUP1 = "testGroup1";
  private static final String TEST_GROUP2 = "testGroup2";


  private void createTable() {
    executeSql("create table mySchema.myTable (id varchar(10) primary key, name varchar(10))");
  }

  private void dropTable() {
    executeSql("drop table mySchema.myTable");
  }

  private void executeSql(String sql) {
    for (MemberVM server : Arrays.asList(server1, server2, server3, server4)) {
      if (server == null)
        continue;
      server.invoke(() -> {
        try {
          DataSource ds = JNDIInvoker.getDataSource("connection");
          Connection conn = ds.getConnection();
          Statement sm = conn.createStatement();
          sm.execute(sql);
          sm.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      });
    }
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
      writer.writeString("id", id);
      writer.writeString("name", name);
    }

    @Override
    public void fromData(PdxReader reader) {
      id = reader.readString("id");
      name = reader.readString("name");
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void listsRegionMapping() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server1 = startupRule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat(
        "create data-source --name=connection --url=\"jdbc:derby:memory:newDB;create=true\"")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();
    createTable();
    try {
      String mapping = "create jdbc-mapping --region=" + regionName + " --data-source=connection "
          + "--table=myTable --pdx-name="
          + IdAndName.class.getName() + " --schema=mySchema";
      gfsh.executeAndAssertThat(mapping).statusIsSuccess();

      CommandStringBuilder csb = new CommandStringBuilder(LIST_MAPPING);
      CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

      commandResultAssert.statusIsSuccess();
      commandResultAssert.tableHasRowCount(1);
      commandResultAssert.tableHasColumnOnlyWithValues(LIST_OF_MAPPINGS, regionName);
    } finally {
      dropTable();
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void reportsNoRegionMappingsFoundForServerGroup() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server1 = startupRule.startServerVM(1, TEST_GROUP1, locator.getPort());
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat(
        "create data-source --name=connection --url=\"jdbc:derby:memory:newDB;create=true\"")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();
    createTable();
    try {
      String mapping = "create jdbc-mapping --region=" + regionName + " --data-source=connection "
          + "--table=myTable --schema=mySchema --pdx-name=" + IdAndName.class.getName();
      gfsh.executeAndAssertThat(mapping).statusIsSuccess();

      CommandStringBuilder csb =
          new CommandStringBuilder(LIST_MAPPING + " --groups=" + TEST_GROUP1);
      CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

      commandResultAssert.statusIsError().hasNoTableSection();

      csb = new CommandStringBuilder(LIST_MAPPING);
      commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

      commandResultAssert.statusIsSuccess();
      commandResultAssert.tableHasRowCount(1);
      commandResultAssert.tableHasColumnOnlyWithValues(LIST_OF_MAPPINGS, regionName);
    } finally {
      dropTable();
    }
  }


  @SuppressWarnings("deprecation")
  @Test
  public void listsRegionMappingForServerGroup() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server1 = startupRule.startServerVM(1, TEST_GROUP1, locator.getPort());
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat(
        "create data-source --name=connection --url=\"jdbc:derby:memory:newDB;create=true\"")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --groups=" + TEST_GROUP1 + " --type=REPLICATE")
        .statusIsSuccess();
    createTable();
    try {
      String mapping =
          "create jdbc-mapping --region=" + regionName + " --groups=" + TEST_GROUP1
              + " --data-source=connection " + "--table=myTable --schema=mySchema --pdx-name="
              + IdAndName.class.getName();
      gfsh.executeAndAssertThat(mapping).statusIsSuccess();

      CommandStringBuilder csb =
          new CommandStringBuilder(LIST_MAPPING + " --groups=" + TEST_GROUP1);
      CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

      commandResultAssert.statusIsSuccess();
      commandResultAssert.tableHasRowCount(1);
      commandResultAssert.tableHasColumnOnlyWithValues(LIST_OF_MAPPINGS, regionName);

      csb = new CommandStringBuilder(LIST_MAPPING);
      commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
      commandResultAssert.statusIsSuccess().hasNoTableSection();
    } finally {
      dropTable();
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void listsRegionMappingForMultiServerGroup() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server1 = startupRule.startServerVM(1, locator.getPort());
    server2 = startupRule.startServerVM(2, TEST_GROUP1, locator.getPort());
    server3 = startupRule.startServerVM(3, TEST_GROUP2, locator.getPort());
    server4 = startupRule.startServerVM(4, TEST_GROUP1 + "," + TEST_GROUP2, locator.getPort());

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat(
        "create data-source --name=connection --url=\"jdbc:derby:memory:newDB;create=true\"")
        .statusIsSuccess();
    // create 4 regions
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=" + GROUP1_REGION + " --groups=" + TEST_GROUP1 + " --type=REPLICATE")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=" + GROUP2_REGION + " --groups=" + TEST_GROUP2 + " --type=REPLICATE")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=" + GROUP1_GROUP2_REGION + " --groups=" + TEST_GROUP1 + ","
            + TEST_GROUP2 + " --type=REPLICATE")
        .statusIsSuccess();

    createTable();
    try {
      // create 4 mappings
      String mapping =
          "create jdbc-mapping --region=" + regionName + " --data-source=connection "
              + "--table=myTable --schema=mySchema --pdx-name=" + IdAndName.class.getName();
      gfsh.executeAndAssertThat(mapping).statusIsSuccess();
      mapping =
          "create jdbc-mapping --region=" + GROUP1_REGION + " --groups=" + TEST_GROUP1
              + " --data-source=connection " + "--table=myTable --schema=mySchema --pdx-name="
              + IdAndName.class.getName();
      gfsh.executeAndAssertThat(mapping).statusIsSuccess();
      mapping =
          "create jdbc-mapping --region=" + GROUP2_REGION + " --groups=" + TEST_GROUP2
              + " --data-source=connection " + "--table=myTable --schema=mySchema --pdx-name="
              + IdAndName.class.getName();
      gfsh.executeAndAssertThat(mapping).statusIsSuccess();
      mapping =
          "create jdbc-mapping --region=" + GROUP1_GROUP2_REGION + " --groups=" + TEST_GROUP1 + ","
              + TEST_GROUP2 + " --data-source=connection "
              + "--table=myTable --schema=mySchema --pdx-name=" + IdAndName.class.getName();
      gfsh.executeAndAssertThat(mapping).statusIsSuccess();

      {
        CommandStringBuilder csb =
            new CommandStringBuilder(LIST_MAPPING);
        CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

        commandResultAssert.statusIsSuccess();
        commandResultAssert.tableHasRowCount(1);
        commandResultAssert.tableHasColumnOnlyWithValues(LIST_OF_MAPPINGS, regionName);
      }

      {
        CommandStringBuilder csb =
            new CommandStringBuilder(LIST_MAPPING + " --groups=" + TEST_GROUP1);
        CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

        commandResultAssert.statusIsSuccess();
        commandResultAssert.tableHasRowCount(2);
        commandResultAssert
            .tableHasColumnOnlyWithValues(LIST_OF_MAPPINGS, GROUP1_REGION, GROUP1_GROUP2_REGION);
      }

      {
        CommandStringBuilder csb =
            new CommandStringBuilder(LIST_MAPPING + " --groups=" + TEST_GROUP2);
        CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

        commandResultAssert.statusIsSuccess();
        commandResultAssert.tableHasRowCount(2);
        commandResultAssert
            .tableHasColumnOnlyWithValues(LIST_OF_MAPPINGS, GROUP2_REGION, GROUP1_GROUP2_REGION);
      }

      {
        CommandStringBuilder csb =
            new CommandStringBuilder(LIST_MAPPING + " --groups=" + TEST_GROUP1 + "," + TEST_GROUP2);
        CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
        commandResultAssert.statusIsSuccess();
        // There will be 4 items: testRegion1 for testGroup1, testRegion2 for testGroup2,
        // group1Group2Region for testGroup1, group1Group2Region for testGroup2
        commandResultAssert.tableHasRowCount(4);
        commandResultAssert
            .tableHasColumnOnlyWithValues(LIST_OF_MAPPINGS, GROUP1_REGION, GROUP2_REGION,
                GROUP1_GROUP2_REGION);
      }
    } finally {
      dropTable();
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void reportsNoRegionMappingsFound() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server1 = startupRule.startServerVM(1, TEST_GROUP1, locator.getPort());
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat(
        "create data-source --name=connection --url=\"jdbc:derby:memory:newDB;create=true\"")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --groups=" + TEST_GROUP1 + " --type=REPLICATE")
        .statusIsSuccess();
    createTable();
    try {
      String mapping =
          "create jdbc-mapping --region=" + regionName + " --groups=" + TEST_GROUP1
              + " --data-source=connection --schema=mySchema --table=myTable --pdx-name="
              + IdAndName.class.getName();
      gfsh.executeAndAssertThat(mapping).statusIsSuccess();

      CommandStringBuilder csb =
          new CommandStringBuilder(LIST_MAPPING + " --groups=" + TEST_GROUP1);
      CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

      commandResultAssert.statusIsSuccess();
      commandResultAssert.tableHasRowCount(1);
      commandResultAssert.tableHasColumnOnlyWithValues(LIST_OF_MAPPINGS, regionName);

      csb = new CommandStringBuilder(LIST_MAPPING);
      commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
      commandResultAssert.statusIsSuccess().hasNoTableSection();
    } finally {
      dropTable();
    }
  }

  @Test
  public void testDestroyRegionFailsWithExistingJdbcMapping() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server1 = startupRule.startServerVM(1, TEST_GROUP1, locator.getPort());
    server2 = startupRule.startServerVM(2, TEST_GROUP2, locator.getPort());

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat(
        "create data-source --name=connection --url=\"jdbc:derby:memory:newDB;create=true\"")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=" + GROUP1_REGION + " --groups=" + TEST_GROUP1 + " --type=REPLICATE")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=" + GROUP2_REGION + " --groups=" + TEST_GROUP2 + " --type=REPLICATE")
        .statusIsSuccess();
    createTable();
    try {
      String mapping =
          "create jdbc-mapping --region=" + GROUP1_REGION + " --groups=" + TEST_GROUP1
              + " --data-source=connection --schema=mySchema --table=myTable --pdx-name="
              + IdAndName.class.getName();
      gfsh.executeAndAssertThat(mapping).statusIsSuccess();

      mapping =
          "create jdbc-mapping --region=" + GROUP2_REGION + " --groups=" + TEST_GROUP2
              + " --data-source=connection --schema=mySchema --table=myTable --pdx-name="
              + IdAndName.class.getName();
      gfsh.executeAndAssertThat(mapping).statusIsSuccess();

      CommandStringBuilder csb = new CommandStringBuilder("destroy region --name=" + GROUP1_REGION);
      gfsh.executeAndAssertThat(csb.toString()).statusIsError()
          .containsOutput("Cannot destroy region \"" + GROUP1_REGION
              + "\" because JDBC mapping exists. Use \"destroy jdbc-mapping\" first.");

      csb = new CommandStringBuilder("destroy region --name=" + GROUP2_REGION);
      gfsh.executeAndAssertThat(csb.toString()).statusIsError()
          .containsOutput("Cannot destroy region \"" + GROUP2_REGION
              + "\" because JDBC mapping exists. Use \"destroy jdbc-mapping\" first.");
    } finally {
      dropTable();
    }
  }
}
