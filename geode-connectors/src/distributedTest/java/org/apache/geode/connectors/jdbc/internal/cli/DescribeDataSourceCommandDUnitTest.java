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

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class DescribeDataSourceCommandDUnitTest {

  private MemberVM server;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void before() throws Exception {
    MemberVM locator = cluster.startLocatorVM(0);
    server = cluster.startServerVM(1, new Properties(), locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void describeDataSourceForSimpleDataSource() {
    gfsh.executeAndAssertThat(
        "create data-source --name=simple --pooled=false --url=\"jdbc:derby:memory:newDB;create=true\" --username=joe --password=myPassword")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");

    CommandResultAssert result = gfsh.executeAndAssertThat("describe data-source --name=simple");

    result.statusIsSuccess()
        .tableHasRowWithValues("Property", "Value", "name", "simple")
        .tableHasRowWithValues("Property", "Value", "pooled", "false")
        .tableHasRowWithValues("Property", "Value", "username", "joe")
        .tableHasRowWithValues("Property", "Value", "url", "jdbc:derby:memory:newDB;create=true");
    assertThat(result.getResultModel().toString()).doesNotContain("myPassword");
  }

  private void setupDatabase() {
    executeSql("create table mySchema.region1 (myId varchar(10) primary key, name varchar(10))");
    executeSql("create table mySchema.region2 (myId varchar(10) primary key, name varchar(10))");
  }

  private void teardownDatabase() {
    executeSql("drop table mySchema.region1");
    executeSql("drop table mySchema.region2");
  }

  private void executeSql(String sql) {
    server.invoke(() -> {
      try {
        DataSource ds = JNDIInvoker.getDataSource("pool");
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
      writer.writeString("myId", id);
      writer.writeString("name", name);
    }

    @Override
    public void fromData(PdxReader reader) {
      id = reader.readString("myId");
      name = reader.readString("name");
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void describeDataSourceUsedByRegionsListsTheRegionsInOutput() {
    gfsh.executeAndAssertThat(
        "create data-source --name=pool --url=\"jdbc:derby:memory:newDB;create=true\"")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");
    gfsh.executeAndAssertThat("create region --name=region1 --type=REPLICATE").statusIsSuccess();
    gfsh.executeAndAssertThat("create region --name=region2 --type=PARTITION").statusIsSuccess();
    setupDatabase();
    try {
      gfsh.executeAndAssertThat(
          "create jdbc-mapping --region=region1 --data-source=pool --pdx-name="
              + IdAndName.class.getName() + " --schema=mySchema");
      gfsh.executeAndAssertThat(
          "create jdbc-mapping --region=region2 --data-source=pool --pdx-name="
              + IdAndName.class.getName() + " --schema=mySchema");

      CommandResultAssert result = gfsh.executeAndAssertThat("describe data-source --name=pool");

      result.statusIsSuccess()
          .tableHasRowWithValues("Property", "Value", "name", "pool")
          .tableHasRowWithValues("Property", "Value", "pooled", "true")
          .tableHasRowWithValues("Property", "Value", "url", "jdbc:derby:memory:newDB;create=true");
      InfoResultModel infoSection = result.getResultModel()
          .getInfoSection(DescribeDataSourceCommand.REGIONS_USING_DATA_SOURCE_SECTION);
      assertThat(new HashSet<>(infoSection.getContent()))
          .isEqualTo(new HashSet<>(Arrays.asList("region1", "region2")));
    } finally {
      teardownDatabase();
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void describeDataSourceForPooledDataSource() {
    gfsh.executeAndAssertThat(
        "create data-source --name=pooled --pooled --url=\"jdbc:derby:memory:newDB;create=true\" --pooled-data-source-factory-class=org.apache.geode.internal.jta.CacheJTAPooledDataSourceFactory --pool-properties={'name':'prop1','value':'value1'},{'name':'pool.prop2','value':'value2'}")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");

    gfsh.executeAndAssertThat("describe data-source --name=pooled").statusIsSuccess()
        .tableHasRowWithValues("Property", "Value", "name", "pooled")
        .tableHasRowWithValues("Property", "Value", "pooled", "true")
        .tableHasRowWithValues("Property", "Value", "username", "")
        .tableHasRowWithValues("Property", "Value", "url", "jdbc:derby:memory:newDB;create=true")
        .tableHasRowWithValues("Property", "Value", "pooled-data-source-factory-class",
            "org.apache.geode.internal.jta.CacheJTAPooledDataSourceFactory")
        .tableHasRowWithValues("Property", "Value", "prop1", "value1")
        .tableHasRowWithValues("Property", "Value", "pool.prop2", "value2");
  }

  @Test
  public void describeDataSourceDoesNotExist() {
    gfsh.executeAndAssertThat("describe data-source --name=unknown").statusIsError()
        .containsOutput("Data source: unknown not found");
  }
}
