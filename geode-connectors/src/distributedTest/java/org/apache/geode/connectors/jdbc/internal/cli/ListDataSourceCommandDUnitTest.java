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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class ListDataSourceCommandDUnitTest {

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
  public void listDataSourceForSimpleDataSource() {
    gfsh.executeAndAssertThat(
        "create data-source --name=simple --url=\"jdbc:derby:memory:newDB;create=true\" --username=joe --password=myPassword --pooled=false ")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");

    CommandResultAssert result = gfsh.executeAndAssertThat("list data-source");

    result.statusIsSuccess()
        .tableHasRowWithValues("name", "pooled", "in use", "url", "simple", "false", "false",
            "jdbc:derby:memory:newDB;create=true");
  }


  private void setupDatabase() {
    executeSql("create table region1 (id varchar(10) primary key, name varchar(10))");
    executeSql("create table region2 (id varchar(10) primary key, name varchar(10))");
  }

  private void teardownDatabase() {
    executeSql("drop table region1");
    executeSql("drop table region2");
  }

  private void executeSql(String sql) {
    server.invoke(() -> {
      try {
        DataSource ds = JNDIInvoker.getDataSource("simple");
        Connection conn = ds.getConnection();
        Statement sm = conn.createStatement();
        sm.execute(sql);
        sm.close();
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
  public void listDataSourceUsedByRegionsHasCorrectOutput() {
    gfsh.executeAndAssertThat(
        "create data-source --name=simple --url=\"jdbc:derby:memory:newDB;create=true\" --pooled=false")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");
    setupDatabase();
    gfsh.executeAndAssertThat("create region --name=region1 --type=REPLICATE").statusIsSuccess();
    gfsh.executeAndAssertThat("create region --name=region2 --type=PARTITION").statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create jdbc-mapping --region=region1 --data-source=simple --pdx-name="
            + IdAndName.class.getName() + " --schema=app")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create jdbc-mapping --region=region2 --data-source=simple --pdx-name="
            + IdAndName.class.getName() + " --schema=app")
        .statusIsSuccess();

    CommandResultAssert result = gfsh.executeAndAssertThat("list data-source");

    teardownDatabase();
    result.statusIsSuccess()
        .tableHasRowWithValues("name", "pooled", "in use", "url", "simple", "false", "true",
            "jdbc:derby:memory:newDB;create=true");
  }

  @SuppressWarnings("deprecation")
  @Test
  public void listDataSourceForPooledDataSource() {
    gfsh.executeAndAssertThat(
        "create data-source --name=pooledDataSource --pooled --url=\"jdbc:derby:memory:newDB;create=true\" --pooled-data-source-factory-class=org.apache.geode.internal.jta.CacheJTAPooledDataSourceFactory --pool-properties={'name':'prop1','value':'value1'},{'name':'pool.prop2','value':'value2'}")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");

    gfsh.executeAndAssertThat("list data-source").statusIsSuccess()
        .tableHasRowWithValues("name", "pooled", "in use", "url", "pooledDataSource", "true",
            "false",
            "jdbc:derby:memory:newDB;create=true");
  }

  @SuppressWarnings("deprecation")
  @Test
  public void listDataSourceForPooledDataSourceByDefault() {
    gfsh.executeAndAssertThat(
        "create data-source --name=pooledDataSource --url=\"jdbc:derby:memory:newDB;create=true\" --pooled-data-source-factory-class=org.apache.geode.internal.jta.CacheJTAPooledDataSourceFactory --pool-properties={'name':'prop1','value':'value1'},{'name':'pool.prop2','value':'value2'}")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");

    gfsh.executeAndAssertThat("list data-source").statusIsSuccess()
        .tableHasRowWithValues("name", "pooled", "in use", "url", "pooledDataSource", "true",
            "false",
            "jdbc:derby:memory:newDB;create=true");
  }

  @SuppressWarnings("deprecation")
  @Test
  public void listDataSourceWithMultipleDataSourcesListsAll() {
    gfsh.executeAndAssertThat(
        "create data-source --name=simple --pooled=false --url=\"jdbc:derby:memory:newDB;create=true\" --username=joe --password=myPassword")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");
    gfsh.executeAndAssertThat(
        "create data-source --name=pooledDataSource --pooled --url=\"jdbc:derby:memory:newDB;create=true\" --pooled-data-source-factory-class=org.apache.geode.internal.jta.CacheJTAPooledDataSourceFactory --pool-properties={'name':'prop1','value':'value1'},{'name':'pool.prop2','value':'value2'}")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");

    gfsh.executeAndAssertThat("list data-source").statusIsSuccess()
        .tableHasRowWithValues("name", "pooled", "in use", "url", "pooledDataSource", "true",
            "false",
            "jdbc:derby:memory:newDB;create=true")
        .tableHasRowWithValues("name", "pooled", "in use", "url", "simple", "false", "false",
            "jdbc:derby:memory:newDB;create=true");
  }

  @Test
  public void listDataSourceWithNoDataSources() {
    gfsh.executeAndAssertThat("list data-source").statusIsSuccess()
        .containsOutput("No data sources found");
  }
}
