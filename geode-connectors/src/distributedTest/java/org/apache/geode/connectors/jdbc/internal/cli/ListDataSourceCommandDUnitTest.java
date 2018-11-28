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

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class ListDataSourceCommandDUnitTest {

  private MemberVM locator, server;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void before() throws Exception {
    locator = cluster.startLocatorVM(0);
    server = cluster.startServerVM(1, new Properties(), locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void listDataSourceForSimpleDataSource() {
    gfsh.executeAndAssertThat(
        "create data-source --name=simple --url=\"jdbc:derby:newDB;create=true\" --username=joe --password=myPassword")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");

    CommandResultAssert result = gfsh.executeAndAssertThat("list data-source");

    result.statusIsSuccess()
        .tableHasRowWithValues("name", "pooled", "in use", "url", "simple", "false", "false",
            "jdbc:derby:newDB;create=true");
  }

  @Test
  public void listDataSourceUsedByRegionsHasCorrectOutput() {
    gfsh.executeAndAssertThat(
        "create data-source --name=simple --url=\"jdbc:derby:newDB;create=true\"")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");
    gfsh.executeAndAssertThat("create region --name=region1 --type=REPLICATE").statusIsSuccess();
    gfsh.executeAndAssertThat("create region --name=region2 --type=PARTITION").statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create jdbc-mapping --region=region1 --data-source=simple --pdx-name=myPdx");
    gfsh.executeAndAssertThat(
        "create jdbc-mapping --region=region2 --data-source=simple --pdx-name=myPdx");

    CommandResultAssert result = gfsh.executeAndAssertThat("list data-source");

    result.statusIsSuccess()
        .tableHasRowWithValues("name", "pooled", "in use", "url", "simple", "false", "true",
            "jdbc:derby:newDB;create=true");
  }

  @Test
  public void listDataSourceForPooledDataSource() {
    gfsh.executeAndAssertThat(
        "create data-source --name=pooled --pooled --url=\"jdbc:derby:newDB;create=true\" --pooled-data-source-factory-class=org.apache.geode.internal.jta.CacheJTAPooledDataSourceFactory --pool-properties={'name':'prop1','value':'value1'},{'name':'pool.prop2','value':'value2'}")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");

    gfsh.executeAndAssertThat("list data-source").statusIsSuccess()
        .tableHasRowWithValues("name", "pooled", "in use", "url", "pooled", "true", "false",
            "jdbc:derby:newDB;create=true");
  }

  @Test
  public void listDataSourceWithMultipleDataSourcesListsAll() {
    gfsh.executeAndAssertThat(
        "create data-source --name=simple --url=\"jdbc:derby:newDB;create=true\" --username=joe --password=myPassword")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");
    gfsh.executeAndAssertThat(
        "create data-source --name=pooled --pooled --url=\"jdbc:derby:newDB;create=true\" --pooled-data-source-factory-class=org.apache.geode.internal.jta.CacheJTAPooledDataSourceFactory --pool-properties={'name':'prop1','value':'value1'},{'name':'pool.prop2','value':'value2'}")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");

    gfsh.executeAndAssertThat("list data-source").statusIsSuccess()
        .tableHasRowWithValues("name", "pooled", "in use", "url", "pooled", "true", "false",
            "jdbc:derby:newDB;create=true")
        .tableHasRowWithValues("name", "pooled", "in use", "url", "simple", "false", "false",
            "jdbc:derby:newDB;create=true");
  }

  @Test
  public void listDataSourceWithNoDataSources() {
    gfsh.executeAndAssertThat("list data-source").statusIsSuccess()
        .containsOutput("No data sources found");
  }
}
