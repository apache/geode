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

import static org.apache.geode.connectors.jdbc.internal.cli.ListConnectionCommand.LIST_JDBC_CONNECTION;
import static org.apache.geode.connectors.jdbc.internal.cli.ListConnectionCommand.LIST_OF_CONNECTIONS;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(DistributedTest.class)
public class ListConnectionCommandDUnitTest implements Serializable {

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private MemberVM locator;
  private MemberVM server;

  private String connectionName;

  @Before
  public void before() throws Exception {
    connectionName = "name";

    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void listsOneConnection() {
    String conn1 =
        "create jdbc-connection --name=name --url=url --user=user --password=pass --params=param1:value1,param2:value2";
    gfsh.executeAndAssertThat(conn1).statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(LIST_JDBC_CONNECTION);
    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();
    commandResultAssert.tableHasRowCount(LIST_OF_CONNECTIONS, 1);
    commandResultAssert.tableHasColumnOnlyWithValues(LIST_OF_CONNECTIONS, connectionName);
  }

  @Test
  public void listsMultipleConnections() {
    String conn1 =
        "create jdbc-connection --name=name-1 --url=url --user=user --password=pass --params=param1:value1,param2:value2";
    gfsh.executeAndAssertThat(conn1).statusIsSuccess();
    String conn2 =
        "create jdbc-connection --name=name-2 --url=url --user=user --password=pass --params=param1:value1,param2:value2";
    gfsh.executeAndAssertThat(conn2).statusIsSuccess();
    String conn3 =
        "create jdbc-connection --name=name-3 --url=url --user=user --password=pass --params=param1:value1,param2:value2";
    gfsh.executeAndAssertThat(conn3).statusIsSuccess();

    CommandStringBuilder csb = new CommandStringBuilder(LIST_JDBC_CONNECTION);
    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();
    commandResultAssert.tableHasRowCount(LIST_OF_CONNECTIONS, 3);
    commandResultAssert.tableHasColumnOnlyWithValues(LIST_OF_CONNECTIONS, connectionName + "-1",
        connectionName + "-2", connectionName + "-3");
  }

  @Test
  public void reportsNoConnectionsFound() {
    CommandStringBuilder csb = new CommandStringBuilder(LIST_JDBC_CONNECTION);

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsOutput("No connections found");
  }
}
