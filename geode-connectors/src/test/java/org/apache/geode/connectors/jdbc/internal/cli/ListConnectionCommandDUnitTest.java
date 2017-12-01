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

import static org.apache.geode.connectors.jdbc.internal.cli.DestroyConnectionCommand.DESTROY_CONNECTION;
import static org.apache.geode.connectors.jdbc.internal.cli.DestroyConnectionCommand.DESTROY_CONNECTION__NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.ListConnectionCommand.LIST_JDBC_CONNECTION;
import static org.apache.geode.connectors.jdbc.internal.cli.ListConnectionCommand.LIST_OF_CONNECTIONS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.connectors.jdbc.internal.ConnectionConfigBuilder;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigExistsException;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
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
  public LocatorServerStartupRule startupRule = new LocatorServerStartupRule();

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
  public void listsOneConnection() throws Exception {
    server.invoke(() -> createOneConnection());

    CommandStringBuilder csb = new CommandStringBuilder(LIST_JDBC_CONNECTION);

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();

    commandResultAssert.tableHasRowCount(LIST_OF_CONNECTIONS, 1);

    // TODO: assert that the table contains LIST_OF_CONNECTIONS
    // TODO: assert that the table contains name of connection
  }

  @Test
  public void listsMultipleConnections() throws Exception {
    // TODO: assert that the table contains LIST_OF_CONNECTIONS
    // TODO: assert that the table contains names of several connections
  }

  @Test
  public void reportsNoConnectionsFound() throws Exception {
    // TODO: assert that the results show ListConnectionCommand.NO_CONNECTIONS_FOUND
  }

  private void createOneConnection() throws ConnectionConfigExistsException {
    InternalCache cache = LocatorServerStartupRule.getCache();
    InternalJdbcConnectorService service = cache.getService(InternalJdbcConnectorService.class);

    service.createConnectionConfig(new ConnectionConfigBuilder().withName(connectionName).build());

    assertThat(service.getConnectionConfig(connectionName)).isNotNull();
  }
}
