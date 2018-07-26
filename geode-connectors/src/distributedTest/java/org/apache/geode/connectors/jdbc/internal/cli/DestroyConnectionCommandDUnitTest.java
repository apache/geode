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

import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__PARAMS;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__PASSWORD;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__URL;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__USER;
import static org.apache.geode.connectors.jdbc.internal.cli.DestroyConnectionCommand.DESTROY_CONNECTION;
import static org.apache.geode.connectors.jdbc.internal.cli.DestroyConnectionCommand.DESTROY_CONNECTION__NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.JDBCConnectorTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({JDBCConnectorTest.class})
public class DestroyConnectionCommandDUnitTest implements Serializable {

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

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_CONNECTION);
    csb.addOption(CREATE_CONNECTION__NAME, "name");
    csb.addOption(CREATE_CONNECTION__URL, "url");
    csb.addOption(CREATE_CONNECTION__USER, "username");
    csb.addOption(CREATE_CONNECTION__PASSWORD, "secret");
    csb.addOption(CREATE_CONNECTION__PARAMS, "param1:value1,param2:value2");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  @Test
  public void destroysConnection() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(DESTROY_CONNECTION);
    csb.addOption(DESTROY_CONNECTION__NAME, "name");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    locator.invoke(() -> {
      String xml = InternalLocator.getLocator().getConfigurationPersistenceService()
          .getConfiguration("cluster").getCacheXmlContent();
      assertThat(xml).contains("jdbc:connector-service");
    });

    server.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      ConnectorService.Connection config =
          cache.getService(JdbcConnectorService.class).getConnectionConfig("name");
      assertThat(config).isNull();
    });
  }
}
