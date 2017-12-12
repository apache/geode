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

import static org.apache.geode.connectors.jdbc.internal.cli.AlterConnectionCommand.ALTER_CONNECTION__NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.AlterConnectionCommand.ALTER_CONNECTION__PARAMS;
import static org.apache.geode.connectors.jdbc.internal.cli.AlterConnectionCommand.ALTER_CONNECTION__PASSWORD;
import static org.apache.geode.connectors.jdbc.internal.cli.AlterConnectionCommand.ALTER_CONNECTION__URL;
import static org.apache.geode.connectors.jdbc.internal.cli.AlterConnectionCommand.ALTER_CONNECTION__USER;
import static org.apache.geode.connectors.jdbc.internal.cli.AlterConnectionCommand.ALTER_JDBC_CONNECTION;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__PARAMS;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__PASSWORD;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__URL;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__USER;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(DistributedTest.class)
public class AlterConnectionCommandDUnitTest {

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public LocatorServerStartupRule startupRule = new LocatorServerStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private MemberVM locator;
  private MemberVM server;

  @Before
  public void before() throws Exception {
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
  public void altersConnectionWithNewValues() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(ALTER_JDBC_CONNECTION);
    csb.addOption(ALTER_CONNECTION__NAME, "name");
    csb.addOption(ALTER_CONNECTION__URL, "newUrl");
    csb.addOption(ALTER_CONNECTION__USER, "newUsername");
    csb.addOption(ALTER_CONNECTION__PASSWORD, "newPassword");
    csb.addOption(ALTER_CONNECTION__PARAMS, "Key1:Value1,Key22:Value22");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();


    locator.invoke(() -> {
      String xml = InternalLocator.getLocator().getSharedConfiguration().getConfiguration("cluster")
          .getCacheXmlContent();
      assertThat(xml).isNotNull().contains("jdbc:connector-service");
    });

    server.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.getCache();
      ConnectionConfiguration config =
          cache.getService(InternalJdbcConnectorService.class).getConnectionConfig("name");
      assertThat(config.getUrl()).isEqualTo("newUrl");
      assertThat(config.getUser()).isEqualTo("newUsername");
      assertThat(config.getPassword()).isEqualTo("newPassword");
      assertThat(config.getConnectionProperties()).containsEntry("Key1", "Value1")
          .containsEntry("Key22", "Value22");
    });
  }

  @Test
  public void altersConnectionByRemovingValues() {
    CommandStringBuilder csb = new CommandStringBuilder(ALTER_JDBC_CONNECTION);
    csb.addOption(ALTER_CONNECTION__NAME, "name");
    csb.addOption(ALTER_CONNECTION__URL, "");
    csb.addOption(ALTER_CONNECTION__USER, "");
    csb.addOption(ALTER_CONNECTION__PASSWORD, "");
    csb.addOption(ALTER_CONNECTION__PARAMS, "");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    locator.invoke(() -> {
      String xml = InternalLocator.getLocator().getSharedConfiguration().getConfiguration("cluster")
          .getCacheXmlContent();
      assertThat(xml).isNotNull().contains("jdbc:connector-service");
    });

    server.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.getCache();
      ConnectionConfiguration config =
          cache.getService(InternalJdbcConnectorService.class).getConnectionConfig("name");
      assertThat(config.getUrl()).isNull();
      assertThat(config.getUser()).isNull();
      assertThat(config.getPassword()).isNull();
      assertThat(config.getConnectionProperties()).hasSize(0);
    });
  }
}
