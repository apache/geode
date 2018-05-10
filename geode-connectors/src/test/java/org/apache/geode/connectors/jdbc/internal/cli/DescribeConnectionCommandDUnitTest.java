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
import static org.apache.geode.connectors.jdbc.internal.cli.DescribeConnectionCommand.DESCRIBE_CONNECTION;
import static org.apache.geode.connectors.jdbc.internal.cli.DescribeConnectionCommand.DESCRIBE_CONNECTION__NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.connectors.jdbc.internal.ConnectionConfigExistsException;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class DescribeConnectionCommandDUnitTest implements Serializable {

  private static final String CONNECTION_NAME = "connectionName";

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public transient ClusterStartupRule startupRule = new ClusterStartupRule();

  private MemberVM locator, server;

  @Test
  public void describesExistingConnection() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);

    CommandStringBuilder csb = new CommandStringBuilder(CREATE_CONNECTION);
    csb.addOption(CREATE_CONNECTION__NAME, CONNECTION_NAME);
    csb.addOption(CREATE_CONNECTION__URL, "myUrl");
    csb.addOption(CREATE_CONNECTION__USER, "username");
    csb.addOption(CREATE_CONNECTION__PASSWORD, "secret");
    csb.addOption(CREATE_CONNECTION__PARAMS, "key1:value1,key2:value2");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(DESCRIBE_CONNECTION).addOption(DESCRIBE_CONNECTION__NAME,
        CONNECTION_NAME);

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsKeyValuePair("name", CONNECTION_NAME);
    commandResultAssert.containsKeyValuePair("url", "myUrl");
    commandResultAssert.containsKeyValuePair("user", "username");
    commandResultAssert.containsKeyValuePair("password", "\\*\\*\\*\\*\\*\\*\\*\\*");
    commandResultAssert.containsOutput("key1");
    commandResultAssert.containsOutput("value1");
    commandResultAssert.containsOutput("key2");
    commandResultAssert.containsOutput("value2");
  }

  @Test
  public void reportsNoConfigurationFound() throws Exception {
    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);

    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_CONNECTION)
        .addOption(DESCRIBE_CONNECTION__NAME, "nonExisting");

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());
    commandResultAssert.statusIsError();
    commandResultAssert.containsOutput(
        String.format("(Experimental) \n" + "connection named 'nonExisting' not found"));
  }

  @Test
  public void reportConfigurationFoundOnMember() throws Exception {
    Properties properties = new Properties();
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");

    locator = startupRule.startLocatorVM(0, properties);
    server = startupRule.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);

    server.invoke(() -> createConnection());

    CommandResultAssert commandResultAssert = gfsh
        .executeAndAssertThat(DESCRIBE_CONNECTION + " --name=" + CONNECTION_NAME).statusIsSuccess();

    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsKeyValuePair("name", CONNECTION_NAME);
    commandResultAssert.containsKeyValuePair("url", "myUrl");
    commandResultAssert.containsKeyValuePair("user", "username");
    commandResultAssert.containsKeyValuePair("password", "\\*\\*\\*\\*\\*\\*\\*\\*");
  }

  private void createConnection() throws ConnectionConfigExistsException {
    InternalCache cache = ClusterStartupRule.getCache();
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    service.createConnectionConfig(new ConnectorService.Connection(CONNECTION_NAME, "myUrl",
        "username", "password ", (String) null));
    assertThat(service.getConnectionConfig(CONNECTION_NAME)).isNotNull();
  }
}
