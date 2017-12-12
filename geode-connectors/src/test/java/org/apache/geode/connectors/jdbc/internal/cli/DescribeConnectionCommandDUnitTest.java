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

import static org.apache.geode.connectors.jdbc.internal.cli.DescribeConnectionCommand.DESCRIBE_CONNECTION;
import static org.apache.geode.connectors.jdbc.internal.cli.DescribeConnectionCommand.DESCRIBE_CONNECTION__NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.connectors.jdbc.internal.ConnectionConfigBuilder;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigExistsException;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(DistributedTest.class)
public class DescribeConnectionCommandDUnitTest implements Serializable {

  private static final String CONNECTION_NAME = "connectionName";

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public LocatorServerStartupRule startupRule = new LocatorServerStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private MemberVM server;

  @Before
  public void before() throws Exception {
    MemberVM locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void describesExistingConnection() {
    server.invoke(this::createConnection);
    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_CONNECTION)
        .addOption(DESCRIBE_CONNECTION__NAME, CONNECTION_NAME);

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();
    commandResultAssert.containsKeyValuePair("name", CONNECTION_NAME);
    commandResultAssert.containsKeyValuePair("url", "myUrl");
    commandResultAssert.containsKeyValuePair("user", "username");
    commandResultAssert.containsKeyValuePair("password", "\\*\\*\\*\\*\\*\\*\\*\\*");
    commandResultAssert.containsKeyValuePair("key1", "value1");
    commandResultAssert.containsKeyValuePair("key2", "value2");
  }

  @Test
  public void reportsNoConfigurationFound() {
    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_CONNECTION)
        .addOption(DESCRIBE_CONNECTION__NAME, "nonExisting");

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsSuccess();
    commandResultAssert
        .containsOutput(String.format("Connection named '%s' not found", "nonExisting"));
  }

  private void createConnection() throws ConnectionConfigExistsException {
    InternalCache cache = LocatorServerStartupRule.getCache();
    InternalJdbcConnectorService service = cache.getService(InternalJdbcConnectorService.class);

    String[] params = new String[] {"key1:value1", "key2:value2"};
    service.createConnectionConfig(
        new ConnectionConfigBuilder().withName(CONNECTION_NAME).withUrl("myUrl")
            .withUser("username").withPassword("secret").withParameters(params).build());

    assertThat(service.getConnectionConfig(CONNECTION_NAME)).isNotNull();
  }


}
