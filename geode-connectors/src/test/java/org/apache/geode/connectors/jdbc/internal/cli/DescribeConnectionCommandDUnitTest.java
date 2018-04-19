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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class DescribeConnectionCommandDUnitTest {

  private static final String CONNECTION_NAME = "connectionName";

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static ClusterStartupRule startupRule = new ClusterStartupRule();

  private static MemberVM locator, server;

  @BeforeClass
  public static void before() throws Exception {
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
  }

  @Test
  public void describesExistingConnection() {
    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_CONNECTION)
        .addOption(DESCRIBE_CONNECTION__NAME, CONNECTION_NAME);

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
  public void reportsNoConfigurationFound() {
    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_CONNECTION)
        .addOption(DESCRIBE_CONNECTION__NAME, "nonExisting");

    CommandResultAssert commandResultAssert = gfsh.executeAndAssertThat(csb.toString());

    commandResultAssert.statusIsError();
    commandResultAssert.containsOutput(String.format("connection named 'nonExisting' not found"));
  }

  @Test
  public void reportConfigurationFoundOnMember() {
    CommandResultAssert commandResultAssert = gfsh
        .executeAndAssertThat(
            DESCRIBE_CONNECTION + " --name=" + CONNECTION_NAME + " --member=server-1")
        .statusIsSuccess();

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
}
