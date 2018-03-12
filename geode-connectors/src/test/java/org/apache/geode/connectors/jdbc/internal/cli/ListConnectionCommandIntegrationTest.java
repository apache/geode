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

import static org.apache.geode.connectors.jdbc.internal.cli.ListConnectionCommand.LIST_OF_CONNECTIONS;
import static org.apache.geode.connectors.jdbc.internal.cli.ListConnectionCommand.NO_CONNECTIONS_FOUND;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigBuilder;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class ListConnectionCommandIntegrationTest {

  private InternalCache cache;
  private JdbcConnectorService service;

  private ConnectionConfiguration connectionConfig1;
  private ConnectionConfiguration connectionConfig2;
  private ConnectionConfiguration connectionConfig3;

  private ListConnectionCommand command;

  @Before
  public void setup() throws Exception {
    String[] params = new String[] {"param1:value1", "param2:value2"};

    connectionConfig1 = new ConnectionConfigBuilder().withName("connection1").withUrl("url1")
        .withUser("user1").withPassword("password1").withParameters(params).build();
    connectionConfig2 = new ConnectionConfigBuilder().withName("connection2").withUrl("url2")
        .withUser("user2").withPassword("password2").withParameters(params).build();
    connectionConfig3 = new ConnectionConfigBuilder().withName("connection3").withUrl("url3")
        .withUser("user3").withPassword("password3").withParameters(params).build();

    cache = (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
        .set(ENABLE_CLUSTER_CONFIGURATION, "true").create();
    service = cache.getService(JdbcConnectorService.class);

    command = new ListConnectionCommand();
    command.setCache(cache);
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void displaysNoConnectionsFoundWhenZeroConnectionsExist() throws Exception {
    Result result = command.listConnection();

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    CommandResult commandResult = (CommandResult) result;
    String tableContent = commandResult.getTableContent().toString();
    assertThat(tableContent).contains(NO_CONNECTIONS_FOUND);
    assertThat(tableContent).doesNotContain(connectionConfig1.getName())
        .doesNotContain(connectionConfig2.getName()).doesNotContain(connectionConfig3.getName());
  }

  @Test
  public void displaysListOfConnectionsHeaderWhenOneConnectionExists() throws Exception {
    service.createConnectionConfig(connectionConfig1);

    Result result = command.listConnection();

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    CommandResult commandResult = (CommandResult) result;
    String tableContent = commandResult.getTableContent().toString();
    assertThat(tableContent).contains(LIST_OF_CONNECTIONS);
    assertThat(tableContent).contains(connectionConfig1.getName());
  }

  @Test
  public void displaysMultipleConnectionsByName() throws Exception {
    service.createConnectionConfig(connectionConfig1);
    service.createConnectionConfig(connectionConfig2);
    service.createConnectionConfig(connectionConfig3);

    Result result = command.listConnection();

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    CommandResult commandResult = (CommandResult) result;
    assertThat(commandResult.getTableContent().toString()).contains(connectionConfig1.getName())
        .contains(connectionConfig2.getName()).contains(connectionConfig3.getName());
  }
}
