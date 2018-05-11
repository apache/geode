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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.JDBCConnectorTest;

@Category({IntegrationTest.class, JDBCConnectorTest.class, GfshTest.class})
public class DestroyConnectionCommandIntegrationTest {

  private String connectionName;
  private InternalCache cache;
  private ConnectorService.Connection connectionConfig;

  private DestroyConnectionCommand command;

  @Before
  public void setup() throws Exception {
    connectionName = "connectionName";

    String[] params = new String[] {"param1:value1", "param2:value2"};
    connectionConfig =
        new ConnectorService.Connection(connectionName, "url", "user", "password", params);

    cache = (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
        .set(ENABLE_CLUSTER_CONFIGURATION, "true").create();

    command = new DestroyConnectionCommand();
    command.setCache(cache);
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void destroysNamedConnection() throws Exception {
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    service.createConnectionConfig(connectionConfig);
    assertThat(service.getConnectionConfig(connectionName)).isSameAs(connectionConfig);

    ResultModel result = command.destroyConnection(connectionName);
    assertThat(result.getStatus()).isSameAs(Result.Status.OK);

    assertThat(service.getConnectionConfig(connectionName)).isNull();
  }

  @Test
  public void returnsErrorIfNamedConnectionNotFound() throws Exception {
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    assertThat(service.getConnectionConfig(connectionName)).isNull();

    ResultModel result = command.destroyConnection(connectionName);
    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);

    assertThat(service.getConnectionConfig(connectionName)).isNull();
  }
}
