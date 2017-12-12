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
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigBuilder;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class DestroyConnectionCommandIntegrationTest {

  private String connectionName;
  private InternalCache cache;
  private ConnectionConfiguration connectionConfig;

  private DestroyConnectionCommand command;

  @Before
  public void setup() throws Exception {
    connectionName = "connectionName";

    String[] params = new String[] {"param1:value1", "param2:value2"};
    connectionConfig = new ConnectionConfigBuilder().withName(connectionName).withUrl("url")
        .withUser("user").withPassword("password").withParameters(params).build();

    cache = (InternalCache) new CacheFactory().set(ENABLE_CLUSTER_CONFIGURATION, "true").create();

    command = new DestroyConnectionCommand();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void destroysNamedConnection() throws Exception {
    InternalJdbcConnectorService service = cache.getService(InternalJdbcConnectorService.class);
    service.createConnectionConfig(connectionConfig);
    assertThat(service.getConnectionConfig(connectionName)).isSameAs(connectionConfig);

    Result result = command.destroyConnection(connectionName);
    assertThat(result.getStatus()).isSameAs(Result.Status.OK);

    assertThat(service.getConnectionConfig(connectionName)).isNull();
  }

  @Test
  public void returnsErrorIfNamedConnectionNotFound() throws Exception {
    InternalJdbcConnectorService service = cache.getService(InternalJdbcConnectorService.class);
    assertThat(service.getConnectionConfig(connectionName)).isNull();

    Result result = command.destroyConnection(connectionName);
    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);

    assertThat(service.getConnectionConfig(connectionName)).isNull();
  }
}
