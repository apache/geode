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
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigExistsException;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.result.ResultModel;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.JDBCConnectorTest;

@Category({JDBCConnectorTest.class})
public class CreateConnectionCommandIntegrationTest {

  private InternalCache cache;
  private CreateConnectionCommand createConnectionCommand;

  private String name;
  private String url;
  private String user;
  private String password;
  private String[] params;

  @Before
  public void setup() throws Exception {
    name = "name";
    url = "url";
    user = "user";
    password = "password";
    params = new String[] {"param1:value1", "param2:value2"};

    cache = (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
        .set(ENABLE_CLUSTER_CONFIGURATION, "true").create();

    createConnectionCommand = new CreateConnectionCommand();
    createConnectionCommand.setCache(cache);
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void createsConnectionConfigurationInService() throws Exception {
    ResultModel result =
        createConnectionCommand.createConnection(name, url, user, password, params);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);

    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    ConnectorService.Connection connectionConfig = service.getConnectionConfig(name);

    assertThat(connectionConfig).isNotNull();
    assertThat(connectionConfig.getName()).isEqualTo(name);
    assertThat(connectionConfig.getUrl()).isEqualTo(url);
    assertThat(connectionConfig.getUser()).isEqualTo(user);
    assertThat(connectionConfig.getPassword()).isEqualTo(password);
    assertThat(connectionConfig.getConnectionProperties()).containsEntry("param1", "value1")
        .containsEntry("param2", "value2");
  }

  @Test
  public void createsConnectionOnceOnly() throws Exception {
    createConnectionCommand.createConnection(name, url, user, password, params);
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);

    ConnectorService.Connection connectionConfig = service.getConnectionConfig(name);

    IgnoredException ignoredException =
        IgnoredException.addIgnoredException(ConnectionConfigExistsException.class.getName());

    ResultModel result;
    try {
      result = createConnectionCommand.createConnection(name, url, user, password, params);
    } finally {
      ignoredException.remove();
    }

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);

    assertThat(service.getConnectionConfig(name)).isSameAs(connectionConfig);
  }

  @Test
  public void createsConnectionConfigurationWithMinimumParams() throws Exception {
    ResultModel result = createConnectionCommand.createConnection(name, url, null, null, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);

    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    ConnectorService.Connection connectionConfig = service.getConnectionConfig(name);

    assertThat(connectionConfig).isNotNull();
    assertThat(connectionConfig.getName()).isEqualTo(name);
    assertThat(connectionConfig.getUrl()).isEqualTo(url);
    assertThat(connectionConfig.getUser()).isNull();
    assertThat(connectionConfig.getPassword()).isNull();
    assertThat(connectionConfig.getConnectionProperties().size()).isEqualTo(0);
  }

}
