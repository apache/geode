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
import org.apache.geode.management.cli.result.ResultModel;
import org.apache.geode.test.junit.categories.JDBCConnectorTest;

@Category({JDBCConnectorTest.class})
public class AlterConnectionCommandIntegrationTest {

  private InternalCache cache;
  private AlterConnectionCommand alterConnectionCommand;

  private String name;

  @Before
  public void setup() {
    name = "name";
    String url = "url";
    String user = "user";
    String password = "password";
    String[] params = new String[] {"param1:value1", "param2:value2"};

    cache = (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
        .set(ENABLE_CLUSTER_CONFIGURATION, "true").create();
    CreateConnectionCommand create = new CreateConnectionCommand();
    create.setCache(cache);
    create.createConnection(name, url, user, password, params);

    alterConnectionCommand = new AlterConnectionCommand();
    alterConnectionCommand.setCache(cache);
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void altersConnectionConfigurationInService() {
    String[] newParams = new String[] {"key1:value1", "key2:value2"};
    ResultModel result =
        alterConnectionCommand.alterConnection(name, "newUrl", "newUser", "newPassword", newParams);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);

    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    ConnectorService.Connection connectionConfig = service.getConnectionConfig(name);

    assertThat(connectionConfig).isNotNull();
    assertThat(connectionConfig.getName()).isEqualTo(name);
    assertThat(connectionConfig.getUrl()).isEqualTo("newUrl");
    assertThat(connectionConfig.getUser()).isEqualTo("newUser");
    assertThat(connectionConfig.getPassword()).isEqualTo("newPassword");
    assertThat(connectionConfig.getConnectionProperties()).containsEntry("key1", "value1")
        .containsEntry("key2", "value2");
  }

}
