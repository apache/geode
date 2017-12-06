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

import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__PASSWORD;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__URL;
import static org.apache.geode.connectors.jdbc.internal.cli.DescribeConnectionCommand.OBSCURED_PASSWORD;
import static org.apache.geode.connectors.jdbc.internal.cli.DescribeConnectionCommand.RESULT_SECTION_NAME;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.management.internal.cli.result.AbstractResultData.SECTION_DATA_ACCESSOR;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigBuilder;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.CommandResult;

public class DescribeConnectionCommandIntegrationTest {
  private static final String CONNECTION = "connection";

  private InternalCache cache;
  private InternalJdbcConnectorService service;
  private ConnectionConfiguration connectionConfig;
  private DescribeConnectionCommand command;

  @Before
  public void setup() {
    String[] params = new String[] {"param1:value1", "param2:value2"};

    connectionConfig = new ConnectionConfigBuilder().withName(CONNECTION).withUrl("myUrl")
        .withUser("username").withPassword("secret").withParameters(params).build();

    cache = (InternalCache) new CacheFactory().set(ENABLE_CLUSTER_CONFIGURATION, "true").create();
    service = cache.getService(InternalJdbcConnectorService.class);

    command = new DescribeConnectionCommand();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void displaysNoConnectionFoundMessageWhenConfigurationDoesNotExist() {
    String notExistingConnectionName = "non existing";
    Result result = command.describeConnection(notExistingConnectionName);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    CommandResult commandResult = (CommandResult) result;
    String tableContent = commandResult.getTableContent().toString();
    assertThat(tableContent)
        .contains("Connection named '" + notExistingConnectionName + "' not found");
  }

  @Test
  public void displaysConnectionInformationWhenConfigurationExists() throws Exception {
    service.createConnectionConfig(connectionConfig);
    Result result = command.describeConnection(CONNECTION);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    CommandResult commandResult = (CommandResult) result;
    GfJsonObject sectionContent = commandResult.getTableContent()
        .getJSONObject(SECTION_DATA_ACCESSOR + "-" + RESULT_SECTION_NAME);

    assertThat(sectionContent.get(CREATE_CONNECTION__NAME)).isEqualTo(connectionConfig.getName());
    assertThat(sectionContent.get(CREATE_CONNECTION__URL)).isEqualTo(connectionConfig.getUrl());

    Properties connectionProperties = connectionConfig.getConnectionProperties();
    for (String parameterName : connectionProperties.stringPropertyNames()) {
      if (parameterName.equals(CREATE_CONNECTION__PASSWORD)) {
        assertThat(sectionContent.get(parameterName)).isEqualTo(OBSCURED_PASSWORD);
      } else {
        assertThat(sectionContent.get(parameterName))
            .isEqualTo(connectionProperties.getProperty(parameterName));
      }
    }
  }

  @Test
  public void doesNotDisplayParametersWithNoValue() throws Exception {
    connectionConfig = new ConnectionConfigBuilder().withName(CONNECTION).withUrl("myUrl").build();

    service.createConnectionConfig(connectionConfig);
    Result result = command.describeConnection(CONNECTION);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    CommandResult commandResult = (CommandResult) result;
    GfJsonObject sectionContent = commandResult.getTableContent()
        .getJSONObject(SECTION_DATA_ACCESSOR + "-" + RESULT_SECTION_NAME);

    assertThat(sectionContent.get(CREATE_CONNECTION__NAME)).isEqualTo(connectionConfig.getName());
    assertThat(sectionContent.get(CREATE_CONNECTION__URL)).isEqualTo(connectionConfig.getUrl());
    assertThat(sectionContent.get(CREATE_CONNECTION__PASSWORD)).isNull();
  }
}
