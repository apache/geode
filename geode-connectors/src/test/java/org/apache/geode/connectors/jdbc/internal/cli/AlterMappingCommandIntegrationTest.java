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
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.JDBCConnectorTest;

@Category({IntegrationTest.class, JDBCConnectorTest.class})
public class AlterMappingCommandIntegrationTest {

  private InternalCache cache;
  private AlterMappingCommand alterRegionMappingCommand;

  private String regionName;

  @Before
  public void setup() {
    regionName = "regionName";
    String connectionName = "connection";
    String tableName = "testTable";
    String pdxClass = "myPdxClass";
    Boolean keyInValue = true;
    String[] fieldMappings = new String[] {"field1:column1", "field2:column2"};

    cache = (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
        .set(ENABLE_CLUSTER_CONFIGURATION, "true").create();
    CreateMappingCommand create = new CreateMappingCommand();
    create.setCache(cache);
    create.createMapping(regionName, connectionName, tableName, pdxClass, keyInValue,
        fieldMappings);

    alterRegionMappingCommand = new AlterMappingCommand();
    alterRegionMappingCommand.setCache(cache);
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void altersRegionMappingInService() {
    String[] newMappings = new String[] {"field3:column3", "field4:column4"};
    ResultModel result = alterRegionMappingCommand.alterMapping(regionName, "newConnection",
        "newTable", "newPdxClass", false, newMappings);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);

    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    ConnectorService.RegionMapping regionMapping = service.getMappingForRegion(regionName);

    assertThat(regionMapping).isNotNull();
    assertThat(regionMapping.getConnectionConfigName()).isEqualTo("newConnection");
    assertThat(regionMapping.getTableName()).isEqualTo("newTable");
    assertThat(regionMapping.getPdxClassName()).isEqualTo("newPdxClass");
    assertThat(regionMapping.isPrimaryKeyInValue()).isFalse();
    // assertThat(regionMapping.getFieldToColumnMap()).containsEntry("field3", "column3")
    // .containsEntry("field4", "column4");
  }

}
