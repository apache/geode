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
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class CreateRegionMappingCommandIntegrationTest {

  private InternalCache cache;
  private CreateRegionMappingCommand createRegionMappingCommand;

  private String regionName;
  private String connectionName;
  private String tableName;
  private String pdxClass;
  private boolean keyInValue;
  private String[] fieldMappings;

  @Before
  public void setup() {
    regionName = "regionName";
    connectionName = "connection";
    tableName = "testTable";
    pdxClass = "myPdxClass";
    keyInValue = true;
    fieldMappings = new String[] {"field1:column1", "field2:column2"};

    cache = (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
        .set(ENABLE_CLUSTER_CONFIGURATION, "true").create();

    createRegionMappingCommand = new CreateRegionMappingCommand();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void createsRegionMappingInService() {
    Result result = createRegionMappingCommand.createMapping(regionName, connectionName, tableName,
        pdxClass, keyInValue, fieldMappings);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);

    InternalJdbcConnectorService service = cache.getService(InternalJdbcConnectorService.class);
    RegionMapping regionMapping = service.getMappingForRegion(regionName);

    assertThat(regionMapping).isNotNull();
    assertThat(regionMapping.getRegionName()).isEqualTo(regionName);
    assertThat(regionMapping.getConnectionConfigName()).isEqualTo(connectionName);
    assertThat(regionMapping.getTableName()).isEqualTo(tableName);
    assertThat(regionMapping.getPdxClassName()).isEqualTo(pdxClass);
    assertThat(regionMapping.isPrimaryKeyInValue()).isEqualTo(keyInValue);
    assertThat(regionMapping.getColumnNameForField("field1")).isEqualTo("column1");
    assertThat(regionMapping.getColumnNameForField("field2")).isEqualTo("column2");
  }

  @Test
  public void createsConnectionOnceOnly() {
    createRegionMappingCommand.createMapping(regionName, connectionName, tableName, pdxClass,
        keyInValue, fieldMappings);
    InternalJdbcConnectorService service = cache.getService(InternalJdbcConnectorService.class);

    ConnectionConfiguration connectionConfig = service.getConnectionConfig(regionName);

    Result result = createRegionMappingCommand.createMapping(regionName, connectionName, tableName,
        pdxClass, keyInValue, fieldMappings);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);

    assertThat(service.getConnectionConfig(regionName)).isSameAs(connectionConfig);
  }
}
