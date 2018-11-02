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
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.JDBCConnectorTest;

@Category({JDBCConnectorTest.class})
public class CreateMappingCommandIntegrationTest {

  private InternalCache cache;
  private CreateMappingCommand createRegionMappingCommand;

  private String regionName;
  private String connectionName;
  private String tableName;
  private String pdxClass;

  @Before
  public void setup() {
    regionName = "regionName";
    connectionName = "connection";
    tableName = "testTable";
    pdxClass = "myPdxClass";

    cache = (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
        .set(ENABLE_CLUSTER_CONFIGURATION, "true").create();
    cache.createRegionFactory(RegionShortcut.LOCAL).create(regionName);

    createRegionMappingCommand = new CreateMappingCommand();
    createRegionMappingCommand.setCache(cache);
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void createsRegionMappingInService() {
    ResultModel result = createRegionMappingCommand.createMapping(regionName, connectionName,
        tableName, pdxClass);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);

    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    RegionMapping regionMapping = service.getMappingForRegion(regionName);

    assertThat(regionMapping).isNotNull();
    assertThat(regionMapping.getRegionName()).isEqualTo(regionName);
    assertThat(regionMapping.getConnectionConfigName()).isEqualTo(connectionName);
    assertThat(regionMapping.getTableName()).isEqualTo(tableName);
    assertThat(regionMapping.getPdxName()).isEqualTo(pdxClass);
  }

  @Test
  public void createsRegionMappingOnceOnly() {
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    ResultModel result;
    result =
        createRegionMappingCommand.createMapping(regionName, connectionName, tableName, pdxClass);
    assertThat(result.getStatus()).isSameAs(Result.Status.OK);

    IgnoredException ignoredException =
        IgnoredException.addIgnoredException(RegionMappingExistsException.class.getName());

    try {
      result =
          createRegionMappingCommand.createMapping(regionName, connectionName, tableName, pdxClass);
    } finally {
      ignoredException.remove();
    }

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
  }

  @Test
  public void createsRegionMappingWithMinimumParams() {
    ResultModel result =
        createRegionMappingCommand.createMapping(regionName, connectionName, null, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);

    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    RegionMapping regionMapping = service.getMappingForRegion(regionName);

    assertThat(regionMapping).isNotNull();
    assertThat(regionMapping.getRegionName()).isEqualTo(regionName);
    assertThat(regionMapping.getConnectionConfigName()).isEqualTo(connectionName);
    assertThat(regionMapping.getTableName()).isNull();
    assertThat(regionMapping.getPdxName()).isNull();
  }
}
