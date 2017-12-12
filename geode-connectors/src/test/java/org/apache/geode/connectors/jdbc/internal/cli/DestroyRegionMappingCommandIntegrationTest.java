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
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMapping;
import org.apache.geode.connectors.jdbc.internal.RegionMappingBuilder;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class DestroyRegionMappingCommandIntegrationTest {

  private String regionName;
  private InternalCache cache;
  private RegionMapping mapping;

  private DestroyRegionMappingCommand command;

  @Before
  public void setup() throws Exception {
    regionName = "testRegion";

    String[] params = new String[] {"param1:value1", "param2:value2"};
    mapping = new RegionMappingBuilder().withRegionName(regionName).build();

    cache = (InternalCache) new CacheFactory().set(ENABLE_CLUSTER_CONFIGURATION, "true").create();

    command = new DestroyRegionMappingCommand();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void destroysNamedMapping() throws Exception {
    InternalJdbcConnectorService service = cache.getService(InternalJdbcConnectorService.class);
    service.createRegionMapping(mapping);
    assertThat(service.getMappingForRegion(regionName)).isSameAs(mapping);

    Result result = command.destroyMapping(regionName);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    assertThat(service.getMappingForRegion(regionName)).isNull();
  }

  @Test
  public void returnsErrorIfNamedMappingNotFound() throws Exception {
    InternalJdbcConnectorService service = cache.getService(InternalJdbcConnectorService.class);
    assertThat(service.getMappingForRegion(regionName)).isNull();

    Result result = command.destroyMapping(regionName);
    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);

    assertThat(service.getMappingForRegion(regionName)).isNull();
  }
}
