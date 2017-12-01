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
package org.apache.geode.connectors.jdbc.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class JdbcConnectorServiceTest {

  private static final String TEST_CONFIG_NAME = "testConfig";
  private static final String TEST_REGION_NAME = "testRegion";

  private ConnectionConfiguration config;
  private ConnectionConfiguration config2;
  private RegionMapping mapping;

  private JdbcConnectorService service;

  @Before
  public void setUp() throws Exception {
    InternalCache cache = mock(InternalCache.class);
    config = mock(ConnectionConfiguration.class);
    mapping = mock(RegionMapping.class);
    config2 = mock(ConnectionConfiguration.class);

    when(cache.getExtensionPoint()).thenReturn(mock(ExtensionPoint.class));
    when(config.getName()).thenReturn(TEST_CONFIG_NAME);
    when(config2.getName()).thenReturn(TEST_CONFIG_NAME);
    when(mapping.getRegionName()).thenReturn(TEST_REGION_NAME);

    service = new JdbcConnectorService();
    service.init(cache);
  }

  @Test
  public void returnsNoConfigIfEmpty() throws Exception {
    assertThat(service.getConnectionConfig("foo")).isNull();
  }

  @Test
  public void returnsNoMappingIfEmpty() throws Exception {
    assertThat(service.getMappingForRegion("foo")).isNull();
  }

  @Test
  public void returnsCorrectConfig() throws Exception {
    service.createConnectionConfig(config);

    assertThat(service.getConnectionConfig(TEST_CONFIG_NAME)).isSameAs(config);
  }

  @Test
  public void doesNotReturnConfigWithDifferentName() throws Exception {
    when(config.getName()).thenReturn("theOtherConfig");
    service.createConnectionConfig(config);

    assertThat(service.getConnectionConfig(TEST_CONFIG_NAME)).isNull();
  }

  @Test
  public void returnsCorrectMapping() throws Exception {
    service.addOrUpdateRegionMapping(mapping);

    assertThat(service.getMappingForRegion(TEST_REGION_NAME)).isSameAs(mapping);
  }

  @Test
  public void doesNotReturnMappingForDifferentRegion() throws Exception {
    when(mapping.getRegionName()).thenReturn("theOtherMapping");
    service.addOrUpdateRegionMapping(mapping);

    assertThat(service.getMappingForRegion(TEST_REGION_NAME)).isNull();
  }

  @Test
  public void createConnectionConfig_throwsIfConnectionExists() throws Exception {
    service.createConnectionConfig(config);

    assertThatThrownBy(() -> service.createConnectionConfig(config2))
        .isInstanceOf(ConnectionConfigExistsException.class).hasMessageContaining(TEST_CONFIG_NAME);
  }
}
