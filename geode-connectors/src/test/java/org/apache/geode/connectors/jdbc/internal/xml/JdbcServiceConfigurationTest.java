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
package org.apache.geode.connectors.jdbc.internal.xml;

import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.NAMESPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class JdbcServiceConfigurationTest {

  private JdbcServiceConfiguration configuration;

  private InternalCache cache;
  private InternalJdbcConnectorService service;
  private ConnectionConfiguration connection1;
  private ConnectionConfiguration connection2;
  private RegionMapping mapping1;
  private RegionMapping mapping2;

  @Before
  public void setUp() {
    connection1 = mock(ConnectionConfiguration.class);
    connection2 = mock(ConnectionConfiguration.class);
    mapping1 = mock(RegionMapping.class);
    mapping2 = mock(RegionMapping.class);

    service = mock(InternalJdbcConnectorService.class);
    cache = mock(InternalCache.class);
    when(cache.getService(InternalJdbcConnectorService.class)).thenReturn(service);

    configuration = new JdbcServiceConfiguration();
  }

  @Test
  public void getXmlGeneratorReturnsJdbcConnectorServiceXmlGenerator() {
    XmlGenerator<Cache> generator = configuration.getXmlGenerator();

    assertThat(generator).isInstanceOf(JdbcConnectorServiceXmlGenerator.class);
  }

  @Test
  public void getXmlGeneratorReturnsGeneratorWithJdbcConnectorNamespace() {
    XmlGenerator<Cache> generator = configuration.getXmlGenerator();

    assertThat(generator.getNamespaceUri()).isEqualTo(NAMESPACE);
  }

  @Test
  public void getXmlGeneratorReturnsEmptyGeneratorByDefault() {
    JdbcConnectorServiceXmlGenerator generator =
        (JdbcConnectorServiceXmlGenerator) configuration.getXmlGenerator();

    assertThat(generator.getConnections()).isEmpty();
    assertThat(generator.getMappings()).isEmpty();
  }

  @Test
  public void getXmlGeneratorWithConnections() {
    configuration.addConnectionConfig(connection1);
    configuration.addConnectionConfig(connection2);

    JdbcConnectorServiceXmlGenerator generator =
        (JdbcConnectorServiceXmlGenerator) configuration.getXmlGenerator();

    assertThat(generator.getConnections()).containsExactly(connection1, connection2);
  }

  @Test
  public void getXmlGeneratorWithRegionMappings() {
    configuration.addRegionMapping(mapping1);
    configuration.addRegionMapping(mapping2);

    JdbcConnectorServiceXmlGenerator generator =
        (JdbcConnectorServiceXmlGenerator) configuration.getXmlGenerator();

    assertThat(generator.getMappings()).containsExactly(mapping1, mapping2);
  }

  @Test
  public void onCreateWithNoConnectionsOrMappings() {
    configuration.onCreate(cache, cache);
    verifyZeroInteractions(service);
  }

  @Test
  public void onCreateWithConnections() {
    configuration.addConnectionConfig(connection1);
    configuration.addConnectionConfig(connection2);

    configuration.onCreate(cache, cache);

    verify(service, times(1)).addOrUpdateConnectionConfig(connection1);
    verify(service, times(1)).addOrUpdateConnectionConfig(connection2);
  }

  @Test
  public void onCreateWithRegionMappings() {
    configuration.addRegionMapping(mapping1);
    configuration.addRegionMapping(mapping2);

    configuration.onCreate(cache, cache);

    verify(service, times(1)).addOrUpdateRegionMapping(mapping1);
    verify(service, times(1)).addOrUpdateRegionMapping(mapping2);
  }

}
