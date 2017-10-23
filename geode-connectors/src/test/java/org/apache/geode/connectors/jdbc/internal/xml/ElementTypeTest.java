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

import static org.apache.geode.connectors.jdbc.internal.xml.ElementType.CONNECTION;
import static org.apache.geode.connectors.jdbc.internal.xml.ElementType.CONNECTION_SERVICE;
import static org.apache.geode.connectors.jdbc.internal.xml.ElementType.FIELD_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.xml.ElementType.REGION_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.COLUMN_NAME;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.CONNECTION_NAME;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.FIELD_NAME;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.PDX_CLASS;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.PRIMARY_KEY_IN_VALUE;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.REGION;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Stack;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.xml.sax.Attributes;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.RegionMapping;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ElementTypeTest {
  private Stack<Object> stack = new Stack<>();
  private Attributes attributes;
  private CacheCreation cacheCreation;
  private ExtensionPoint<Cache> extensionPoint;

  @Before
  public void setup() {
    attributes = mock(Attributes.class);
    cacheCreation = mock(CacheCreation.class);
    extensionPoint = mock(ExtensionPoint.class);
    when(cacheCreation.getExtensionPoint()).thenReturn(extensionPoint);
  }

  @Test
  public void gettingElementTypeByNameReturnsCorrectType() {
    assertThat(ElementType.getTypeFromName(CONNECTION_SERVICE.getTypeName()))
        .isSameAs(CONNECTION_SERVICE);
    assertThat(ElementType.getTypeFromName(CONNECTION.getTypeName())).isSameAs(CONNECTION);
    assertThat(ElementType.getTypeFromName(REGION_MAPPING.getTypeName())).isSameAs(REGION_MAPPING);
    assertThat(ElementType.getTypeFromName(FIELD_MAPPING.getTypeName())).isSameAs(FIELD_MAPPING);
  }

  @Test
  public void gettingElementTypeThatDoesNotExistThrowsException() {
    assertThatThrownBy(() -> ElementType.getTypeFromName("non-existant element"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void startElementConnectionServiceThrowsWithoutCacheCreation() {
    stack.push(new Object());
    assertThatThrownBy(() -> CONNECTION_SERVICE.startElement(stack, attributes))
        .isInstanceOf(CacheXmlException.class);
  }

  @Test
  public void startElementConnectionService() {
    stack.push(cacheCreation);
    CONNECTION_SERVICE.startElement(stack, attributes);
    verify(extensionPoint, times(1)).addExtension(any(JdbcServiceConfiguration.class));
    assertThat(stack.peek()).isInstanceOf(JdbcServiceConfiguration.class);
  }

  @Test
  public void endElementConnectionService() {
    stack.push(new Object());
    CONNECTION_SERVICE.endElement(stack);
    assertThat(stack).isEmpty();
  }

  @Test
  public void startElementConnectionThrowsWithoutJdbcServiceConfiguration() {
    stack.push(new Object());
    assertThatThrownBy(() -> CONNECTION.startElement(stack, attributes))
        .isInstanceOf(CacheXmlException.class);
  }

  @Test
  public void startElementConnection() {
    JdbcServiceConfiguration serviceConfiguration = mock(JdbcServiceConfiguration.class);
    stack.push(serviceConfiguration);

    when(attributes.getValue(JdbcConnectorServiceXmlParser.NAME)).thenReturn("connectionName");
    when(attributes.getValue(JdbcConnectorServiceXmlParser.URL)).thenReturn("url");
    when(attributes.getValue(JdbcConnectorServiceXmlParser.USER)).thenReturn("username");
    when(attributes.getValue(JdbcConnectorServiceXmlParser.PASSWORD)).thenReturn("secret");

    CONNECTION.startElement(stack, attributes);
    ConnectionConfiguration config = ((ConnectionConfigBuilder) stack.pop()).build();

    assertThat(config.getName()).isEqualTo("connectionName");
    assertThat(config.getUrl()).isEqualTo("url");
    assertThat(config.getUser()).isEqualTo("username");
    assertThat(config.getPassword()).isEqualTo("secret");
  }

  @Test
  public void endElementConnection() {
    ConnectionConfigBuilder builder = mock(ConnectionConfigBuilder.class);
    JdbcServiceConfiguration serviceConfiguration = mock(JdbcServiceConfiguration.class);
    stack.push(serviceConfiguration);
    stack.push(builder);

    CONNECTION.endElement(stack);

    assertThat(stack.size()).isEqualTo(1);
    verify(serviceConfiguration, times(1)).addConnectionConfig(any());
  }

  @Test
  public void startElementRegionMappingThrowsWithoutJdbcServiceConfiguration() {
    stack.push(new Object());
    assertThatThrownBy(() -> REGION_MAPPING.startElement(stack, attributes))
        .isInstanceOf(CacheXmlException.class);
  }

  @Test
  public void startElementRegionMapping() {
    JdbcServiceConfiguration serviceConfiguration = mock(JdbcServiceConfiguration.class);
    stack.push(serviceConfiguration);

    when(attributes.getValue(REGION)).thenReturn("region");
    when(attributes.getValue(CONNECTION_NAME)).thenReturn("connectionName");
    when(attributes.getValue(TABLE)).thenReturn("table");
    when(attributes.getValue(PDX_CLASS)).thenReturn("pdxClass");
    when(attributes.getValue(PRIMARY_KEY_IN_VALUE)).thenReturn("true");

    ElementType.REGION_MAPPING.startElement(stack, attributes);

    RegionMapping regionMapping = ((RegionMappingBuilder) stack.pop()).build();
    assertThat(regionMapping.getRegionName()).isEqualTo("region");
    assertThat(regionMapping.getConnectionConfigName()).isEqualTo("connectionName");
    assertThat(regionMapping.getTableName()).isEqualTo("table");
    assertThat(regionMapping.getPdxClassName()).isEqualTo("pdxClass");
    assertThat(regionMapping.isPrimaryKeyInValue()).isEqualTo(true);
  }

  @Test
  public void endElementRegionMapping() {
    RegionMappingBuilder builder = mock(RegionMappingBuilder.class);
    JdbcServiceConfiguration serviceConfiguration = mock(JdbcServiceConfiguration.class);
    stack.push(serviceConfiguration);
    stack.push(builder);

    ElementType.REGION_MAPPING.endElement(stack);

    assertThat(stack.size()).isEqualTo(1);
    verify(serviceConfiguration, times(1)).addRegionMapping(any());
  }

  @Test
  public void startElementFieldMappingThrowsWithoutRegionMappingBuilder() {
    stack.push(new Object());
    assertThatThrownBy(() -> FIELD_MAPPING.startElement(stack, attributes))
        .isInstanceOf(CacheXmlException.class);
  }

  @Test
  public void startElementFieldMapping() {
    RegionMappingBuilder builder = new RegionMappingBuilder();
    stack.push(builder);
    when(attributes.getValue(FIELD_NAME)).thenReturn("fieldName");
    when(attributes.getValue(COLUMN_NAME)).thenReturn("columnName");

    ElementType.FIELD_MAPPING.startElement(stack, attributes);
    RegionMapping regionMapping = ((RegionMappingBuilder) stack.pop()).build();

    assertThat(regionMapping.getColumnNameForField("fieldName")).isEqualTo("columnName");
  }

  @Test
  public void endElementFieldMapping() {
    RegionMappingBuilder builder = mock(RegionMappingBuilder.class);
    JdbcServiceConfiguration serviceConfiguration = mock(JdbcServiceConfiguration.class);
    stack.push(serviceConfiguration);
    stack.push(builder);

    ElementType.FIELD_MAPPING.endElement(stack);

    assertThat(stack.size()).isEqualTo(2);
    verifyZeroInteractions(builder);
  }
}
