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

import static java.util.Collections.singletonList;
import static org.apache.geode.connectors.jdbc.internal.xml.ElementType.FIELD_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.xml.ElementType.JDBC_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.CATALOG;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.DATA_SOURCE;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.IDS;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.JDBC_NAME;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.JDBC_NULLABLE;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.JDBC_TYPE;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.PDX_NAME;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.PDX_TYPE;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.SCHEMA;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Stack;

import org.junit.Before;
import org.junit.Test;
import org.xml.sax.Attributes;

import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.Region;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.xmlcache.RegionCreation;

public class ElementTypeTest {

  private Attributes attributes;
  private RegionCreation regionCreation;
  private Stack<Object> stack;

  @Before
  public void setup() {
    attributes = mock(Attributes.class);
    regionCreation = mock(RegionCreation.class);
    @SuppressWarnings("unchecked")
    ExtensionPoint<Region<?, ?>> extensionPoint = mock(ExtensionPoint.class);

    when(regionCreation.getExtensionPoint()).thenReturn(extensionPoint);

    stack = new Stack<>();
  }

  @Test
  public void gettingElementTypeThatDoesNotExistThrowsException() {
    assertThatThrownBy(() -> ElementType.getTypeFromName("non-existent element"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void startElementRegionMappingThrowsWithoutJdbcServiceConfiguration() {
    stack.push(new Object());

    assertThatThrownBy(() -> JDBC_MAPPING.startElement(stack, attributes))
        .isInstanceOf(CacheXmlException.class);
  }

  @Test
  public void startElementRegionMapping() {
    when(attributes.getValue(DATA_SOURCE)).thenReturn("connectionName");
    when(attributes.getValue(TABLE)).thenReturn("table");
    when(attributes.getValue(PDX_NAME)).thenReturn("pdxClass");
    when(attributes.getValue(IDS)).thenReturn("ids");
    when(attributes.getValue(CATALOG)).thenReturn("catalog");
    when(attributes.getValue(SCHEMA)).thenReturn("schema");
    when(regionCreation.getFullPath()).thenReturn("/region");
    stack.push(regionCreation);

    ElementType.JDBC_MAPPING.startElement(stack, attributes);

    RegionMapping regionMapping = (RegionMapping) stack.pop();
    assertThat(regionMapping.getRegionName()).isEqualTo("region");
    assertThat(regionMapping.getDataSourceName()).isEqualTo("connectionName");
    assertThat(regionMapping.getTableName()).isEqualTo("table");
    assertThat(regionMapping.getPdxName()).isEqualTo("pdxClass");
    assertThat(regionMapping.getIds()).isEqualTo("ids");
    assertThat(regionMapping.getCatalog()).isEqualTo("catalog");
    assertThat(regionMapping.getSchema()).isEqualTo("schema");
  }

  @Test
  public void endElementRegionMapping() {
    RegionMapping mapping = mock(RegionMapping.class);
    stack.push(regionCreation);
    stack.push(mapping);

    ElementType.JDBC_MAPPING.endElement(stack);

    assertThat(stack.size()).isEqualTo(1);
  }

  @Test
  public void startElementFieldMappingThrowsWithoutRegionMapping() {
    stack.push(new Object());

    assertThatThrownBy(() -> FIELD_MAPPING.startElement(stack, attributes))
        .isInstanceOf(CacheXmlException.class)
        .hasMessage("<jdbc:field-mapping> elements must occur within <jdbc:mapping> elements");
  }

  @Test
  public void startElementFieldMapping() {
    RegionMapping mapping = new RegionMapping();
    stack.push(mapping);
    when(attributes.getValue(PDX_NAME)).thenReturn("myPdxName");
    when(attributes.getValue(PDX_TYPE)).thenReturn("myPdxType");
    when(attributes.getValue(JDBC_NAME)).thenReturn("myJdbcName");
    when(attributes.getValue(JDBC_TYPE)).thenReturn("myJdbcType");
    when(attributes.getValue(JDBC_NULLABLE)).thenReturn("true");
    FieldMapping expected =
        new FieldMapping("myPdxName", "myPdxType", "myJdbcName", "myJdbcType", true);

    ElementType.FIELD_MAPPING.startElement(stack, attributes);

    RegionMapping mapping1 = (RegionMapping) stack.pop();
    assertThat(mapping1.getFieldMappings()).isEqualTo(singletonList(expected));
  }

  @Test
  public void endElementFieldMapping() {
    RegionMapping mapping = mock(RegionMapping.class);
    stack.push(mapping);

    ElementType.FIELD_MAPPING.endElement(stack);

    assertThat(stack.size()).isEqualTo(1);
    verifyZeroInteractions(mapping);
  }
}
