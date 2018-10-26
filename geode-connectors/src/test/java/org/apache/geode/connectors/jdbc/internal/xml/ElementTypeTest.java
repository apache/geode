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


import static org.apache.geode.connectors.jdbc.internal.xml.ElementType.FIELD_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.xml.ElementType.JDBC_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.COLUMN_NAME;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.CONNECTION_NAME;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.FIELD_NAME;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.PDX_CLASS;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.PRIMARY_KEY_IN_VALUE;
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
import org.apache.geode.connectors.jdbc.internal.TableMetaDataView;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.xmlcache.RegionCreation;

public class ElementTypeTest {

  private Attributes attributes;
  private RegionCreation regionCreation;
  private ExtensionPoint<Region<?, ?>> extensionPoint;
  private Stack<Object> stack;

  @Before
  public void setup() {
    attributes = mock(Attributes.class);
    regionCreation = mock(RegionCreation.class);
    extensionPoint = mock(ExtensionPoint.class);

    when(regionCreation.getExtensionPoint()).thenReturn(extensionPoint);

    stack = new Stack<>();
  }

  // @Test
  // public void gettingElementTypeByNameReturnsCorrectType() {
  // assertThat(ElementType.getTypeFromName(CONNECTION_SERVICE.getTypeName()))
  // .isSameAs(CONNECTION_SERVICE);
  // assertThat(ElementType.getTypeFromName(REGION_MAPPING.getTypeName())).isSameAs(REGION_MAPPING);
  // assertThat(ElementType.getTypeFromName(FIELD_MAPPING.getTypeName())).isSameAs(FIELD_MAPPING);
  // }

  @Test
  public void gettingElementTypeThatDoesNotExistThrowsException() {
    assertThatThrownBy(() -> ElementType.getTypeFromName("non-existent element"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // @Test
  // public void startElementConnectionServiceThrowsWithoutCacheCreation() {
  // stack.push(new Object());
  //
  // assertThatThrownBy(() -> CONNECTION_SERVICE.startElement(stack, attributes))
  // .isInstanceOf(CacheXmlException.class);
  // }

  // @Test
  // public void startElementConnectionService() {
  // stack.push(cacheCreation);
  //
  // CONNECTION_SERVICE.startElement(stack, attributes);
  //
  // verify(extensionPoint, times(1)).addExtension(any(JdbcServiceConfiguration.class));
  // assertThat(stack.peek()).isInstanceOf(JdbcServiceConfiguration.class);
  // }
  //
  // @Test
  // public void endElementConnectionService() {
  // stack.push(new Object());
  //
  // CONNECTION_SERVICE.endElement(stack);
  //
  // assertThat(stack).isEmpty();
  // }

  @Test
  public void startElementRegionMappingThrowsWithoutJdbcServiceConfiguration() {
    stack.push(new Object());

    assertThatThrownBy(() -> JDBC_MAPPING.startElement(stack, attributes))
        .isInstanceOf(CacheXmlException.class);
  }

  @Test
  public void startElementRegionMapping() {
    when(attributes.getValue(CONNECTION_NAME)).thenReturn("connectionName");
    when(attributes.getValue(TABLE)).thenReturn("table");
    when(attributes.getValue(PDX_CLASS)).thenReturn("pdxClass");
    when(attributes.getValue(PRIMARY_KEY_IN_VALUE)).thenReturn("true");
    when(regionCreation.getFullPath()).thenReturn("/region");
    stack.push(regionCreation);

    ElementType.JDBC_MAPPING.startElement(stack, attributes);

    RegionMapping regionMapping = (RegionMapping) stack.pop();
    assertThat(regionMapping.getRegionName()).isEqualTo("region");
    assertThat(regionMapping.getConnectionConfigName()).isEqualTo("connectionName");
    assertThat(regionMapping.getTableName()).isEqualTo("table");
    assertThat(regionMapping.getPdxClassName()).isEqualTo("pdxClass");
    assertThat(regionMapping.isPrimaryKeyInValue()).isEqualTo(true);
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
  public void startElementFieldMappingThrowsWithoutRegionMappingBuilder() {
    stack.push(new Object());

    assertThatThrownBy(() -> FIELD_MAPPING.startElement(stack, attributes))
        .isInstanceOf(CacheXmlException.class);
  }

  @Test
  public void startElementFieldMapping() {
    RegionMapping mapping = new RegionMapping();
    stack.push(mapping);
    when(attributes.getValue(FIELD_NAME)).thenReturn("fieldName");
    when(attributes.getValue(COLUMN_NAME)).thenReturn("columnName");

    ElementType.FIELD_MAPPING.startElement(stack, attributes);

    RegionMapping mapping1 = (RegionMapping) stack.pop();
    assertThat(mapping1.getColumnNameForField("fieldName", mock(TableMetaDataView.class)))
        .isEqualTo("columnName");
  }

  @Test
  public void endElementFieldMapping() {
    RegionMapping mapping = mock(RegionMapping.class);
    RegionMappingConfiguration regionMappingConfiguration = mock(RegionMappingConfiguration.class);
    stack.push(regionMappingConfiguration);
    stack.push(mapping);

    ElementType.FIELD_MAPPING.endElement(stack);

    assertThat(stack.size()).isEqualTo(2);
    verifyZeroInteractions(mapping);
  }
}
